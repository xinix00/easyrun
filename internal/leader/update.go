package leader

import (
	"fmt"
	"log"
	"time"

	"easyrun/internal/types"
)

var (
	// RollingUpdateDelay can be overridden in tests for faster execution
	RollingUpdateDelay = 2 * time.Second
)

// lockJob atomically sets the dispatching flag for a job.
// Returns false if the job is already being dispatched or updated.
func (l *Leader) lockJob(name string) bool {
	return !query(l, func(s *leaderState) bool {
		if s.dispatching[name] {
			return true
		}
		s.dispatching[name] = true
		return false
	})
}

// unlockJob clears the dispatching flag for a job.
func (l *Leader) unlockJob(name string) {
	l.do(func(s *leaderState) { delete(s.dispatching, name) })
}

// UpdateJob updates an existing job (found by name) with a new definition.
// The update policy is taken from newJob.UpdatePolicy (default: rolling).
// Returns an error if the job is already being updated or dispatched.
func (l *Leader) UpdateJob(newJob *types.Job) error {
	oldJob := l.jobStore.GetJob(newJob.Name)
	if oldJob == nil {
		return fmt.Errorf("job %s not found", newJob.Name)
	}

	// Preserve priority from old job if not explicitly set
	if newJob.Priority == nil {
		newJob.Priority = oldJob.Priority
	}

	policy := newJob.UpdatePolicy
	if policy == "" {
		policy = types.UpdateRolling
	}

	if !l.lockJob(newJob.Name) {
		return fmt.Errorf("job %s is already being updated or dispatched", newJob.Name)
	}

	log.Printf("Updating job %s with policy=%s", newJob.Name, policy)

	var err error
	switch policy {
	case types.UpdateRolling:
		err = l.updateRolling(newJob)
	case types.UpdateRecreate:
		err = l.updateRecreate(newJob)
	case types.UpdateBlueGreen:
		err = l.updateBlueGreen(newJob)
	default:
		return fmt.Errorf("unknown update policy: %s", policy)
	}

	l.normalizePriorities(l.jobStore.GetJobs())
	return err
}

// updateRolling: for each old task, dispatch new (if within count) → stop old.
// After the loop, reconcileJobs dispatches extra instances for scale-up.
func (l *Leader) updateRolling(job *types.Job) error {
	count := job.Count
	if count <= 0 {
		count = 1
	}

	oldTasks := l.snapshotJobTasks(job.Name)
	l.jobStore.StoreJob(job)

	for i, oldTask := range oldTasks {
		if i < count {
			// Replace: dispatch new version before stopping old (zero downtime)
			if err := l.dispatchToAvailableAgent(job); err != nil {
				return fmt.Errorf("rolling update failed at instance %d: %w", i+1, err)
			}
		}

		// Stop old task (replacement or scale-down excess)
		agent := l.agentForTask(oldTask.agentID)
		if agent != nil {
			l.stopTaskByID(agent, oldTask.taskID)
			l.do(func(s *leaderState) {
				if s.placed[oldTask.agentID] != nil && s.placed[oldTask.agentID][job.Name] > 0 {
					s.placed[oldTask.agentID][job.Name]--
				}
			})
		}

		l.eventBus.Notify("job:" + job.Name)

		if i < count-1 {
			time.Sleep(RollingUpdateDelay)
		}
	}

	log.Printf("Rolling update for job %s complete (%d old, %d desired)", job.Name, len(oldTasks), count)

	// Reconcile handles scale-up (placed < desired → dispatch extra).
	l.unlockJob(job.Name)
	go l.reconcileJobs()
	return nil
}

// updateRecreate: stop all → store new definition → dispatch all new.
func (l *Leader) updateRecreate(job *types.Job) error {
	agents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for agentID, jobs := range s.placed {
			if jobs[job.Name] > 0 {
				if a := s.agents[agentID]; a != nil {
					result = append(result, a)
				}
				delete(jobs, job.Name)
			}
		}
		return result
	})

	for _, agent := range agents {
		_ = l.stopTasksOnAgent(agent, job.Name)
	}

	l.unlockJob(job.Name)
	return l.DispatchJob(job)
}

// updateBlueGreen: snapshot old tasks, dispatch all new, then stop all old.
func (l *Leader) updateBlueGreen(job *types.Job) error {
	defer l.unlockJob(job.Name)
	oldTasks := l.snapshotJobTasks(job.Name)

	count := job.Count
	if count <= 0 {
		count = 1
	}

	// Store new definition and dispatch all new instances.
	l.jobStore.StoreJob(job)
	for i := 0; i < count; i++ {
		if err := l.dispatchToAvailableAgent(job); err != nil {
			return fmt.Errorf("blue-green: failed to dispatch instance %d/%d: %w", i+1, count, err)
		}
	}

	// Stop all old instances by task ID.
	for _, oldTask := range oldTasks {
		agent := l.agentForTask(oldTask.agentID)
		if agent != nil {
			l.stopTaskByID(agent, oldTask.taskID)
			l.do(func(s *leaderState) {
				if s.placed[oldTask.agentID] != nil && s.placed[oldTask.agentID][job.Name] > 0 {
					s.placed[oldTask.agentID][job.Name]--
				}
			})
		}
	}

	l.eventBus.Notify("job:" + job.Name)
	return nil
}

// taskRef holds the agent ID and task ID of a running task.
type taskRef struct {
	agentID string
	taskID  string
}

// snapshotJobTasks fetches all currently running task IDs for a job from all agents.
// Used by rolling and blue-green updates to know which tasks to stop.
func (l *Leader) snapshotJobTasks(jobName string) []taskRef {
	// Only query agents that have this job placed.
	agents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for agentID, jobs := range s.placed {
			if jobs[jobName] > 0 {
				if a := s.agents[agentID]; a != nil {
					result = append(result, a)
				}
			}
		}
		return result
	})

	tasksByAgent, _ := l.GetJobStatus(jobName)

	var refs []taskRef
	for _, agent := range agents {
		for _, task := range tasksByAgent[agent.ID] {
			refs = append(refs, taskRef{agentID: agent.ID, taskID: task.ID})
		}
	}
	return refs
}

// agentForTask returns the agent with the given ID (or nil if not registered).
func (l *Leader) agentForTask(agentID string) *types.Agent {
	return query(l, func(s *leaderState) *types.Agent {
		return s.agents[agentID]
	})
}
