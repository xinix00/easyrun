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

// UpdateJob updates an existing job (found by name) with a new definition.
// The update policy is taken from newJob.UpdatePolicy (default: rolling).
func (l *Leader) UpdateJob(newJob *types.Job) error {
	oldJob := l.jobStore.GetJob(newJob.Name)
	if oldJob == nil {
		return fmt.Errorf("job %s not found", newJob.Name)
	}

	policy := newJob.UpdatePolicy
	if policy == "" {
		policy = types.UpdateRolling
	}

	log.Printf("Updating job %s with policy=%s", newJob.Name, policy)

	switch policy {
	case types.UpdateRolling:
		return l.updateRolling(newJob)
	case types.UpdateRecreate:
		return l.updateRecreate(newJob)
	case types.UpdateBlueGreen:
		return l.updateBlueGreen(newJob)
	default:
		return fmt.Errorf("unknown update policy: %s", policy)
	}
}

// updateRolling: for each instance, dispatch new → stop one old task (by task ID).
// Snapshots old task IDs first so we know exactly which tasks to stop.
func (l *Leader) updateRolling(job *types.Job) error {
	count := job.Count
	if count <= 0 {
		count = 1
	}

	// Snapshot old task IDs before updating the job definition.
	// These are the tasks we need to replace.
	oldTasks := l.snapshotJobTasks(job.Name)

	// Store new job definition (agents will use it for new tasks).
	l.jobStore.StoreJob(job)

	replaced := 0
	for i, oldTask := range oldTasks {
		if i >= count {
			break // don't replace more than desired count
		}
		log.Printf("Rolling update %d/%d for job %s: dispatching new instance", i+1, count, job.Name)

		if err := l.dispatchToAvailableAgent(job); err != nil {
			return fmt.Errorf("rolling update failed at instance %d/%d: %w", i+1, count, err)
		}

		// Stop the old task by its specific task ID
		agent := l.agentForTask(oldTask.agentID)
		if agent != nil {
			l.stopTaskByID(agent, oldTask.taskID)
			l.do(func(s *leaderState) {
				if s.placed[oldTask.agentID] != nil && s.placed[oldTask.agentID][job.Name] > 0 {
					s.placed[oldTask.agentID][job.Name]--
				}
			})
		}

		replaced++
		l.eventBus.Notify("job:" + job.Name)

		if i < count-1 {
			time.Sleep(RollingUpdateDelay)
		}
	}

	log.Printf("Rolling update for job %s complete (%d/%d instances replaced)", job.Name, replaced, count)
	return nil
}

// updateRecreate: stop all → store new definition → reconcile dispatches all new.
func (l *Leader) updateRecreate(job *types.Job) error {
	// Stop all running instances (deleteTaskOnAgent removes job from agent too,
	// but we re-store immediately below via DispatchJob).
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
		l.stopTasksOnAgent(agent, job.Name)
	}

	return l.DispatchJob(job)
}

// updateBlueGreen: snapshot old tasks, dispatch all new, then stop all old.
func (l *Leader) updateBlueGreen(job *types.Job) error {
	oldTasks := l.snapshotJobTasks(job.Name)

	// Store new definition and dispatch all new instances.
	l.jobStore.StoreJob(job)
	if err := l.dispatchInstances(job, job.Count); err != nil {
		return fmt.Errorf("blue-green: failed to dispatch new instances: %w", err)
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
			if task.State == types.TaskRunning {
				refs = append(refs, taskRef{agentID: agent.ID, taskID: task.ID})
			}
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
