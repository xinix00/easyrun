package leader

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

var (
	// RollingUpdateDelay can be overridden in tests for faster execution
	RollingUpdateDelay = 2 * time.Second
)

// UpdateJob updates an existing job with a new version
func (l *Leader) UpdateJob(newJob *types.Job) error {
	oldJob := l.FindJobByName(newJob.Name)
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
		return l.updateRolling(oldJob, newJob)
	case types.UpdateRecreate:
		return l.updateRecreate(oldJob, newJob)
	case types.UpdateBlueGreen:
		return l.updateBlueGreen(oldJob, newJob)
	default:
		return fmt.Errorf("unknown update policy: %s", policy)
	}
}

// updateRolling: dispatch → KILL → delay (per instance, maintains capacity)
func (l *Leader) updateRolling(old, new *types.Job) error {
	count := old.Count
	if count <= 0 {
		count = 1
	}

	// Remove old job from store immediately so reconcileJobs won't
	// try to re-dispatch old instances during the rolling update.
	// We still have the old Job in memory for stopOneInstance.
	l.jobStore.DeleteJob(old.ID)

	for i := 0; i < count; i++ {
		log.Printf("Rolling update %d/%d (old ID %s → new ID %s)", i+1, count, old.ID, new.ID)

		// Start new instance first (tracked under new.ID)
		if err := l.dispatchToAvailableAgent(new); err != nil {
			// Restore old job — its instances are still running
			l.jobStore.StoreJob(old)
			return fmt.Errorf("failed at instance %d/%d: %w", i+1, count, err)
		}

		// Only stop old after new is running (tracked under old.ID)
		l.stopOneInstance(old)
		l.eventBus.Notify(new.Name)

		if i < count-1 {
			time.Sleep(RollingUpdateDelay)
		}
	}

	// Store new job definition
	l.jobStore.StoreJob(new)
	return nil
}

// updateRecreate: KILL all → dispatch all
func (l *Leader) updateRecreate(old, new *types.Job) error {
	l.DeleteJobByID(old)
	return l.DispatchJob(new)
}

// updateBlueGreen: dispatch all → KILL all
func (l *Leader) updateBlueGreen(old, new *types.Job) error {
	if err := l.DispatchJob(new); err != nil {
		return err
	}
	l.DeleteJobByID(old) // Deletes old by ID, new stays (different ID!)
	return nil
}

// stopOneInstance stops one instance of a job (uses placed to find agent, decrements count)
func (l *Leader) stopOneInstance(job *types.Job) {
	agent := query(l, func(s *leaderState) *types.Agent {
		for agentID, jobs := range s.placed {
			if jobs[job.ID] > 0 {
				jobs[job.ID]--
				if jobs[job.ID] == 0 {
					delete(jobs, job.ID)
				}
				return s.agents[agentID]
			}
		}
		return nil
	})

	if agent != nil {
		l.deleteTaskOnAgent(agent, job.ID)
	}
}

// deleteTaskOnAgent deletes a job on specific agent (by job ID).
// Uses a dedicated long timeout because Docker stops can take ~20s.
func (l *Leader) deleteTaskOnAgent(agent *types.Agent, jobID string) {
	url := fmt.Sprintf("%s/delete/%s", agent.Endpoint, jobID)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Printf("Failed to create delete request for %s on %s: %v", jobID, agent.ID, err)
		return
	}
	client := &http.Client{Timeout: DeleteClientTimeout}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to delete %s on %s: %v", jobID, agent.ID, err)
		return
	}
	resp.Body.Close()
}
