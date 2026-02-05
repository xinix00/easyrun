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

	for i := 0; i < count; i++ {
		log.Printf("Rolling update %d/%d (old ID %s → new ID %s)", i+1, count, old.ID, new.ID)

		// Start new instance first (tracked under new.ID)
		if err := l.dispatchToAvailableAgent(new); err != nil {
			return fmt.Errorf("failed at instance %d/%d: %w", i+1, count, err)
		}

		// Only stop old after new is running (tracked under old.ID)
		l.stopOneInstance(old)

		if i < count-1 {
			time.Sleep(RollingUpdateDelay)
		}
	}

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
	new.Count = old.Count
	if err := l.DispatchJob(new); err != nil {
		return err
	}
	l.DeleteJobByID(old)
	// Re-store new job (DeleteJobByID removes by name, which affects new too!)
	l.jobStore.StoreJob(new)
	return nil
}

// stopOneInstance stops one instance of a job (uses job.ID for placement, job.Name for agent)
func (l *Leader) stopOneInstance(job *types.Job) {
	agentID := query(l, func(s *leaderState) string {
		if len(s.placement[job.ID]) == 0 {
			return ""
		}
		id := s.placement[job.ID][0]
		s.placement[job.ID] = s.placement[job.ID][1:]
		return id
	})

	if agentID == "" {
		return
	}

	agent := query(l, func(s *leaderState) *types.Agent {
		return s.agents[agentID]
	})

	if agent != nil {
		l.deleteTaskOnAgent(agent, job.Name)
	}
}

// deleteTaskOnAgent deletes a job on specific agent
func (l *Leader) deleteTaskOnAgent(agent *types.Agent, jobName string) {
	url := fmt.Sprintf("%s/delete/%s", agent.Endpoint, jobName)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := l.httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to delete %s on %s: %v", jobName, agent.ID, err)
		return
	}
	resp.Body.Close()
}
