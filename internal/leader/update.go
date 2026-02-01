package leader

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

const (
	rollingUpdateDelay = 2 * time.Second
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

// updateRolling: KILL → dispatch → delay (per instance)
func (l *Leader) updateRolling(old, new *types.Job) error {
	count := old.Count
	if count <= 0 {
		count = 1
	}

	for i := 0; i < count; i++ {
		log.Printf("Rolling update %d/%d", i+1, count)

		l.stopOneInstance(old.Name)

		if err := l.dispatchToAvailableAgent(new); err != nil {
			return fmt.Errorf("failed at instance %d/%d: %w", i+1, count, err)
		}

		if i < count-1 {
			time.Sleep(rollingUpdateDelay)
		}
	}

	l.jobStore.StoreJob(new)
	return nil
}

// updateRecreate: KILL all → dispatch all
func (l *Leader) updateRecreate(old, new *types.Job) error {
	l.StopJob(old.Name)
	time.Sleep(1 * time.Second)
	return l.DispatchJob(new)
}

// updateBlueGreen: dispatch all → KILL all
func (l *Leader) updateBlueGreen(old, new *types.Job) error {
	new.Count = old.Count
	if err := l.DispatchJob(new); err != nil {
		return err
	}
	time.Sleep(1 * time.Second)
	l.StopJob(old.Name)
	return nil
}

// stopOneInstance stops one instance of a job
func (l *Leader) stopOneInstance(jobName string) {
	agentID := query(l, func(s *leaderState) string {
		if len(s.placement[jobName]) == 0 {
			return ""
		}
		id := s.placement[jobName][0]
		s.placement[jobName] = s.placement[jobName][1:]
		return id
	})

	if agentID == "" {
		return
	}

	agent := query(l, func(s *leaderState) *types.Agent {
		return s.agents[agentID]
	})

	if agent != nil {
		l.stopTaskOnAgent(agent, jobName)
	}
}

// stopTaskOnAgent stops a job on specific agent
func (l *Leader) stopTaskOnAgent(agent *types.Agent, jobName string) {
	url := fmt.Sprintf("%s/stop/%s", agent.Endpoint, jobName)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := l.httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to stop %s on %s: %v", jobName, agent.ID, err)
		return
	}
	resp.Body.Close()
}
