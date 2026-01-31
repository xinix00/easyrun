package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"easyrun/internal/types"
)

// Run starts the leader's dead agent checker loop
func (l *Leader) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.checkDeadAgents()
		}
	}
}

// checkDeadAgents removes agents that haven't sent heartbeat and redispatches their jobs
func (l *Leader) checkDeadAgents() {
	l.agentsMu.Lock()
	var dead []string
	for id, agent := range l.agents {
		if time.Since(agent.LastSeen) > l.agentTimeout {
			log.Printf("Agent %s is dead (no heartbeat for %v)", id, time.Since(agent.LastSeen))
			dead = append(dead, id)
			delete(l.agents, id)
		}
	}
	l.agentsMu.Unlock()

	for _, agentID := range dead {
		l.redispatchJobsFrom(agentID)
	}
}

// redispatchJobsFrom moves instances from a failed agent to other agents
func (l *Leader) redispatchJobsFrom(failedAgentID string) {
	l.placementMu.Lock()
	var jobsToRedispatch []*types.Job

	for jobID, agentIDs := range l.placement {
		// Count how many instances were on failed agent
		instancesOnFailedAgent := 0
		newAgentList := []string{}

		for _, agentID := range agentIDs {
			if agentID == failedAgentID {
				instancesOnFailedAgent++
			} else {
				newAgentList = append(newAgentList, agentID)
			}
		}

		// Update placement (remove failed agent instances)
		if len(newAgentList) > 0 {
			l.placement[jobID] = newAgentList
		} else {
			delete(l.placement, jobID)
		}

		// Queue redispatch for lost instances
		if instancesOnFailedAgent > 0 {
			if job := l.jobStore.GetJob(jobID); job != nil {
				// Create a copy with adjusted Count for only the lost instances
				jobCopy := *job
				jobCopy.Count = instancesOnFailedAgent
				jobsToRedispatch = append(jobsToRedispatch, &jobCopy)
			}
		}
	}
	l.placementMu.Unlock()

	for _, job := range jobsToRedispatch {
		log.Printf("Redispatching %d instance(s) of job %s from dead agent %s", job.Count, job.ID, failedAgentID)
		if err := l.DispatchJob(job); err != nil {
			log.Printf("Failed to redispatch job %s: %v", job.ID, err)
		}
	}
}

// GetClusterStatus fetches status from all agents
func (l *Leader) GetClusterStatus() map[string][]*types.Task {
	l.agentsMu.RLock()
	agents := make([]*types.Agent, 0, len(l.agents))
	for _, a := range l.agents {
		agents = append(agents, a)
	}
	l.agentsMu.RUnlock()

	result := make(map[string][]*types.Task)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, agent := range agents {
		wg.Add(1)
		go func(a *types.Agent) {
			defer wg.Done()

			tasks, err := l.fetchAgentTasks(a)
			if err != nil {
				log.Printf("Failed to fetch tasks from %s: %v", a.ID, err)
				return
			}

			mu.Lock()
			result[a.ID] = tasks
			mu.Unlock()
		}(agent)
	}

	wg.Wait()
	return result
}

// fetchAgentTasks gets the task list from an agent
func (l *Leader) fetchAgentTasks(agent *types.Agent) ([]*types.Task, error) {
	url := fmt.Sprintf("%s/tasks", agent.Endpoint)

	resp, err := l.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var tasks []*types.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}

	return tasks, nil
}
