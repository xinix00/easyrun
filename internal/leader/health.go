package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

// Run starts the leader's state loop and dead agent checker
func (l *Leader) Run(ctx context.Context) {
	// Start the state loop
	go l.stateLoop(ctx)

	ticker := time.NewTicker(deadAgentCheckInterval)
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
	dead := query(l, func(s *leaderState) []string {
		var deadAgents []string
		for id, agent := range s.agents {
			if time.Since(agent.LastSeen) > l.agentTimeout {
				log.Printf("Agent %s is dead (no heartbeat for %v)", id, time.Since(agent.LastSeen))
				deadAgents = append(deadAgents, id)
				delete(s.agents, id)
			}
		}
		return deadAgents
	})

	for _, agentID := range dead {
		l.redispatchJobsFrom(agentID)
	}
}

// redispatchJobsFrom moves instances from a failed agent to other agents
func (l *Leader) redispatchJobsFrom(failedAgentID string) {
	// Get jobs to redispatch and update placement
	jobsToRedispatch := query(l, func(s *leaderState) []*types.Job {
		var jobs []*types.Job

		for jobID, agentIDs := range s.placement {
			instancesOnFailedAgent := 0
			var newAgentList []string

			for _, agentID := range agentIDs {
				if agentID == failedAgentID {
					instancesOnFailedAgent++
				} else {
					newAgentList = append(newAgentList, agentID)
				}
			}

			// Update placement
			if len(newAgentList) > 0 {
				s.placement[jobID] = newAgentList
			} else {
				delete(s.placement, jobID)
			}

			// Queue redispatch for lost instances
			if instancesOnFailedAgent > 0 {
				if job := l.jobStore.GetJob(jobID); job != nil {
					jobCopy := *job
					jobCopy.Count = instancesOnFailedAgent
					jobs = append(jobs, &jobCopy)
				}
			}
		}

		return jobs
	})

	for _, job := range jobsToRedispatch {
		log.Printf("Redispatching %d instance(s) of job %s from dead agent %s", job.Count, job.Name, failedAgentID)
		if err := l.DispatchJob(job); err != nil {
			log.Printf("Failed to redispatch job %s: %v", job.Name, err)
		}
	}
}

// GetClusterStatus fetches status from all agents (parallel, no goroutine leaks!)
func (l *Leader) GetClusterStatus() map[string][]*types.Task {
	agents := l.GetAgents()
	result := make(map[string][]*types.Task)

	// Context with timeout - cancels all goroutines when done
	ctx, cancel := context.WithTimeout(context.Background(), HTTPClientTimeout)
	defer cancel()

	// Channel-based concurrency - no mutexes needed!
	type agentResult struct {
		agentID string
		tasks   []*types.Task
	}
	resultCh := make(chan agentResult, len(agents))

	// Fetch from all agents in parallel
	for _, agent := range agents {
		go func(a *types.Agent) {
			tasks, err := l.fetchAgentTasks(ctx, a)
			if err != nil {
				log.Printf("Failed to fetch tasks from %s: %v", a.ID, err)
				return
			}
			resultCh <- agentResult{agentID: a.ID, tasks: tasks}
		}(agent)
	}

	// Collect results (context cancellation stops all goroutines)
	collected := 0
	for collected < len(agents) {
		select {
		case res := <-resultCh:
			result[res.agentID] = res.tasks
			collected++
		case <-ctx.Done():
			log.Printf("GetClusterStatus timeout after collecting %d/%d agents", collected, len(agents))
			return result
		}
	}

	return result
}

// fetchAgentTasks gets the task list from an agent
func (l *Leader) fetchAgentTasks(ctx context.Context, agent *types.Agent) ([]*types.Task, error) {
	url := fmt.Sprintf("%s/tasks", agent.Endpoint)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := l.httpClient.Do(req)
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
