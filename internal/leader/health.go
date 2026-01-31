package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
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
		log.Printf("Redispatching %d instance(s) of job %s from dead agent %s", job.Count, job.ID, failedAgentID)
		if err := l.DispatchJob(job); err != nil {
			log.Printf("Failed to redispatch job %s: %v", job.ID, err)
		}
	}
}

// GetClusterStatus fetches status from all agents
func (l *Leader) GetClusterStatus() map[string][]*types.Task {
	agents := l.GetAgents()

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

// ServiceEndpoint represents a running task's network endpoint
type ServiceEndpoint struct {
	IP    string         `json:"ip"`
	Ports map[string]int `json:"ports"` // port name -> port number
}

// ResolveJob returns endpoints (IP + ports) of running tasks for the given job name
func (l *Leader) ResolveJob(jobName string) []ServiceEndpoint {
	agents := l.GetAgents()
	agentEndpoints := make(map[string]string) // agentID -> endpoint
	for _, a := range agents {
		agentEndpoints[a.ID] = a.Endpoint
	}

	status := l.GetClusterStatus()

	var endpoints []ServiceEndpoint

	for agentID, tasks := range status {
		for _, task := range tasks {
			if task.JobName != jobName || task.State != types.TaskRunning {
				continue
			}

			endpoint := agentEndpoints[agentID]
			ip := extractIPFromEndpoint(endpoint)
			if ip != "" {
				endpoints = append(endpoints, ServiceEndpoint{
					IP:    ip,
					Ports: task.Ports,
				})
			}
		}
	}

	return endpoints
}

// extractIPFromEndpoint extracts IP from endpoint (http://ip:port -> ip)
func extractIPFromEndpoint(endpoint string) string {
	// Remove http:// or https://
	s := endpoint
	if len(s) > 7 && s[:7] == "http://" {
		s = s[7:]
	} else if len(s) > 8 && s[:8] == "https://" {
		s = s[8:]
	}

	// Find the colon for port
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[:i]
		}
	}
	return s
}

// fetchAgentTasks gets the task list from an agent
func (l *Leader) fetchAgentTasks(agent *types.Agent) ([]*types.Task, error) {
	url := fmt.Sprintf("%s/tasks", agent.Endpoint)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
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
