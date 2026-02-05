package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

const (
	defaultInitialTimeout = 30 * time.Second
	verifyInterval        = 500 * time.Millisecond
)

// DispatchJob sends a job to agents (Count times with round-robin spreading)
// count=-1 means run on ALL agents (exactly once per agent)
func (l *Leader) DispatchJob(job *types.Job) error {
	var count int
	var targetAgents []*types.Agent

	if job.Count == -1 {
		// Dispatch to ALL agents (specific list, no round-robin)
		targetAgents = l.GetAgents()
		count = len(targetAgents)
	} else {
		// Normal count: use round-robin (agents selected dynamically)
		count = job.Count
		if count <= 0 {
			count = 1
		}
	}

	// Dispatch to agents
	succeeded := 0
	for i := 0; i < count; i++ {
		var agent *types.Agent
		if targetAgents != nil {
			// count=-1: use specific agent from list
			agent = targetAgents[i]
		}

		if err := l.dispatchToAgent(agent, job); err != nil {
			log.Printf("Failed to dispatch instance %d/%d: %v", i+1, count, err)
			if targetAgents == nil {
				// Normal count: fail if can't dispatch
				return fmt.Errorf("failed to dispatch instance %d/%d: %w", i+1, count, err)
			}
			// count=-1: skip failed agent, continue
			continue
		}
		succeeded++
	}

	if succeeded == 0 && count > 0 {
		return fmt.Errorf("all agents rejected job")
	}

	l.jobStore.StoreJob(job)
	return nil
}

// dispatchToAgent dispatches to specific agent (count=-1) or finds one via round-robin (normal count)
func (l *Leader) dispatchToAgent(agent *types.Agent, job *types.Job) error {
	if agent == nil {
		// No specific agent: find one via round-robin
		return l.dispatchToAvailableAgent(job)
	}

	// Specific agent: dispatch directly
	if err := l.sendJobToAgent(agent, job); err != nil {
		return err
	}

	l.do(func(s *leaderState) {
		s.placement[job.ID] = append(s.placement[job.ID], agent.ID)
	})
	return nil
}

// dispatchToAvailableAgent tries agents until one accepts the job AND task is running
func (l *Leader) dispatchToAvailableAgent(job *types.Job) error {
	// If job is pinned to specific node, dispatch only to that node
	if job.AgentID != "" {
		agent := query(l, func(s *leaderState) *types.Agent {
			return s.agents[job.AgentID]
		})

		if agent == nil {
			return fmt.Errorf("node %s not found (job %s requires this node)", job.AgentID, job.Name)
		}

		if err := l.sendJobToAgent(agent, job); err != nil {
			return fmt.Errorf("node %s rejected job %s: %w", job.AgentID, job.Name, err)
		}

		l.do(func(s *leaderState) {
			s.placement[job.ID] = append(s.placement[job.ID], agent.ID)
		})
		return nil
	}

	// No node constraint, try all agents
	agentCount := query(l, func(s *leaderState) int {
		return len(s.agents)
	})

	if agentCount == 0 {
		return fmt.Errorf("no agents available")
	}

	tried := 0
	maxTries := agentCount + 1

	for tried < maxTries {
		agent := l.nextAgent()
		if agent == nil {
			return fmt.Errorf("no agents available")
		}

		if err := l.sendJobToAgent(agent, job); err != nil {
			log.Printf("Agent %s rejected job %s: %v, trying next agent", agent.ID, job.Name, err)
			tried++
			continue
		}

		l.do(func(s *leaderState) {
			s.placement[job.ID] = append(s.placement[job.ID], agent.ID)
		})
		return nil
	}

	return fmt.Errorf("no agent has capacity after trying %d agents", tried)
}


// DeleteJobByID sends delete requests to all agents running instances of this job
// Uses job.ID for placement tracking, job.Name for agent communication
func (l *Leader) DeleteJobByID(job *types.Job) {
	agents := query(l, func(s *leaderState) []*types.Agent {
		agentIDs := s.placement[job.ID]
		delete(s.placement, job.ID)

		var result []*types.Agent
		for _, id := range agentIDs {
			if a := s.agents[id]; a != nil {
				result = append(result, a)
			}
		}
		return result
	})

	// Delete on all agents (if any)
	if len(agents) > 0 {
		ctx := context.Background()
		for _, agent := range agents {
			url := fmt.Sprintf("%s/delete/%s", agent.Endpoint, job.Name)
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
			if err != nil {
				log.Printf("Failed to create delete request for agent %s: %v", agent.ID, err)
				continue
			}
			if _, err := l.httpClient.Do(req); err != nil {
				log.Printf("Failed to delete job %s on agent %s: %v", job.Name, agent.ID, err)
			}
		}
		log.Printf("Deleted job %s (ID %s) from %d agents", job.Name, job.ID, len(agents))
	}

	// Always remove from job store (even if no agents)
	l.jobStore.DeleteJob(job.Name)
}

// DeleteJob finds a job by name and deletes it (for API compatibility)
func (l *Leader) DeleteJob(jobName string) {
	job := l.FindJobByName(jobName)
	if job == nil {
		log.Printf("Job %s not found for deletion", jobName)
		return
	}
	l.DeleteJobByID(job)
}

// nextAgent returns the next agent in round-robin order
func (l *Leader) nextAgent() *types.Agent {
	return query(l, func(s *leaderState) *types.Agent {
		if len(s.agents) == 0 {
			return nil
		}

		var agents []*types.Agent
		for _, a := range s.agents {
			agents = append(agents, a)
		}

		idx := s.roundRobin % len(agents)
		s.roundRobin++

		return agents[idx]
	})
}

// sendJobToAgent sends a job to a specific agent and waits until task is running
func (l *Leader) sendJobToAgent(agent *types.Agent, job *types.Job) error {
	url := fmt.Sprintf("%s/run", agent.Endpoint)

	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to contact agent %s: %w", agent.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("agent %s returned status %d", agent.ID, resp.StatusCode)
	}

	// Wait until task is actually running
	timeout := defaultInitialTimeout
	if job.HealthCheck != nil && job.HealthCheck.InitialTimeout > 0 {
		timeout = job.HealthCheck.InitialTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(verifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("task didn't start on agent %s (timeout)", agent.ID)
		case <-ticker.C:
			tasks, err := l.fetchAgentTasks(ctx, agent)
			if err == nil {
				for _, task := range tasks {
					if task.JobName == job.Name && task.State == types.TaskRunning {
						log.Printf("Job %s dispatched to agent %s", job.Name, agent.ID)
						return nil
					}
				}
			}
		}
	}
}
