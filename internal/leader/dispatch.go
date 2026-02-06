package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"easyrun/internal/types"
	"github.com/google/uuid"
)

var (
	// VerifyInterval can be overridden in tests for faster execution
	VerifyInterval = 500 * time.Millisecond
)

func generateID() string {
	return uuid.New().String()
}

// DispatchJob sends a job to agents and stores it.
// count=-1 means run on ALL agents (exactly once per agent)
func (l *Leader) DispatchJob(job *types.Job) error {
	if job.ID == "" {
		job.ID = generateID()
	}

	// During settle period: just store, reconcileJobs after settle will dispatch
	settled := query(l, func(s *leaderState) bool { return s.settled })
	if !settled {
		l.jobStore.StoreJob(job)
		log.Printf("Job %s stored (leader settling, dispatch deferred)", job.Name)
		return nil
	}

	if job.Count == -1 {
		// Daemon: use reconcile-based dispatch (same path as reconciliation)
		agents := l.GetAgents()
		if err := l.reconcileJob(job, agents); err != nil {
			return err
		}
	} else {
		if err := l.dispatchInstances(job, job.Count); err != nil {
			return err
		}
	}

	l.jobStore.StoreJob(job)
	return nil
}

// dispatchInstances dispatches N instances of a job via round-robin.
// Daemon jobs (count=-1) use reconcileJob instead.
func (l *Leader) dispatchInstances(job *types.Job, count int) error {
	if count <= 0 {
		count = 1
	}

	// Mark as actively dispatching so reconcileJob skips this job
	l.do(func(s *leaderState) { s.dispatching[job.ID] = true })
	defer l.do(func(s *leaderState) { delete(s.dispatching, job.ID) })

	for i := 0; i < count; i++ {
		if err := l.dispatchToAvailableAgent(job); err != nil {
			return fmt.Errorf("failed to dispatch instance %d/%d: %w", i+1, count, err)
		}
	}
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
		if s.placed[agent.ID] == nil {
			s.placed[agent.ID] = make(map[string]int)
		}
		s.placed[agent.ID][job.ID]++
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

		return l.dispatchToAgent(agent, job)
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
			if s.placed[agent.ID] == nil {
				s.placed[agent.ID] = make(map[string]int)
			}
			s.placed[agent.ID][job.ID]++
		})
		return nil
	}

	return fmt.Errorf("no agent has capacity after trying %d agents", tried)
}


// DeleteJobByID sends delete requests to all agents running instances of this job.
// Uses placed to find agents, plus GetClusterStatus as safety net for orphaned tasks.
func (l *Leader) DeleteJobByID(job *types.Job) {
	// Phase 1: Get agents from placed (atomic read + clear)
	placedAgents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for agentID, jobs := range s.placed {
			if jobs[job.ID] > 0 {
				if a := s.agents[agentID]; a != nil {
					result = append(result, a)
				}
				delete(jobs, job.ID)
			}
		}
		return result
	})

	// Phase 2: Check cluster status for orphaned tasks not in placed
	status := l.GetClusterStatus()
	seen := make(map[string]bool)
	for _, a := range placedAgents {
		seen[a.ID] = true
	}

	var extraAgents []*types.Agent
	for agentID, tasks := range status {
		if seen[agentID] {
			continue
		}
		for _, task := range tasks {
			if task.JobID == job.ID {
				agent := query(l, func(s *leaderState) *types.Agent {
					return s.agents[agentID]
				})
				if agent != nil {
					extraAgents = append(extraAgents, agent)
				}
				break
			}
		}
	}

	// Send delete to union of both sets
	allAgents := append(placedAgents, extraAgents...)
	if len(allAgents) > 0 {
		ctx := context.Background()
		for _, agent := range allAgents {
			url := fmt.Sprintf("%s/delete/%s", agent.Endpoint, job.ID)
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
			if err != nil {
				log.Printf("Failed to create delete request for agent %s: %v", agent.ID, err)
				continue
			}
			if _, err := l.httpClient.Do(req); err != nil {
				log.Printf("Failed to delete job %s on agent %s: %v", job.Name, agent.ID, err)
			}
		}
		log.Printf("Deleted job %s (ID %s) from %d agents", job.Name, job.ID, len(allAgents))
	}

	l.jobStore.DeleteJob(job.ID)
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
		sort.Slice(agents, func(i, j int) bool {
			return agents[i].ID < agents[j].ID
		})

		idx := s.roundRobin % len(agents)
		s.roundRobin++

		return agents[idx]
	})
}

// sendJobToAgent sends a job to a specific agent.
// Agent accepts (2xx) or rejects (non-2xx) based on capacity. No polling needed.
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("agent %s returned status %d", agent.ID, resp.StatusCode)
	}

	log.Printf("Job %s dispatched to agent %s", job.Name, agent.ID)
	return nil
}
