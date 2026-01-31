package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"easyrun/internal/types"
)

// DispatchJob sends a job to agents (Count times with round-robin spreading)
func (l *Leader) DispatchJob(job *types.Job) error {
	count := job.Count
	if count <= 0 {
		count = 1
	}

	// Dispatch Count instances
	for i := 0; i < count; i++ {
		if err := l.dispatchToAvailableAgent(job); err != nil {
			return fmt.Errorf("failed to dispatch instance %d/%d: %w", i+1, count, err)
		}
	}

	l.jobStore.StoreJob(job)
	return nil
}

// dispatchToAvailableAgent tries agents until one accepts the job
func (l *Leader) dispatchToAvailableAgent(job *types.Job) error {
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

		err := l.sendJobToAgent(agent, job)
		if err == nil {
			// Success! Track placement
			l.do(func(s *leaderState) {
				s.placement[job.ID] = append(s.placement[job.ID], agent.ID)
			})
			return nil
		}

		log.Printf("Agent %s rejected job %s: %v, trying next agent", agent.ID, job.ID, err)
		tried++
	}

	return fmt.Errorf("no agent has capacity after trying %d agents", tried)
}

// StopJob sends stop requests to all agents running instances of this job
func (l *Leader) StopJob(jobID string) {
	// Get agents and clear placement
	agentIDs := query(l, func(s *leaderState) []string {
		ids := s.placement[jobID]
		delete(s.placement, jobID)
		return ids
	})

	if len(agentIDs) == 0 {
		return
	}

	// Get agent endpoints
	agents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for _, id := range agentIDs {
			if a := s.agents[id]; a != nil {
				result = append(result, a)
			}
		}
		return result
	})

	// Stop on all agents (outside state loop - HTTP can block)
	ctx := context.Background()
	for _, agent := range agents {
		url := fmt.Sprintf("%s/stop/%s", agent.Endpoint, jobID)
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			log.Printf("Failed to create stop request for agent %s: %v", agent.ID, err)
			continue
		}
		if _, err := l.httpClient.Do(req); err != nil {
			log.Printf("Failed to stop job %s on agent %s: %v", jobID, agent.ID, err)
		}
	}
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

// sendJobToAgent sends a job to a specific agent
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

	log.Printf("Job %s dispatched to agent %s", job.ID, agent.ID)
	return nil
}
