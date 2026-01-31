package leader

import (
	"bytes"
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
	l.agentsMu.RLock()
	agentCount := len(l.agents)
	l.agentsMu.RUnlock()

	if agentCount == 0 {
		return fmt.Errorf("no agents available")
	}

	tried := 0
	maxTries := agentCount + 1 // try all agents at least once

	for tried < maxTries {
		agent := l.nextAgent()
		if agent == nil {
			return fmt.Errorf("no agents available")
		}

		err := l.sendJobToAgent(agent, job)
		if err == nil {
			// Success! Track placement
			l.placementMu.Lock()
			l.placement[job.ID] = append(l.placement[job.ID], agent.ID)
			l.placementMu.Unlock()
			return nil
		}

		// Agent said no or error, try next
		log.Printf("Agent %s rejected job %s: %v, trying next agent", agent.ID, job.ID, err)
		tried++
	}

	return fmt.Errorf("no agent has capacity after trying %d agents", tried)
}

// StopJob sends stop requests to all agents running instances of this job
func (l *Leader) StopJob(jobID string) {
	l.placementMu.Lock()
	agentIDs := l.placement[jobID]
	delete(l.placement, jobID)
	l.placementMu.Unlock()

	// Note: we don't remove from jobStore - agent handles that

	if len(agentIDs) == 0 {
		return
	}

	// Stop on all agents
	for _, agentID := range agentIDs {
		l.agentsMu.RLock()
		agent := l.agents[agentID]
		l.agentsMu.RUnlock()

		if agent == nil {
			continue
		}

		url := fmt.Sprintf("%s/stop/%s", agent.Endpoint, jobID)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		l.httpClient.Do(req)
	}
}

// nextAgent returns the next agent in round-robin order
func (l *Leader) nextAgent() *types.Agent {
	l.agentsMu.RLock()
	defer l.agentsMu.RUnlock()

	if len(l.agents) == 0 {
		return nil
	}

	var agents []*types.Agent
	for _, a := range l.agents {
		agents = append(agents, a)
	}

	l.rrMu.Lock()
	idx := l.roundRobin % len(agents)
	l.roundRobin++
	l.rrMu.Unlock()

	return agents[idx]
}

// sendJobToAgent sends a job to a specific agent
func (l *Leader) sendJobToAgent(agent *types.Agent, job *types.Job) error {
	url := fmt.Sprintf("%s/run", agent.Endpoint)

	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	resp, err := l.httpClient.Post(url, "application/json", bytes.NewReader(body))
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
