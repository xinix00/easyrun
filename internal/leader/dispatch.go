package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
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

// DispatchJob stores a job and sends it to agents.
// count=-1 means run on ALL agents (exactly once per agent)
// The job is ALWAYS stored, even if dispatch fails (reconciliation will retry later).
func (l *Leader) DispatchJob(job *types.Job) error {
	if job.ID == "" {
		job.ID = generateID()
	}

	// Always store the job first — even if no agents have capacity now,
	// reconciliation will pick it up when capacity becomes available.
	l.jobStore.StoreJob(job)
	l.do(func(s *leaderState) { s.nameToID[job.Name] = job.ID })

	// During settle period: reconcileJobs after settle will dispatch
	settled := query(l, func(s *leaderState) bool { return s.settled })
	if !settled {
		log.Printf("Job %s stored (leader settling, dispatch deferred)", job.Name)
		return nil
	}

	if job.Count == -1 {
		// Daemon: use reconcile-based dispatch (same path as reconciliation)
		agents := l.GetAgents()
		if err := l.reconcileJob(job, agents); err != nil {
			log.Printf("Job %s stored but dispatch failed: %v (will retry on reconciliation)", job.Name, err)
			return err
		}
	} else {
		if err := l.dispatchInstances(job, job.Count); err != nil {
			log.Printf("Job %s stored but dispatch failed: %v (will retry on reconciliation)", job.Name, err)
			return err
		}
	}

	l.eventBus.Notify("job:" + job.Name)
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

// trackPlacement records that an agent is running an instance of a job
func (l *Leader) trackPlacement(agentID, jobID string) {
	l.do(func(s *leaderState) {
		if s.placed[agentID] == nil {
			s.placed[agentID] = make(map[string]int)
		}
		s.placed[agentID][jobID]++
	})
}

// dispatchToAvailableAgent tries agents until one accepts the job.
// Affinity is checked agent-side (agent rejects with 406 if no match).
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

		if err := l.sendJobToAgent(agent, job); err != nil {
			log.Printf("Agent %s rejected job %s: %v, trying next agent", agent.ID, job.Name, err)
			tried++
			continue
		}

		l.trackPlacement(agent.ID, job.ID)
		return nil
	}

	return fmt.Errorf("no agent has capacity after trying %d agents", tried)
}


// DeleteJobByID sends delete requests to all agents in parallel, waits for
// all stops to complete, then reconciles so freed capacity is immediately usable.
func (l *Leader) DeleteJobByID(job *types.Job) {
	agents := query(l, func(s *leaderState) []*types.Agent {
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

	// Delete on all agents in parallel (each agent blocks until stops complete)
	var wg sync.WaitGroup
	for _, agent := range agents {
		wg.Add(1)
		go func(a *types.Agent) {
			defer wg.Done()
			l.deleteTaskOnAgent(a, job.ID)
		}(agent)
	}
	wg.Wait()

	if len(agents) > 0 {
		log.Printf("Deleted job %s (ID %s) from %d agents", job.Name, job.ID, len(agents))
	}

	l.jobStore.DeleteJob(job.ID)
	l.do(func(s *leaderState) {
		if s.nameToID[job.Name] == job.ID {
			delete(s.nameToID, job.Name)
		}
	})

	// Reconcile immediately — all capacity is freed
	if len(agents) > 0 {
		l.reconcileJobs()
	}
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
		if len(s.agentsSorted) == 0 {
			return nil
		}
		idx := s.roundRobin % len(s.agentsSorted)
		s.roundRobin++
		return s.agentsSorted[idx]
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
	if l.apiKey != "" {
		req.Header.Set("X-API-Key", l.apiKey)
	}

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
