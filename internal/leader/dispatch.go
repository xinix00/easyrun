package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"easyrun/internal/types"
	"github.com/google/uuid"
)

// Sentinel errors returned by sendJobToAgent — lets callers distinguish why an agent rejected.
var (
	errAffinityMismatch = errors.New("affinity mismatch") // 406: agent can never run this job
	errNoCapacity       = errors.New("no capacity")       // 503: agent is full but affinity is ok
)

// effectivePriority returns the sort key: lower = more important.
// nil (not set) sorts last. 0 = top (most important).
func effectivePriority(p *int) int {
	if p == nil {
		return math.MaxInt
	}
	return *p
}

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

	var err error
	if job.Count == -1 {
		err = l.reconcileJob(job, l.GetAgents()) // daemon: same path as reconciliation
	} else {
		err = l.dispatchInstances(job, job.Count)
	}
	if err != nil {
		log.Printf("Job %s stored but dispatch failed: %v (will retry on reconciliation)", job.Name, err)
		return err
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

	// Atomically check-and-set dispatching flag to prevent concurrent dispatch
	// of the same job from two simultaneous reconcileJobs goroutines.
	alreadyDispatching := query(l, func(s *leaderState) bool {
		if s.dispatching[job.ID] {
			return true
		}
		s.dispatching[job.ID] = true
		return false
	})
	if alreadyDispatching {
		return nil
	}
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
// First pass: round-robin over all agents.
// Second pass: preemption — evict lowest-priority job from capacity-failed agents.
// Agents that returned 406 (affinity mismatch) are never preemption candidates.
func (l *Leader) dispatchToAvailableAgent(job *types.Job) error {
	agentCount := query(l, func(s *leaderState) int { return len(s.agents) })
	if agentCount == 0 {
		return fmt.Errorf("no agents available")
	}

	// First pass: try every agent once via round-robin.
	var capacityCandidates []*types.Agent
	for range agentCount {
		agent := l.nextAgent()
		if agent == nil {
			break
		}
		err := l.sendJobToAgent(agent, job)
		if err == nil {
			l.trackPlacement(agent.ID, job.ID)
			return nil
		}
		if errors.Is(err, errNoCapacity) {
			capacityCandidates = append(capacityCandidates, agent)
			log.Printf("Agent %s at capacity for job %s, trying next agent", agent.ID, job.Name)
		} else if !errors.Is(err, errAffinityMismatch) {
			// errAffinityMismatch: agent can never run this job — skip silently.
			log.Printf("Agent %s rejected job %s: %v, trying next agent", agent.ID, job.Name, err)
		}
	}

	// Second pass: preemption on capacity-failed agents only.
	for _, agent := range capacityCandidates {
		victim := l.findVictim(agent.ID, job.Priority)
		if victim == nil {
			continue
		}
		log.Printf("Preempting job %s (prio %d) on %s to make room for %s (prio %d)",
			victim.Name, effectivePriority(victim.Priority), agent.ID,
			job.Name, effectivePriority(job.Priority))
		l.stopTasksOnAgent(agent, victim.ID)
		l.do(func(s *leaderState) { delete(s.placed[agent.ID], victim.ID) })
		if err := l.sendJobToAgent(agent, job); err == nil {
			l.trackPlacement(agent.ID, job.ID)
			l.eventBus.Notify("job:" + victim.Name)
			return nil
		}
	}

	return fmt.Errorf("no agent has capacity for %s after trying %d agents", job.Name, agentCount)
}

// findVictim returns the lowest-priority job placed on agentID that has lower
// priority than jobPriority. Returns nil if no such job exists.
func (l *Leader) findVictim(agentID string, jobPriority *int) *types.Job {
	return query(l, func(s *leaderState) *types.Job {
		var victim *types.Job
		worstPrio := effectivePriority(jobPriority) // only evict jobs strictly less important
		for jobID, count := range s.placed[agentID] {
			if count <= 0 {
				continue
			}
			j := l.jobStore.GetJob(jobID)
			if j == nil {
				continue
			}
			if ep := effectivePriority(j.Priority); ep > worstPrio {
				victim = j
				worstPrio = ep
			}
		}
		return victim
	})
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
		go func() {
			defer wg.Done()
			l.deleteTaskOnAgent(agent, job.ID)
		}()
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

// DeleteJob deletes a job by ID (or name as fallback for API/CLI compatibility).
func (l *Leader) DeleteJob(idOrName string) {
	job := l.jobStore.GetJob(idOrName)
	if job == nil {
		job = l.FindJobByName(idOrName)
	}
	if job == nil {
		log.Printf("Job %s not found for deletion", idOrName)
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

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted:
		// success
	case http.StatusNotAcceptable: // 406: affinity mismatch — agent can never run this job
		return errAffinityMismatch
	case http.StatusServiceUnavailable: // 503: capacity full — agent could run it later
		return errNoCapacity
	default:
		return fmt.Errorf("agent %s returned status %d", agent.ID, resp.StatusCode)
	}

	log.Printf("Job %s dispatched to agent %s", job.Name, agent.ID)
	return nil
}
