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

	"easyrun/internal/types"
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


// DispatchJob stores a job and sends it to agents.
// count=-1 means run on ALL agents (exactly once per agent)
// The job is ALWAYS stored, even if dispatch fails (reconciliation will retry later).
func (l *Leader) DispatchJob(job *types.Job) error {
	if job.Name == "" {
		return fmt.Errorf("job name required")
	}

	// Always store the job first — even if no agents have capacity now,
	// reconciliation will pick it up when capacity becomes available.
	l.jobStore.StoreJob(job)

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
		if s.dispatching[job.Name] {
			return true
		}
		s.dispatching[job.Name] = true
		return false
	})
	if alreadyDispatching {
		return nil
	}
	defer l.do(func(s *leaderState) { delete(s.dispatching, job.Name) })

	for i := 0; i < count; i++ {
		if err := l.dispatchToAvailableAgent(job); err != nil {
			return fmt.Errorf("failed to dispatch instance %d/%d: %w", i+1, count, err)
		}
	}
	return nil
}

// trackPlacement records that an agent is running an instance of a job
func (l *Leader) trackPlacement(agentID, jobName string) {
	l.do(func(s *leaderState) {
		if s.placed[agentID] == nil {
			s.placed[agentID] = make(map[string]int)
		}
		s.placed[agentID][jobName]++
	})
}

// dispatchToAvailableAgent tries agents until one accepts the job.
// First pass: round-robin over all agents.
// Second pass: preemption — evict lowest-priority job from capacity-failed agents.
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
			l.trackPlacement(agent.ID, job.Name)
			return nil
		}
		if errors.Is(err, errNoCapacity) {
			capacityCandidates = append(capacityCandidates, agent)
			log.Printf("Agent %s at capacity for job %s, trying next agent", agent.ID, job.Name)
		} else if !errors.Is(err, errAffinityMismatch) {
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
		if !l.stopTasksOnAgent(agent, victim.Name) {
			continue // stop failed, tasks still running — try next agent
		}
		l.do(func(s *leaderState) { delete(s.placed[agent.ID], victim.Name) })
		if err := l.sendJobToAgent(agent, job); err == nil {
			l.trackPlacement(agent.ID, job.Name)
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
		for jobName, count := range s.placed[agentID] {
			if count <= 0 {
				continue
			}
			j := l.jobStore.GetJob(jobName)
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

// DeleteJobByName deletes a job by name: sends delete requests to all agents in parallel,
// waits for all stops to complete, then reconciles so freed capacity is immediately usable.
func (l *Leader) DeleteJobByName(name string) {
	if l.jobStore.GetJob(name) == nil {
		log.Printf("Job %s not found for deletion", name)
		return
	}

	agents := query(l, func(s *leaderState) []*types.Agent {
		var result []*types.Agent
		for agentID, jobs := range s.placed {
			if jobs[name] > 0 {
				if a := s.agents[agentID]; a != nil {
					result = append(result, a)
				}
				delete(jobs, name)
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
			l.deleteTaskOnAgent(agent, name)
		}()
	}
	wg.Wait()

	if len(agents) > 0 {
		log.Printf("Deleted job %s from %d agents", name, len(agents))
	}

	l.jobStore.DeleteJob(name)

	// Reconcile immediately — frees capacity and renormalizes priorities (0..N-1)
	go l.reconcileJobs()
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

// stopTasksOnAgent stops tasks for a job on a specific agent WITHOUT removing the job definition.
// Used for preemption and rolling updates. Returns true if the stop was confirmed successful.
func (l *Leader) stopTasksOnAgent(agent *types.Agent, jobName string) bool {
	url := fmt.Sprintf("%s/stop/%s", agent.Endpoint, jobName)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		log.Printf("Failed to create stop request for %s on %s: %v", jobName, agent.ID, err)
		return false
	}
	if l.apiKey != "" {
		req.Header.Set("X-API-Key", l.apiKey)
	}
	resp, err := l.deleteClient.Do(req)
	if err != nil {
		log.Printf("Failed to stop %s on %s: %v", jobName, agent.ID, err)
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// stopTaskByID stops a single specific task on an agent by task ID.
// Used for rolling and blue-green updates to stop precise old instances.
func (l *Leader) stopTaskByID(agent *types.Agent, taskID string) {
	url := fmt.Sprintf("%s/stop-task/%s", agent.Endpoint, taskID)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		log.Printf("Failed to create stop-task request for %s on %s: %v", taskID, agent.ID, err)
		return
	}
	if l.apiKey != "" {
		req.Header.Set("X-API-Key", l.apiKey)
	}
	resp, err := l.deleteClient.Do(req)
	if err != nil {
		log.Printf("Failed to stop task %s on %s: %v", taskID, agent.ID, err)
		return
	}
	resp.Body.Close()
}

// deleteTaskOnAgent deletes a job on specific agent (by job name).
func (l *Leader) deleteTaskOnAgent(agent *types.Agent, jobName string) {
	url := fmt.Sprintf("%s/delete/%s", agent.Endpoint, jobName)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Printf("Failed to create delete request for %s on %s: %v", jobName, agent.ID, err)
		return
	}
	if l.apiKey != "" {
		req.Header.Set("X-API-Key", l.apiKey)
	}
	resp, err := l.deleteClient.Do(req)
	if err != nil {
		log.Printf("Failed to delete %s on %s: %v", jobName, agent.ID, err)
		return
	}
	resp.Body.Close()
}
