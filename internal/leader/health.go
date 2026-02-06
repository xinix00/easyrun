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

// checkDeadAgents removes agents that haven't sent heartbeat and reconciles jobs
func (l *Leader) checkDeadAgents() {
	hadDead := query(l, func(s *leaderState) bool {
		hadDead := false
		for id, agent := range s.agents {
			if time.Since(agent.LastSeen) > l.agentTimeout {
				log.Printf("Agent %s is dead (no heartbeat for %v)", id, time.Since(agent.LastSeen))
				delete(s.agents, id)
				cleanPlacementForAgent(s, id)
				hadDead = true
			}
		}
		return hadDead
	})

	// If any agents died, reconcile all jobs
	if hadDead {
		l.reconcileJobs()
	}
}

// reconcileJobs ensures all jobs have the correct number of running instances.
func (l *Leader) reconcileJobs() {
	jobs := l.jobStore.GetJobs()
	if len(jobs) == 0 {
		return
	}

	agents := l.GetAgents()
	if len(agents) == 0 {
		return
	}

	status := l.GetClusterStatus()

	for _, job := range jobs {
		if job.ID == "" {
			continue
		}
		if err := l.reconcileJob(job, status, agents); err != nil {
			log.Printf("Failed to reconcile job %s: %v", job.Name, err)
		}
	}
}

// reconcileJob ensures a single job has the correct instances running.
// Daemon jobs (count=-1): dispatch to all agents missing it, rebuild placement atomically.
// Regular jobs: dispatch missing instances via round-robin.
func (l *Leader) reconcileJob(job *types.Job, status map[string][]*types.Task, agents []*types.Agent) error {
	// Count running instances per agent
	agentHasJob := make(map[string]bool)
	running := 0
	for agentID, tasks := range status {
		for _, task := range tasks {
			if task.JobName == job.Name && task.State == types.TaskRunning {
				agentHasJob[agentID] = true
				running++
			}
		}
	}

	if job.Count == -1 {
		// Daemon: run on ALL agents, rebuild placement atomically
		var newPlacement []string
		for _, agent := range agents {
			if agentHasJob[agent.ID] {
				newPlacement = append(newPlacement, agent.ID)
				continue
			}
			log.Printf("Reconciling daemon %s: dispatching to %s", job.Name, agent.ID)
			if err := l.sendJobToAgent(agent, job); err != nil {
				log.Printf("Failed to dispatch daemon %s to %s: %v", job.Name, agent.ID, err)
			} else {
				newPlacement = append(newPlacement, agent.ID)
			}
		}
		jobID := job.ID
		l.do(func(s *leaderState) {
			s.placement[jobID] = newPlacement
		})
		if len(newPlacement) == 0 && len(agents) > 0 {
			return fmt.Errorf("all agents rejected daemon job %s", job.Name)
		}
		return nil
	}

	// Regular: dispatch missing instances
	desired := job.Count
	if desired <= 0 {
		desired = 1
	}
	missing := desired - running
	if missing > 0 {
		log.Printf("Reconciling job %s: %d/%d running, dispatching %d", job.Name, running, desired, missing)
		return l.dispatchInstances(job, missing)
	}
	return nil
}

// GetClusterStatus fetches status from all agents (parallel, no goroutine leaks!)
func (l *Leader) GetClusterStatus() map[string][]*types.Task {
	agents := l.GetAgents()
	result := make(map[string][]*types.Task)

	if len(agents) == 0 {
		return result
	}

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
				resultCh <- agentResult{agentID: a.ID, tasks: nil}
				return
			}
			resultCh <- agentResult{agentID: a.ID, tasks: tasks}
		}(agent)
	}

	// Collect results (context cancellation stops all goroutines)
	for i := 0; i < len(agents); i++ {
		select {
		case res := <-resultCh:
			if res.tasks != nil {
				result[res.agentID] = res.tasks
			}
		case <-ctx.Done():
			log.Printf("GetClusterStatus timeout after collecting %d/%d agents", len(result), len(agents))
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
