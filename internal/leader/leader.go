package leader

import (
	"context"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

const (
	defaultAgentTimeout       = 30 * time.Second
	defaultHealthCheckTimeout = 30 * time.Second
	deadAgentCheckInterval    = 10 * time.Second
	stateChannelBufferSize    = 64
)

var (
	// HTTPClientTimeout can be overridden in tests for faster execution
	HTTPClientTimeout = 5 * time.Second
)

// JobStore is the interface for accessing jobs (implemented by Agent)
type JobStore interface {
	GetJobs() []*types.Job
	GetJob(id string) *types.Job
	StoreJob(job *types.Job)
	DeleteJob(id string) // Remove job from store by ID
	GetStateTime() time.Time
	SyncJobs(jobs []*types.Job, updated time.Time)
}

// leaderState holds all mutable state (owned by single goroutine)
type leaderState struct {
	agents     map[string]*types.Agent
	placement  map[string][]string // jobID -> []agentID (for round-robin and delete)
	roundRobin int
}

// Leader dispatches jobs to agents and monitors health
type Leader struct {
	localAgentID string
	jobStore     JobStore

	ops chan func(*leaderState) // all state access goes through here

	httpClient   *http.Client
	agentTimeout time.Duration
}

// New creates a new leader with optional HTTP client (nil uses default)
func New(localAgentID string, jobStore JobStore, client *http.Client) *Leader {
	if client == nil {
		client = &http.Client{Timeout: HTTPClientTimeout}
	}
	return &Leader{
		localAgentID: localAgentID,
		jobStore:     jobStore,
		ops:          make(chan func(*leaderState), stateChannelBufferSize),
		agentTimeout: defaultAgentTimeout,
		httpClient:   client,
	}
}

// stateLoop is the single goroutine that owns all mutable state
func (l *Leader) stateLoop(ctx context.Context) {
	state := &leaderState{
		agents:    make(map[string]*types.Agent),
		placement: make(map[string][]string),
	}

	for {
		select {
		case <-ctx.Done():
			return
		case op := <-l.ops:
			op(state)
		}
	}
}

// do executes an operation on state (fire-and-forget)
func (l *Leader) do(op func(*leaderState)) {
	l.ops <- op
}

// query executes an operation and waits for result
func query[T any](l *Leader, fn func(*leaderState) T) T {
	result := make(chan T, 1)
	l.ops <- func(s *leaderState) {
		result <- fn(s)
	}
	return <-result
}

// Heartbeat updates agent's LastSeen and syncs jobs
// - jobs: all job definitions (for state sync if agent has newer state)
func (l *Leader) Heartbeat(id, endpoint string, jobs []*types.Job, stateTime time.Time, version string) []*types.Job {
	// Register or update agent
	isNew := query(l, func(s *leaderState) bool {
		if agent, ok := s.agents[id]; ok {
			agent.LastSeen = time.Now()
			agent.Version = version
			return false
		}
		s.agents[id] = &types.Agent{
			ID:       id,
			Endpoint: endpoint,
			Version:  version,
			LastSeen: time.Now(),
		}
		return true
	})

	// Sync job definitions if agent has newer state
	myStateTime := l.jobStore.GetStateTime()
	if stateTime.After(myStateTime) && len(jobs) > 0 {
		log.Printf("Agent %s has newer state, syncing", id)
		l.jobStore.SyncJobs(jobs, stateTime)
	}

	// New agent: reconcile all jobs (handles both count=-1 and regular jobs)
	// reconcileJobs uses GetClusterStatus to check what's actually running
	if isNew {
		l.reconcileJobs()
	}

	return l.jobStore.GetJobs()
}

// cleanPlacementForAgent removes an agent from all placement entries
func cleanPlacementForAgent(s *leaderState, agentID string) {
	for jobID, agents := range s.placement {
		newPlacement := make([]string, 0, len(agents))
		for _, id := range agents {
			if id != agentID {
				newPlacement = append(newPlacement, id)
			}
		}
		if len(newPlacement) > 0 {
			s.placement[jobID] = newPlacement
		} else {
			delete(s.placement, jobID)
		}
	}
}

// UnregisterAgent removes an agent and reconciles jobs
func (l *Leader) UnregisterAgent(id string) {
	l.do(func(s *leaderState) {
		delete(s.agents, id)
		cleanPlacementForAgent(s, id)
	})
	l.reconcileJobs()
}

// GetAgents returns all registered agents
func (l *Leader) GetAgents() []*types.Agent {
	return query(l, func(s *leaderState) []*types.Agent {
		agents := make([]*types.Agent, 0, len(s.agents))
		for _, a := range s.agents {
			agents = append(agents, a)
		}
		return agents
	})
}

// GetJobs returns all jobs
func (l *Leader) GetJobs() []*types.Job {
	return l.jobStore.GetJobs()
}

// FindJobByName finds a job by name (returns nil if not found)
func (l *Leader) FindJobByName(name string) *types.Job {
	jobs := l.jobStore.GetJobs()
	for _, job := range jobs {
		if job.Name == name {
			return job
		}
	}
	return nil
}

// GetPlacement returns which agents are running a job (for testing/debugging)
func (l *Leader) GetPlacement(jobID string) []string {
	return query(l, func(s *leaderState) []string {
		return s.placement[jobID]
	})
}

// GetStateTime returns the job store's state time
func (l *Leader) GetStateTime() time.Time {
	return l.jobStore.GetStateTime()
}

