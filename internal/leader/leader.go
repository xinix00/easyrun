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
	// HTTPClientTimeout can be overridden in tests for faster execution.
	HTTPClientTimeout = 5 * time.Second

	// DeleteClientTimeout is used for delete operations which may take longer
	// (Docker stop: ~10s SIGTERM + 10s SIGKILL worst case).
	DeleteClientTimeout = 60 * time.Second
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
	agents      map[string]*types.Agent
	placed      map[string]map[string]int // agentID -> jobID -> count
	dispatching map[string]bool           // jobID -> true if actively being dispatched
	settled     bool                      // false during settle period after leader election
	roundRobin  int
}

// Leader dispatches jobs to agents and monitors health
type Leader struct {
	localAgentID string
	jobStore     JobStore

	ops chan func(*leaderState) // all state access goes through here

	httpClient   *http.Client
	agentTimeout time.Duration
	settleDelay  time.Duration // wait before first reconciliation (0 = settled immediately)
	eventBus     *EventBus
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
		eventBus:     NewEventBus(),
	}
}

// EnableSettle enables settle period (waits agentTimeout before first reconciliation)
func (l *Leader) EnableSettle() {
	l.settleDelay = l.agentTimeout
}

// SetSettleDelay sets a custom settle delay (for tests; production should use EnableSettle)
func (l *Leader) SetSettleDelay(d time.Duration) {
	l.settleDelay = d
}

// stateLoop is the single goroutine that owns all mutable state
func (l *Leader) stateLoop(ctx context.Context) {
	state := &leaderState{
		agents:      make(map[string]*types.Agent),
		placed:      make(map[string]map[string]int),
		dispatching: make(map[string]bool),
		settled:     l.settleDelay == 0,
	}

	var settleTimer <-chan time.Time
	if l.settleDelay > 0 {
		settleTimer = time.After(l.settleDelay)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case op := <-l.ops:
			op(state)
		case <-settleTimer:
			state.settled = true
			settleTimer = nil
			log.Printf("Leader settled after %v, reconciling jobs", l.settleDelay)
			go l.reconcileJobs()
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

// Heartbeat updates agent's LastSeen and syncs jobs.
// Does NOT trigger reconciliation — that's RegisterAgent's job.
// Returns (nil, false) if agent is unknown — caller should return 404 to force re-register.
func (l *Leader) Heartbeat(id, endpoint string, jobs []*types.Job, stateTime time.Time, version string) ([]*types.Job, bool) {
	known := query(l, func(s *leaderState) bool {
		if agent, ok := s.agents[id]; ok {
			agent.LastSeen = time.Now()
			agent.Version = version
			return true
		}
		return false
	})

	if !known {
		return nil, false
	}

	// Sync job definitions if agent has newer state
	myStateTime := l.jobStore.GetStateTime()
	if stateTime.After(myStateTime) && len(jobs) > 0 {
		log.Printf("Agent %s has newer state, syncing", id)
		l.jobStore.SyncJobs(jobs, stateTime)
	}

	return l.jobStore.GetJobs(), true
}

// RegisterAgent registers a (re)starting agent. Clears old state and triggers reconciliation.
// Called on agent startup and on leader change via POST /v1/agents.
// Placed counts tell the leader what this agent is already running.
func (l *Leader) RegisterAgent(id, endpoint, version string, placed map[string]int) {
	shouldReconcile := query(l, func(s *leaderState) bool {
		// Clear stale state from previous incarnation
		delete(s.agents, id)
		delete(s.placed, id)

		s.agents[id] = &types.Agent{
			ID:       id,
			Endpoint: endpoint,
			Version:  version,
			LastSeen: time.Now(),
		}
		if placed != nil {
			s.placed[id] = placed
		}
		return s.settled
	})

	if shouldReconcile {
		log.Printf("Agent %s registered, reconciling jobs", id)
		l.reconcileJobs()
	}
}

// UnregisterAgent removes an agent and reconciles jobs
func (l *Leader) UnregisterAgent(id string) {
	l.do(func(s *leaderState) {
		delete(s.agents, id)
		delete(s.placed, id)
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

// GetPlaced returns which agents have a job placed and how many (for testing/debugging)
func (l *Leader) GetPlaced(jobID string) map[string]int {
	return query(l, func(s *leaderState) map[string]int {
		result := make(map[string]int)
		for agentID, jobs := range s.placed {
			if count := jobs[jobID]; count > 0 {
				result[agentID] = count
			}
		}
		return result
	})
}

// GetStateTime returns the job store's state time
func (l *Leader) GetStateTime() time.Time {
	return l.jobStore.GetStateTime()
}

// IsSettled returns whether the leader has finished its settle period
func (l *Leader) IsSettled() bool {
	return query(l, func(s *leaderState) bool { return s.settled })
}

// EventBus returns the leader's event bus for SSE subscribers
func (l *Leader) EventBus() *EventBus {
	return l.eventBus
}

