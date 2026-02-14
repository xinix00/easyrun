package leader

import (
	"context"
	"log"
	"net/http"
	"sort"
	"time"

	"easyrun/internal/types"
)

const (
	defaultAgentTimeout       = 30 * time.Second
	defaultHealthCheckTimeout = 30 * time.Second
	deadAgentCheckInterval    = 10 * time.Second
	stateChannelBufferSize    = 256
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
	agents       map[string]*types.Agent
	agentsSorted []*types.Agent            // cached sorted agent list, rebuilt on mutation
	placed       map[string]map[string]int // agentID -> jobID -> count
	dispatching  map[string]bool           // jobID -> true if actively being dispatched
	nameToID     map[string]string         // job name -> job ID index for O(1) lookup
	settled      bool                      // false during settle period after leader election
	roundRobin   int
}

// rebuildSortedAgents rebuilds the cached sorted agent list from the agents map.
// Must be called inside a state operation whenever agents map is modified.
func (s *leaderState) rebuildSortedAgents() {
	s.agentsSorted = make([]*types.Agent, 0, len(s.agents))
	for _, a := range s.agents {
		s.agentsSorted = append(s.agentsSorted, a)
	}
	sort.Slice(s.agentsSorted, func(i, j int) bool {
		return s.agentsSorted[i].ID < s.agentsSorted[j].ID
	})
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
	apiKey       string
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

// SetAPIKey sets the API key used for leader→agent HTTP requests
func (l *Leader) SetAPIKey(key string) {
	l.apiKey = key
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
	// Build nameToID index from existing jobs in store.
	// Without this, GetPlacedByJobName can't map jobIDs to names
	// and placed counts would show as 0 after leader restart.
	nameToID := make(map[string]string)
	for _, job := range l.jobStore.GetJobs() {
		nameToID[job.Name] = job.ID
	}

	state := &leaderState{
		agents:      make(map[string]*types.Agent),
		placed:      make(map[string]map[string]int),
		dispatching: make(map[string]bool),
		nameToID:    nameToID,
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

// Heartbeat updates agent's LastSeen, syncs placed counts, and syncs jobs.
// Does NOT trigger reconciliation — that's RegisterAgent's job.
// Returns (nil, false) if agent is unknown — caller should return 404 to force re-register.
func (l *Leader) Heartbeat(id, endpoint string, jobs []*types.Job, placed map[string]int, stateTime time.Time, version string) ([]*types.Job, bool) {
	known := query(l, func(s *leaderState) bool {
		if agent, ok := s.agents[id]; ok {
			agent.LastSeen = time.Now()
			agent.Version = version
			// Update placed from agent's ground truth
			if placed != nil {
				s.placed[id] = placed
			}
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
		l.do(func(s *leaderState) {
			for _, j := range jobs {
				s.nameToID[j.Name] = j.ID
			}
		})
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
		s.rebuildSortedAgents()
		return s.settled
	})

	if shouldReconcile {
		log.Printf("Agent %s registered, reconciling jobs", id)
		l.reconcileJobs()
	}
	l.eventBus.Notify("agent:" + id)
}

// UnregisterAgent removes an agent and reconciles jobs
func (l *Leader) UnregisterAgent(id string) {
	l.do(func(s *leaderState) {
		delete(s.agents, id)
		delete(s.placed, id)
		s.rebuildSortedAgents()
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

// FindJobByName finds a job by name using the leader's name→ID index (O(1))
func (l *Leader) FindJobByName(name string) *types.Job {
	id := query(l, func(s *leaderState) string {
		return s.nameToID[name]
	})
	if id == "" {
		return nil
	}
	return l.jobStore.GetJob(id)
}

// GetPlacedByJobName returns placed counts aggregated by job name: jobName → total count.
// Uses the nameToID index to map job IDs back to names.
func (l *Leader) GetPlacedByJobName() map[string]int {
	return query(l, func(s *leaderState) map[string]int {
		idToName := make(map[string]string, len(s.nameToID))
		for name, id := range s.nameToID {
			idToName[id] = name
		}
		result := make(map[string]int)
		for _, jobs := range s.placed {
			for jobID, count := range jobs {
				if name := idToName[jobID]; name != "" {
					result[name] += count
				}
			}
		}
		return result
	})
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

