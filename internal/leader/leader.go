package leader

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"easyrun/internal/types"
)

const (
	defaultAgentTimeout    = 30 * time.Second
	deadAgentCheckInterval = 10 * time.Second
	stateChannelBufferSize = 256
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
	GetJob(name string) *types.Job
	StoreJob(job *types.Job)
	DeleteJob(name string) // Remove job from store by name
	GetStateTime() time.Time
	SyncJobs(jobs []*types.Job, updated time.Time)
}

// leaderState holds all mutable state (owned by single goroutine)
type leaderState struct {
	agents       map[string]*types.Agent
	agentsSorted []*types.Agent         // cached sorted agent list, rebuilt on mutation
	placed       map[string]map[string]int // agentID -> jobName -> count
	dispatching  map[string]bool          // jobName -> true if actively being dispatched
	settled      bool                     // false during settle period after leader election
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
	deleteClient *http.Client
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
		deleteClient: &http.Client{Timeout: DeleteClientTimeout},
		eventBus:     NewEventBus(),
	}
}

// SetAPIKey sets the API key used for leader→agent HTTP requests
func (l *Leader) SetAPIKey(key string) {
	l.apiKey = key
}

// SetAgentTimeout overrides the default agent timeout (for config wiring)
func (l *Leader) SetAgentTimeout(d time.Duration) {
	l.agentTimeout = d
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
			l.eventBus.Notify("status")
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
// Returns (nil, false) if agent is unknown — caller should return 404 to force re-register.
func (l *Leader) Heartbeat(id, endpoint string, jobs []*types.Job, stateTime time.Time, version string) ([]*types.Job, bool) {
	known := query(l, func(s *leaderState) bool {
		agent, ok := s.agents[id]
		if !ok {
			return false
		}
		agent.LastSeen = time.Now()
		agent.Version = version
		return true
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
// Returns false if the ID is already registered with a different endpoint (duplicate).
func (l *Leader) RegisterAgent(id, endpoint, version string, placed map[string]int) bool {
	type result struct {
		reconcile bool
		rejected  bool
	}
	res := query(l, func(s *leaderState) result {
		// Reject if another live agent has the same ID but different endpoint
		if existing, ok := s.agents[id]; ok && existing.Endpoint != endpoint {
			if time.Since(existing.LastSeen) < l.agentTimeout {
				log.Printf("Rejecting agent %s: already registered at %s (new: %s)", id, existing.Endpoint, endpoint)
				return result{rejected: true}
			}
		}

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
		return result{reconcile: s.settled}
	})

	if res.rejected {
		return false
	}
	if res.reconcile {
		log.Printf("Agent %s registered, reconciling jobs", id)
		l.reconcileJobs()
	}
	l.eventBus.Notify("agent:" + id)
	return true
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

// GetJob returns a single job by name (nil if not found)
func (l *Leader) GetJob(name string) *types.Job {
	return l.jobStore.GetJob(name)
}

// NextPriority returns the next available priority index (= number of existing jobs).
func (l *Leader) NextPriority() int {
	return len(l.jobStore.GetJobs())
}

// PatchJobPriority moves a job to the given index position, renumbering all other
// jobs to keep a dense 0..N-1 sequence.
func (l *Leader) PatchJobPriority(name string, targetIdx int) error {
	job := l.jobStore.GetJob(name)
	if job == nil {
		return fmt.Errorf("job %s not found", name)
	}

	jobs := l.jobStore.GetJobs()
	sort.Slice(jobs, func(i, j int) bool {
		pi, pj := effectivePriority(jobs[i].Priority), effectivePriority(jobs[j].Priority)
		if pi != pj {
			return pi < pj
		}
		return jobs[i].Name < jobs[j].Name
	})

	for i := 0; i < len(jobs); i++ {
		if jobs[i].Name == name {
			jobs = append(jobs[:i], jobs[i+1:]...)
			break
		}
	}

	if targetIdx < 0 {
		targetIdx = 0
	}
	if targetIdx > len(jobs) {
		targetIdx = len(jobs)
	}
	jobs = append(jobs[:targetIdx], append([]*types.Job{job}, jobs[targetIdx:]...)...)

	for i, j := range jobs {
		p := i
		updated := *j
		updated.Priority = &p
		l.jobStore.StoreJob(&updated)
	}

	l.eventBus.Notify("job:" + name)
	go l.reconcileJobs()
	return nil
}

// GetPlacedCounts returns placed counts aggregated by job name: jobName → total count.
func (l *Leader) GetPlacedCounts() map[string]int {
	return query(l, func(s *leaderState) map[string]int {
		result := make(map[string]int)
		for _, jobs := range s.placed {
			for jobName, count := range jobs {
				result[jobName] += count
			}
		}
		return result
	})
}

// GetPlaced returns which agents have a job placed and how many (for testing/debugging)
func (l *Leader) GetPlaced(jobName string) map[string]int {
	return query(l, func(s *leaderState) map[string]int {
		result := make(map[string]int)
		for agentID, jobs := range s.placed {
			if count := jobs[jobName]; count > 0 {
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
