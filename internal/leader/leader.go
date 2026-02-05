package leader

import (
	"context"
	"log"
	"net/http"
	"slices"
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
	placement  map[string][]string // jobID -> []agentID
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

// Heartbeat updates agent's LastSeen and learns jobs/placement from agents
func (l *Leader) Heartbeat(id, endpoint string, agentJobs []*types.Job, agentStateTime time.Time, version string) []*types.Job {
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

	// If agent has newer state than us, adopt their state (sync jobs to store)
	myStateTime := l.jobStore.GetStateTime()
	if agentStateTime.After(myStateTime) && len(agentJobs) > 0 {
		log.Printf("Agent %s has newer state, syncing", id)
		l.jobStore.SyncJobs(agentJobs, agentStateTime)
	}

	// Learn placement from ALL agents (including local!) for failover recovery
	// This must happen BEFORE tryRescheduleUnderscheduled to avoid duplicates
	// Use query() to ensure this completes before continuing (not fire-and-forget)
	if len(agentJobs) > 0 {
		query(l, func(s *leaderState) struct{} {
			for _, job := range agentJobs {
				if job.ID == "" {
					continue // Skip jobs without ID (legacy)
				}
				// Store job if we don't know about it yet
				if l.jobStore.GetJob(job.ID) == nil {
					l.jobStore.StoreJob(job)
				}
				// Track placement
				if !slices.Contains(s.placement[job.ID], id) {
					s.placement[job.ID] = append(s.placement[job.ID], id)
				}
			}
			return struct{}{}
		})
	}

	// New agent gets count=-1 jobs and we try to reschedule under-scheduled jobs
	// This runs AFTER learning placement - placement check prevents duplicates
	if isNew {
		l.ensureAllAgentJobs(id, endpoint)
		l.tryRescheduleUnderscheduled(id, endpoint)
	}

	return l.jobStore.GetJobs()
}

// UnregisterAgent removes an agent and redispatches its jobs
func (l *Leader) UnregisterAgent(id string) {
	l.do(func(s *leaderState) {
		delete(s.agents, id)
	})
	l.redispatchJobsFrom(id)
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

// ensureAllAgentJobs dispatches count=-1 jobs to agent if missing
func (l *Leader) ensureAllAgentJobs(agentID, endpoint string) {
	agent := &types.Agent{ID: agentID, Endpoint: endpoint}
	for _, job := range l.jobStore.GetJobs() {
		if job.Count != -1 || job.ID == "" {
			continue
		}
		// Skip if agent already has this job (placement learned from heartbeat)
		if query(l, func(s *leaderState) bool {
			return slices.Contains(s.placement[job.ID], agentID)
		}) {
			continue
		}
		if err := l.sendJobToAgent(agent, job); err != nil {
			log.Printf("Failed to dispatch job %s to agent %s: %v", job.Name, agentID, err)
			continue
		}
		l.do(func(s *leaderState) {
			s.placement[job.ID] = append(s.placement[job.ID], agentID)
		})
	}
}

// tryRescheduleUnderscheduled attempts to place under-scheduled jobs on new agent
func (l *Leader) tryRescheduleUnderscheduled(agentID, endpoint string) {
	agent := &types.Agent{ID: agentID, Endpoint: endpoint}

	for _, job := range l.jobStore.GetJobs() {
		if job.Count == -1 || job.ID == "" {
			continue // Handled by ensureAllAgentJobs
		}

		// Skip if agent already has this job (placement learned from heartbeat)
		if query(l, func(s *leaderState) bool {
			return slices.Contains(s.placement[job.ID], agentID)
		}) {
			continue
		}

		desired := job.Count
		if desired <= 0 {
			desired = 1
		}

		actual := len(query(l, func(s *leaderState) []string {
			return s.placement[job.ID]
		}))

		if actual < desired {
			missing := desired - actual
			log.Printf("Job %s under-scheduled (%d/%d), trying new node %s",
				job.Name, actual, desired, agentID)

			// Try to dispatch missing instances to this new node
			for i := 0; i < missing; i++ {
				if err := l.sendJobToAgent(agent, job); err != nil {
					log.Printf("New node %s cannot run job %s: %v", agentID, job.Name, err)
					break // Try next job (volumes might not exist on this node)
				}
				l.do(func(s *leaderState) {
					s.placement[job.ID] = append(s.placement[job.ID], agentID)
				})
			}
		}
	}
}
