package leader

import (
	"context"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

const (
	defaultAgentTimeout    = 30 * time.Second
	httpClientTimeout      = 5 * time.Second
	deadAgentCheckInterval = 10 * time.Second
	stateChannelBufferSize = 64
)

// JobStore is the interface for accessing jobs (implemented by Agent)
type JobStore interface {
	GetJobs() []*types.Job
	GetJob(id string) *types.Job
	StoreJob(job *types.Job)
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

// New creates a new leader that shares job storage with the agent
func New(localAgentID string, jobStore JobStore) *Leader {
	return &Leader{
		localAgentID: localAgentID,
		jobStore:     jobStore,
		ops:          make(chan func(*leaderState), stateChannelBufferSize),
		agentTimeout: defaultAgentTimeout,
		httpClient: &http.Client{
			Timeout: httpClientTimeout,
		},
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

// Heartbeat updates agent's LastSeen and learns jobs from remote agents
func (l *Leader) Heartbeat(id, endpoint string, agentJobs []*types.Job, agentStateTime time.Time) []*types.Job {
	// Update agent
	l.do(func(s *leaderState) {
		if agent, ok := s.agents[id]; ok {
			agent.LastSeen = time.Now()
		} else {
			s.agents[id] = &types.Agent{
				ID:       id,
				Endpoint: endpoint,
				LastSeen: time.Now(),
			}
		}
	})

	// If agent has newer state than us, adopt their state
	myStateTime := l.jobStore.GetStateTime()
	if agentStateTime.After(myStateTime) && len(agentJobs) > 0 {
		log.Printf("Agent %s has newer state, syncing", id)
		l.jobStore.SyncJobs(agentJobs, agentStateTime)

		// Update placement for these jobs
		l.do(func(s *leaderState) {
			for _, job := range agentJobs {
				if !containsString(s.placement[job.ID], id) {
					s.placement[job.ID] = append(s.placement[job.ID], id)
				}
			}
		})
		return l.jobStore.GetJobs()
	}

	// Learn jobs from remote agents (recovery after leader failover)
	if id != l.localAgentID && len(agentJobs) > 0 {
		l.do(func(s *leaderState) {
			for _, job := range agentJobs {
				if len(s.placement[job.ID]) == 0 {
					l.jobStore.StoreJob(job)
				}
				if !containsString(s.placement[job.ID], id) {
					s.placement[job.ID] = append(s.placement[job.ID], id)
				}
			}
		})
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

// containsString checks if a slice contains a string
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
