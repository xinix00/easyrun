package leader

import (
	"log"
	"net/http"
	"sync"
	"time"

	"easyrun/internal/types"
)

// JobStore is the interface for accessing jobs (implemented by Agent)
type JobStore interface {
	GetJobs() []*types.Job
	GetJob(id string) *types.Job
	StoreJob(job *types.Job)
	GetStateTime() time.Time
	SyncJobs(jobs []*types.Job, updated time.Time)
}

// Leader dispatches jobs to agents and monitors health
type Leader struct {
	localAgentID string
	jobStore     JobStore // shared with agent - no separate jobs map!

	agents   map[string]*types.Agent
	agentsMu sync.RWMutex

	placement   map[string][]string // jobID -> []agentID (multiple instances)
	placementMu sync.RWMutex

	roundRobin int
	rrMu       sync.Mutex

	httpClient   *http.Client
	agentTimeout time.Duration
}

// New creates a new leader that shares job storage with the agent
func New(localAgentID string, jobStore JobStore) *Leader {
	return &Leader{
		localAgentID: localAgentID,
		jobStore:     jobStore,
		agents:       make(map[string]*types.Agent),
		placement:    make(map[string][]string),
		agentTimeout: 30 * time.Second,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Heartbeat updates agent's LastSeen and learns jobs from remote agents
func (l *Leader) Heartbeat(id, endpoint string, agentJobs []*types.Job, agentStateTime time.Time) []*types.Job {
	l.agentsMu.Lock()
	if agent, ok := l.agents[id]; ok {
		agent.LastSeen = time.Now()
	} else {
		l.agents[id] = &types.Agent{
			ID:       id,
			Endpoint: endpoint,
			LastSeen: time.Now(),
		}
	}
	l.agentsMu.Unlock()

	// If agent has newer state than us, adopt their state
	myStateTime := l.jobStore.GetStateTime()
	if agentStateTime.After(myStateTime) && len(agentJobs) > 0 {
		log.Printf("Agent %s has newer state, syncing", id)
		l.jobStore.SyncJobs(agentJobs, agentStateTime)

		// Update placement for these jobs
		l.placementMu.Lock()
		for _, job := range agentJobs {
			// Add this agent if not already in the list
			found := false
			for _, agentID := range l.placement[job.ID] {
				if agentID == id {
					found = true
					break
				}
			}
			if !found {
				l.placement[job.ID] = append(l.placement[job.ID], id)
			}
		}
		l.placementMu.Unlock()
		return l.jobStore.GetJobs()
	}

	// Learn jobs from remote agents (recovery after leader failover)
	// Skip our own agent - we already have those jobs!
	if id != l.localAgentID && len(agentJobs) > 0 {
		l.placementMu.Lock()
		for _, job := range agentJobs {
			if len(l.placement[job.ID]) == 0 {
				l.jobStore.StoreJob(job)
			}
			// Add this agent to placement
			found := false
			for _, agentID := range l.placement[job.ID] {
				if agentID == id {
					found = true
					break
				}
			}
			if !found {
				l.placement[job.ID] = append(l.placement[job.ID], id)
			}
		}
		l.placementMu.Unlock()
	}

	// Return all jobs
	return l.jobStore.GetJobs()
}

// UnregisterAgent removes an agent and redispatches its jobs
func (l *Leader) UnregisterAgent(id string) {
	l.agentsMu.Lock()
	delete(l.agents, id)
	l.agentsMu.Unlock()

	l.redispatchJobsFrom(id)
}

// GetAgents returns all registered agents
func (l *Leader) GetAgents() []*types.Agent {
	l.agentsMu.RLock()
	defer l.agentsMu.RUnlock()

	agents := make([]*types.Agent, 0, len(l.agents))
	for _, a := range l.agents {
		agents = append(agents, a)
	}
	return agents
}

// GetJobs returns all jobs
func (l *Leader) GetJobs() []*types.Job {
	return l.jobStore.GetJobs()
}
