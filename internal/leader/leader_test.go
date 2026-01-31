package leader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"easyrun/internal/types"
)

// MockJobStore implements JobStore for testing
type MockJobStore struct {
	mu        sync.Mutex
	jobs      map[string]*types.Job
	stateTime time.Time
}

func NewMockJobStore() *MockJobStore {
	return &MockJobStore{
		jobs: make(map[string]*types.Job),
	}
}

func (m *MockJobStore) GetJobs() []*types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobs := make([]*types.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (m *MockJobStore) GetJob(id string) *types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[id]
}

func (m *MockJobStore) StoreJob(job *types.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
}

func (m *MockJobStore) GetStateTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stateTime
}

func (m *MockJobStore) SyncJobs(jobs []*types.Job, updated time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, job := range jobs {
		m.jobs[job.ID] = job
	}
	m.stateTime = updated
}

func TestLeaderNew(t *testing.T) {
	store := NewMockJobStore()
	leader := New("agent-1", store, nil)

	if leader == nil {
		t.Fatal("New returned nil")
	}
	if leader.localAgentID != "agent-1" {
		t.Errorf("localAgentID = %q, want %q", leader.localAgentID, "agent-1")
	}
}

func TestLeaderNewWithCustomClient(t *testing.T) {
	store := NewMockJobStore()
	customClient := &http.Client{Timeout: 30 * time.Second}
	leader := New("agent-1", store, customClient)

	if leader.httpClient != customClient {
		t.Error("httpClient should be the custom client")
	}
}

func TestLeaderHeartbeatRegistersAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Send heartbeat from remote agent
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})

	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Errorf("GetAgents() returned %d agents, want 1", len(agents))
	}

	if agents[0].ID != "remote-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "remote-agent")
	}
	if agents[0].Endpoint != "http://192.168.1.10:8080" {
		t.Errorf("Agent Endpoint = %q, want %q", agents[0].Endpoint, "http://192.168.1.10:8080")
	}
}

func TestLeaderHeartbeatUpdatesLastSeen(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// First heartbeat
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	firstSeen := agents[0].LastSeen

	// Wait and send another heartbeat
	time.Sleep(50 * time.Millisecond)
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	agents = leader.GetAgents()
	if !agents[0].LastSeen.After(firstSeen) {
		t.Error("LastSeen should be updated after second heartbeat")
	}
}

func TestLeaderHeartbeatLearnsJobsFromRemoteAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Remote agent has jobs
	remoteJobs := []*types.Job{
		{ID: "job-1", Name: "remote-job-1", Command: "echo 1"},
		{ID: "job-2", Name: "remote-job-2", Command: "echo 2"},
	}

	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", remoteJobs, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Store should have learned about the jobs
	jobs := store.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("Store has %d jobs, want 2", len(jobs))
	}
}

func TestLeaderHeartbeatSyncsNewerState(t *testing.T) {
	store := NewMockJobStore()
	store.stateTime = time.Now().Add(-1 * time.Hour) // Our state is old

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Remote agent has newer state
	newerTime := time.Now()
	remoteJobs := []*types.Job{
		{ID: "newer-job", Name: "newer", Command: "echo newer"},
	}

	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", remoteJobs, newerTime)
	time.Sleep(10 * time.Millisecond)

	// Store should have synced
	if !store.stateTime.Equal(newerTime) {
		t.Errorf("Store stateTime = %v, want %v", store.stateTime, newerTime)
	}
}

func TestLeaderGetAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register multiple agents
	for i := 0; i < 5; i++ {
		leader.Heartbeat("agent-"+string(rune('a'+i)), "http://host:8080", nil, time.Time{})
	}

	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 5 {
		t.Errorf("GetAgents() returned %d agents, want 5", len(agents))
	}
}

func TestLeaderGetJobs(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{ID: "job-1", Name: "job1"})
	store.StoreJob(&types.Job{ID: "job-2", Name: "job2"})

	leader := New("local-agent", store, nil)

	jobs := leader.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("GetJobs() returned %d jobs, want 2", len(jobs))
	}
}

func TestLeaderUnregisterAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agent
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 1 {
		t.Fatal("Agent not registered")
	}

	// Unregister
	leader.UnregisterAgent("remote-agent")
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 0 {
		t.Error("Agent should be unregistered")
	}
}

func TestLeaderConcurrentHeartbeats(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	var wg sync.WaitGroup

	// Concurrent heartbeats from multiple agents
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			agentID := "agent-" + string(rune('a'+n%10))
			for j := 0; j < 10; j++ {
				leader.Heartbeat(agentID, "http://host:8080", nil, time.Time{})
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(10 * time.Millisecond)

	// Should have 10 unique agents (a-j)
	agents := leader.GetAgents()
	if len(agents) != 10 {
		t.Errorf("GetAgents() returned %d agents, want 10", len(agents))
	}
}

func TestLeaderDispatchJobToAgent(t *testing.T) {
	store := NewMockJobStore()

	// Create a mock agent server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register mock agent
	leader.Heartbeat("mock-agent", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Dispatch job
	job := &types.Job{
		ID:      "test-job",
		Name:    "test",
		Command: "echo hello",
		Count:   1,
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	// Job should be stored
	if store.GetJob("test-job") == nil {
		t.Error("Job should be stored after dispatch")
	}
}

func TestLeaderDispatchJobNoAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		ID:      "test-job",
		Name:    "test",
		Command: "echo",
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when no agents available")
	}
}

func TestContainsString(t *testing.T) {
	tests := []struct {
		slice []string
		s     string
		want  bool
	}{
		{[]string{"a", "b", "c"}, "b", true},
		{[]string{"a", "b", "c"}, "d", false},
		{[]string{}, "a", false},
		{nil, "a", false},
	}

	for _, tt := range tests {
		got := containsString(tt.slice, tt.s)
		if got != tt.want {
			t.Errorf("containsString(%v, %q) = %v, want %v", tt.slice, tt.s, got, tt.want)
		}
	}
}
