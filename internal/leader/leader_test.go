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

// ============== EDGE CASE TESTS ==============

func TestLeaderCheckDeadAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)
	leader.agentTimeout = 50 * time.Millisecond // Short timeout for testing

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agent
	leader.Heartbeat("dying-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 1 {
		t.Fatal("Agent not registered")
	}

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Manually trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(10 * time.Millisecond)

	// Agent should be removed
	if len(leader.GetAgents()) != 0 {
		t.Error("Dead agent should be removed")
	}
}

func TestLeaderRedispatchJobsFromDeadAgent(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{ID: "job-1", Name: "test-job", Command: "echo", Count: 1})

	// Create mock agent that accepts jobs
	acceptCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" {
			acceptCount++
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)
	leader.agentTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register two agents: one will "die", one will accept redispatched jobs
	leader.Heartbeat("dying-agent", "http://dead.host:8080", nil, time.Time{})
	leader.Heartbeat("healthy-agent", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Manually add placement for job on dying agent
	leader.do(func(s *leaderState) {
		s.placement["job-1"] = []string{"dying-agent"}
	})
	time.Sleep(10 * time.Millisecond)

	// Wait for dying-agent to timeout
	time.Sleep(100 * time.Millisecond)

	// Keep healthy-agent alive
	leader.Heartbeat("healthy-agent", server.URL, nil, time.Time{})

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(50 * time.Millisecond)

	// Job should have been redispatched to healthy-agent
	if acceptCount == 0 {
		t.Error("Job should have been redispatched to healthy agent")
	}
}

func TestLeaderDispatchAllAgentsReject(t *testing.T) {
	store := NewMockJobStore()

	// Create mock agent that always rejects
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register rejecting agent
	leader.Heartbeat("rejecting-agent", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		ID:      "test-job",
		Name:    "test",
		Command: "echo",
		Count:   1,
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when all agents reject")
	}
}

func TestLeaderDispatchMultipleInstances(t *testing.T) {
	store := NewMockJobStore()

	// Track which agents received jobs
	received := make(map[string]int)
	var mu sync.Mutex

	// Create multiple mock agents
	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		agentID := string(rune('a' + i))
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/run" {
				mu.Lock()
				received[agentID]++
				mu.Unlock()
				w.WriteHeader(http.StatusCreated)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer servers[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents
	for i, s := range servers {
		leader.Heartbeat("agent-"+string(rune('a'+i)), s.URL, nil, time.Time{})
	}
	time.Sleep(10 * time.Millisecond)

	// Dispatch job with 6 instances (2 per agent with round-robin)
	job := &types.Job{
		ID:      "multi-job",
		Name:    "test",
		Command: "echo",
		Count:   6,
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	// All 6 instances should be dispatched
	mu.Lock()
	total := 0
	for _, count := range received {
		total += count
	}
	mu.Unlock()

	if total != 6 {
		t.Errorf("Total dispatched = %d, want 6", total)
	}
}

func TestLeaderStopJobOnMultipleAgents(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{ID: "job-1", Name: "test", Command: "echo"})

	// Track stop requests
	stopCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/stop/job-1" {
			mu.Lock()
			stopCount++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents and set placement
	leader.Heartbeat("agent-a", server.URL, nil, time.Time{})
	leader.Heartbeat("agent-b", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Set job placement on both agents
	leader.do(func(s *leaderState) {
		s.placement["job-1"] = []string{"agent-a", "agent-b"}
	})
	time.Sleep(10 * time.Millisecond)

	// Stop job
	leader.StopJob("job-1")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if stopCount != 2 {
		t.Errorf("Stop requests = %d, want 2", stopCount)
	}
	mu.Unlock()
}

func TestLeaderConcurrentDispatchAndStop(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate some latency
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-1", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup

	// Concurrent dispatches
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := &types.Job{
				ID:      "job-" + string(rune('0'+n)),
				Name:    "test",
				Command: "echo",
			}
			leader.DispatchJob(job)
		}(i)
	}

	// Concurrent stops
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			leader.StopJob("job-" + string(rune('0'+n)))
		}(i)
	}

	wg.Wait()
	// Should not panic or deadlock
}

func TestLeaderAgentTimeoutConfigurable(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	// Default timeout should be 30 seconds
	if leader.agentTimeout != 30*time.Second {
		t.Errorf("Default agentTimeout = %v, want 30s", leader.agentTimeout)
	}

	// Can be modified
	leader.agentTimeout = 5 * time.Second
	if leader.agentTimeout != 5*time.Second {
		t.Error("agentTimeout should be modifiable")
	}
}

func TestLeaderDispatchJobWithZeroCount(t *testing.T) {
	store := NewMockJobStore()

	dispatchCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" {
			dispatchCount++
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

	leader.Heartbeat("agent-1", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Count = 0 should default to 1
	job := &types.Job{
		ID:      "zero-count-job",
		Name:    "test",
		Command: "echo",
		Count:   0,
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	if dispatchCount != 1 {
		t.Errorf("dispatchCount = %d, want 1 (default)", dispatchCount)
	}
}

func TestLeaderGetClusterStatusWithFailingAgent(t *testing.T) {
	store := NewMockJobStore()

	// One working agent, one failing
	workingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tasks" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"id":"task-1","state":"running"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer workingServer.Close()

	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failingServer.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("working-agent", workingServer.URL, nil, time.Time{})
	leader.Heartbeat("failing-agent", failingServer.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	status := leader.GetClusterStatus()

	// Should have tasks from working agent, not from failing
	if _, ok := status["working-agent"]; !ok {
		t.Error("Should have status from working agent")
	}

	// Failing agent might not be in result (error case)
	if tasks, ok := status["failing-agent"]; ok && len(tasks) > 0 {
		t.Error("Should not have tasks from failing agent")
	}
}
