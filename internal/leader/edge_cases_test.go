package leader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"easyrun/internal/types"
)

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

func TestLeaderHeartbeatWithOlderState(t *testing.T) {
	store := NewMockJobStore()
	store.stateTime = time.Now() // Our state is current

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Remote agent has older state
	olderTime := time.Now().Add(-1 * time.Hour)
	remoteJobs := []*types.Job{
		{ID: "old-job", Name: "old", Command: "echo old"},
	}

	beforeSync := store.stateTime
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", remoteJobs, olderTime)
	time.Sleep(10 * time.Millisecond)

	// Store should NOT have synced (our state is newer)
	if store.stateTime.Before(beforeSync) {
		t.Error("Store stateTime should not be updated to older time")
	}
}

func TestLeaderDispatchWithHTTPTimeout(t *testing.T) {
	store := NewMockJobStore()

	// Create a server that times out
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond) // Simulate slow response
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Use a short timeout client
	shortTimeoutClient := &http.Client{Timeout: 50 * time.Millisecond}
	leader := New("local-agent", store, shortTimeoutClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("slow-agent", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		ID:      "timeout-job",
		Name:    "test",
		Command: "echo",
		Count:   1,
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when agent times out")
	}
}

func TestLeaderMultipleAgentsPartialFailure(t *testing.T) {
	store := NewMockJobStore()

	successCount := 0

	// First agent succeeds
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" {
			successCount++
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer successServer.Close()

	// Second agent fails
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failServer.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("success-agent", successServer.URL, nil, time.Time{})
	leader.Heartbeat("fail-agent", failServer.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Dispatch job with 1 instance - should succeed on first agent
	job := &types.Job{
		ID:      "partial-job",
		Name:    "test",
		Command: "echo",
		Count:   1,
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should succeed with at least one working agent: %v", err)
	}

	if successCount == 0 {
		t.Error("Job should have been dispatched to successful agent")
	}
}

func TestLeaderStopJobNotFound(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Stopping non-existent job should not panic
	leader.StopJob("nonexistent-job")
	// If we get here without panic, test passes
}

func TestLeaderEmptyClusterStatus(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	status := leader.GetClusterStatus()

	// Should return empty map, not nil
	if status == nil {
		t.Error("GetClusterStatus should return empty map, not nil")
	}
	if len(status) != 0 {
		t.Errorf("GetClusterStatus should be empty, got %d entries", len(status))
	}
}
