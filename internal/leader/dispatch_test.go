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

// ============== DISPATCH TESTS ==============

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

func TestLeaderDispatchCountMinusOne(t *testing.T) {
	store := NewMockJobStore()

	dispatches := make(map[string]int)
	var mu sync.Mutex

	// Create 3 agents
	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		idx := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/run" {
				mu.Lock()
				dispatches[servers[idx].URL]++
				mu.Unlock()
				w.WriteHeader(http.StatusCreated)
			}
		}))
		defer servers[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register all 3 agents
	for i, s := range servers {
		leader.Heartbeat("agent-"+string(rune('a'+i)), s.URL, nil, time.Time{})
	}
	time.Sleep(10 * time.Millisecond)

	// count=-1 should run on ALL agents
	job := &types.Job{
		ID:      "everywhere-job",
		Name:    "easydns",
		Command: "/usr/bin/easydns",
		Count:   -1,
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	mu.Lock()
	total := 0
	for _, count := range dispatches {
		total += count
	}
	mu.Unlock()

	if total != 3 {
		t.Errorf("Total dispatched = %d, want 3 (all agents)", total)
	}
}

func TestLeaderCountMinusOneNewAgent(t *testing.T) {
	store := NewMockJobStore()

	dispatches := make(map[string]int)
	var mu sync.Mutex

	makeServer := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/run" {
				mu.Lock()
				dispatches[r.Host]++
				mu.Unlock()
				w.WriteHeader(http.StatusCreated)
			}
		}))
	}

	server1 := makeServer()
	defer server1.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register first agent
	leader.Heartbeat("agent-a", server1.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Dispatch count=-1 job
	job := &types.Job{
		ID:      "everywhere-job",
		Name:    "easydns",
		Command: "/usr/bin/easydns",
		Count:   -1,
	}
	leader.DispatchJob(job)
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if len(dispatches) != 1 {
		t.Errorf("Expected 1 dispatch, got %d", len(dispatches))
	}
	mu.Unlock()

	// Add new agent - should automatically get the job
	server2 := makeServer()
	defer server2.Close()
	leader.Heartbeat("agent-b", server2.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	total := 0
	for _, count := range dispatches {
		total += count
	}
	mu.Unlock()

	if total != 2 {
		t.Errorf("Total dispatched = %d, want 2 (new agent should get job)", total)
	}
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
