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
// Uses mockAgent from failover_test.go

func TestLeaderDispatchJobToAgent(t *testing.T) {
	store := NewMockJobStore()
	agent := newMockAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("mock-agent", agent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "test-job",
		Command: "echo hello",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	if store.GetJobByName("test-job") == nil {
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

	job := &types.Job{Name: "test-job", Command: "echo", HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when no agents available")
	}
}

func TestLeaderDispatchAllAgentsReject(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("rejecting-agent", server.URL, nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "test-job", Command: "echo", Count: 1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when all agents reject")
	}
}

func TestLeaderDispatchMultipleInstances(t *testing.T) {
	store := NewMockJobStore()

	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	for i, a := range agents {
		leader.Heartbeat("agent-"+string(rune('a'+i)), a.URL(), nil, nil, time.Time{}, "")
	}
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "multi-job", Command: "echo", Count: 6, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	total := 0
	for _, a := range agents {
		total += a.TaskCount()
	}
	if total != 6 {
		t.Errorf("Total dispatched = %d, want 6", total)
	}
}

func TestLeaderDeleteJobOnMultipleAgents(t *testing.T) {
	store := NewMockJobStore()
	// Store job AFTER agents are registered to avoid tryRescheduleUnderscheduled
	job := &types.Job{ID: "test-job-id", Name: "test-job", Command: "echo"}

	deleteCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/delete/test-job" {
			mu.Lock()
			deleteCount++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents first (no jobs in store yet, so no rescheduling triggered)
	leader.Heartbeat("agent-a", server.URL, nil, nil, time.Time{}, "")
	leader.Heartbeat("agent-b", server.URL, nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	// Now store the job and set up placement manually
	store.StoreJob(job)
	leader.do(func(s *leaderState) {
		s.placement["test-job-id"] = []string{"agent-a", "agent-b"}
	})
	time.Sleep(10 * time.Millisecond)

	leader.DeleteJob("test-job")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if deleteCount != 2 {
		t.Errorf("Delete requests = %d, want 2", deleteCount)
	}
	mu.Unlock()
}

func TestLeaderDispatchJobWithZeroCount(t *testing.T) {
	store := NewMockJobStore()
	agent := newMockAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-1", agent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "zero-count-job", Command: "echo", Count: 0, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	if agent.TaskCount() != 1 {
		t.Errorf("JobCount = %d, want 1 (default)", agent.TaskCount())
	}
}

func TestLeaderDispatchCountMinusOne(t *testing.T) {
	store := NewMockJobStore()

	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	for i, a := range agents {
		leader.Heartbeat("agent-"+string(rune('a'+i)), a.URL(), nil, nil, time.Time{}, "")
	}
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "easydns", Command: "/usr/bin/easydns", Count: -1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	total := 0
	for _, a := range agents {
		total += a.TaskCount()
	}
	if total != 3 {
		t.Errorf("Total dispatched = %d, want 3 (all agents)", total)
	}
}

func TestLeaderCountMinusOneNewAgent(t *testing.T) {
	store := NewMockJobStore()
	agent1 := newMockAgent()
	defer agent1.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-a", agent1.URL(), nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{ID: "easydns-id", Name: "easydns", Command: "/usr/bin/easydns", Count: -1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}
	leader.DispatchJob(job)
	time.Sleep(10 * time.Millisecond)

	if agent1.TaskCount() != 1 {
		t.Errorf("Expected 1 dispatch, got %d", agent1.TaskCount())
	}

	// Add new agent - should automatically get the job
	agent2 := newMockAgent()
	defer agent2.Close()
	leader.Heartbeat("agent-b", agent2.URL(), nil, nil, time.Time{}, "")
	time.Sleep(100 * time.Millisecond) // Allow time for ensureAllAgentJobs

	if agent2.TaskCount() != 1 {
		t.Errorf("New agent should get job, got %d", agent2.TaskCount())
	}
}

func TestLeaderConcurrentDispatchAndDelete(t *testing.T) {
	store := NewMockJobStore()
	agent := newMockAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-1", agent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := &types.Job{Name: "job-" + string(rune('0'+n)), Command: "echo", HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}
			leader.DispatchJob(job)
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			leader.DeleteJob("job-" + string(rune('0'+n)))
		}(i)
	}

	wg.Wait()
}

// ============== HTTP STATUS CODE TESTS ==============

// TestLeaderDispatchAccepts202 tests that 202 Accepted is treated as success
func TestLeaderDispatchAccepts202(t *testing.T) {
	store := NewMockJobStore()

	// Create agent that returns 202 Accepted (async job handling)
	taskReturned := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			w.WriteHeader(http.StatusAccepted) // 202
		case "/tasks":
			// Return running task after first /run call
			if taskReturned {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`[{"id":"task-1","job_name":"async-job","state":"running"}]`))
			} else {
				taskReturned = true
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`[{"id":"task-1","job_name":"async-job","state":"running"}]`))
			}
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("async-agent", server.URL, nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "async-job",
		Command: "echo async",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should accept 202 Accepted, got error: %v", err)
	}

	if store.GetJobByName("async-job") == nil {
		t.Error("Job should be stored after 202 response")
	}
}

// TestLeaderDispatchAccepts201 tests that 201 Created is treated as success
func TestLeaderDispatchAccepts201(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/run":
			w.WriteHeader(http.StatusCreated) // 201
		case "/tasks":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"id":"task-1","job_name":"created-job","state":"running"}]`))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("create-agent", server.URL, nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "created-job",
		Command: "echo created",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob should accept 201 Created, got error: %v", err)
	}
}

// TestLeaderDispatchRejects500 tests that 500 errors are rejected
func TestLeaderDispatchRejects500(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // 500
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("error-agent", server.URL, nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "error-job",
		Command: "echo error",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should reject 500 Internal Server Error")
	}
}

// TestLeaderDispatchRejects400 tests that 400 errors are rejected
func TestLeaderDispatchRejects400(t *testing.T) {
	store := NewMockJobStore()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // 400
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("bad-agent", server.URL, nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		Name:    "bad-job",
		Command: "echo bad",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should reject 400 Bad Request")
	}
}
