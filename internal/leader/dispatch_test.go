package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"easyrun/internal/types"
)

// testAgent is a mock agent that tracks jobs and returns tasks
type testAgent struct {
	server *httptest.Server
	mu     sync.Mutex
	tasks  []*types.Task
	seq    int
}

func newTestAgent() *testAgent {
	ta := &testAgent{}
	ta.server = httptest.NewServer(http.HandlerFunc(ta.handle))
	return ta
}

func (ta *testAgent) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/run" && r.Method == http.MethodPost:
		var job types.Job
		json.NewDecoder(r.Body).Decode(&job)
		ta.mu.Lock()
		ta.seq++
		ta.tasks = append(ta.tasks, &types.Task{
			ID:    fmt.Sprintf("task-%s-%d", job.Name, ta.seq),
			JobName: job.Name,
			State: types.TaskRunning,
		})
		ta.mu.Unlock()
		w.WriteHeader(http.StatusCreated)

	case r.URL.Path == "/tasks" && r.Method == http.MethodGet:
		ta.mu.Lock()
		json.NewEncoder(w).Encode(ta.tasks)
		ta.mu.Unlock()

	default:
		w.WriteHeader(http.StatusOK)
	}
}

func (ta *testAgent) URL() string    { return ta.server.URL }
func (ta *testAgent) Close()         { ta.server.Close() }
func (ta *testAgent) JobCount() int  { ta.mu.Lock(); defer ta.mu.Unlock(); return len(ta.tasks) }

// ============== DISPATCH TESTS ==============

func TestLeaderDispatchJobToAgent(t *testing.T) {
	store := NewMockJobStore()
	agent := newTestAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("mock-agent", agent.URL(), nil, time.Time{})
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

	leader.Heartbeat("rejecting-agent", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "test-job", Command: "echo", Count: 1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("DispatchJob should fail when all agents reject")
	}
}

func TestLeaderDispatchMultipleInstances(t *testing.T) {
	store := NewMockJobStore()

	agents := make([]*testAgent, 3)
	for i := range agents {
		agents[i] = newTestAgent()
		defer agents[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	for i, a := range agents {
		leader.Heartbeat("agent-"+string(rune('a'+i)), a.URL(), nil, time.Time{})
	}
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "multi-job", Command: "echo", Count: 6, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	total := 0
	for _, a := range agents {
		total += a.JobCount()
	}
	if total != 6 {
		t.Errorf("Total dispatched = %d, want 6", total)
	}
}

func TestLeaderStopJobOnMultipleAgents(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{Name: "test-job", Command: "echo"})

	stopCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/stop/test-job" {
			mu.Lock()
			stopCount++
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-a", server.URL, nil, time.Time{})
	leader.Heartbeat("agent-b", server.URL, nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	leader.do(func(s *leaderState) {
		s.placement["test-job"] = []string{"agent-a", "agent-b"}
	})
	time.Sleep(10 * time.Millisecond)

	leader.StopJob("test-job")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if stopCount != 2 {
		t.Errorf("Stop requests = %d, want 2", stopCount)
	}
	mu.Unlock()
}

func TestLeaderDispatchJobWithZeroCount(t *testing.T) {
	store := NewMockJobStore()
	agent := newTestAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-1", agent.URL(), nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "zero-count-job", Command: "echo", Count: 0, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	if agent.JobCount() != 1 {
		t.Errorf("JobCount = %d, want 1 (default)", agent.JobCount())
	}
}

func TestLeaderDispatchCountMinusOne(t *testing.T) {
	store := NewMockJobStore()

	agents := make([]*testAgent, 3)
	for i := range agents {
		agents[i] = newTestAgent()
		defer agents[i].Close()
	}

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	for i, a := range agents {
		leader.Heartbeat("agent-"+string(rune('a'+i)), a.URL(), nil, time.Time{})
	}
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "easydns", Command: "/usr/bin/easydns", Count: -1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}

	err := leader.DispatchJob(job)
	if err != nil {
		t.Errorf("DispatchJob failed: %v", err)
	}

	total := 0
	for _, a := range agents {
		total += a.JobCount()
	}
	if total != 3 {
		t.Errorf("Total dispatched = %d, want 3 (all agents)", total)
	}
}

func TestLeaderCountMinusOneNewAgent(t *testing.T) {
	store := NewMockJobStore()
	agent1 := newTestAgent()
	defer agent1.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-a", agent1.URL(), nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	job := &types.Job{Name: "easydns", Command: "/usr/bin/easydns", Count: -1, HealthCheck: &types.HealthCheck{InitialTimeout: 2 * time.Second}}
	leader.DispatchJob(job)
	time.Sleep(10 * time.Millisecond)

	if agent1.JobCount() != 1 {
		t.Errorf("Expected 1 dispatch, got %d", agent1.JobCount())
	}

	// Add new agent - should automatically get the job
	agent2 := newTestAgent()
	defer agent2.Close()
	leader.Heartbeat("agent-b", agent2.URL(), nil, time.Time{})
	time.Sleep(50 * time.Millisecond)

	if agent2.JobCount() != 1 {
		t.Errorf("New agent should get job, got %d", agent2.JobCount())
	}
}

func TestLeaderConcurrentDispatchAndStop(t *testing.T) {
	store := NewMockJobStore()
	agent := newTestAgent()
	defer agent.Close()

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	leader.Heartbeat("agent-1", agent.URL(), nil, time.Time{})
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
			leader.StopJob("job-" + string(rune('0'+n)))
		}(i)
	}

	wg.Wait()
}
