package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"easyrun/internal/runner"
	"easyrun/internal/types"
	"easyrun/pkg/config"
)

// MockRunner implements runner.Runner for testing
type MockRunner struct {
	mu       sync.Mutex
	tasks    map[string]*types.Task
	stopped  map[string]bool
	runErr   error
	stopErr  error
	nextPid  int
	stdout   map[string]*runner.LogBroadcaster
	stderr   map[string]*runner.LogBroadcaster
}

func NewMockRunner() *MockRunner {
	return &MockRunner{
		tasks:   make(map[string]*types.Task),
		stopped: make(map[string]bool),
		nextPid: 1000,
		stdout:  make(map[string]*runner.LogBroadcaster),
		stderr:  make(map[string]*runner.LogBroadcaster),
	}
}

func (m *MockRunner) Run(job *types.Job, ports map[string]int) (*types.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runErr != nil {
		return nil, m.runErr
	}

	taskID := "task-" + job.ID
	m.nextPid++

	task := &types.Task{
		ID:        taskID,
		JobID:     job.ID,
		JobName:   job.Name,
		Ports:     ports,
		Pid:       m.nextPid,
		State:     types.TaskRunning,
		StartedAt: time.Now(),
	}

	m.tasks[taskID] = task
	m.stdout[taskID] = runner.NewLogBroadcaster()
	m.stderr[taskID] = runner.NewLogBroadcaster()

	return task, nil
}

func (m *MockRunner) Stop(task *types.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopErr != nil {
		return m.stopErr
	}

	m.stopped[task.ID] = true
	delete(m.tasks, task.ID)
	return nil
}

func (m *MockRunner) Status(task *types.Task) (types.TaskState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.tasks[task.ID]; ok {
		return t.State, nil
	}
	return types.TaskFailed, nil
}

func (m *MockRunner) GetStdout(taskID string) *runner.LogBroadcaster {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stdout[taskID]
}

func (m *MockRunner) GetStderr(taskID string) *runner.LogBroadcaster {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stderr[taskID]
}

func (m *MockRunner) Cleanup() error {
	return nil
}

func (m *MockRunner) WasStopped(taskID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped[taskID]
}

// Helper to create test config
func testConfig() *config.Config {
	return &config.Config{
		Node: config.NodeConfig{
			IP:   "127.0.0.1",
			Port: 8080,
		},
		Paths: config.PathsConfig{
			RootfsBase: "/tmp/test-easyrun",
			StateFile:  "/tmp/test-easyrun/state.json",
		},
		Capacity: config.CapacityConfig{
			CPUShares: 1000,
			Memory:    1024 * 1024 * 1024,
		},
	}
}

func TestAgentNew(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	agent := New(cfg, "test-agent", mockRunner)

	if agent.ID() != "test-agent" {
		t.Errorf("ID() = %q, want %q", agent.ID(), "test-agent")
	}
	if agent.Endpoint() != "http://127.0.0.1:8080" {
		t.Errorf("Endpoint() = %q, want %q", agent.Endpoint(), "http://127.0.0.1:8080")
	}
}

func TestAgentStoreAndGetJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	// Start state loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{
		ID:      "job-123",
		Name:    "test-job",
		Command: "echo hello",
	}

	agent.StoreJob(job)

	// Give state loop time to process
	time.Sleep(10 * time.Millisecond)

	got := agent.GetJob("job-123")
	if got == nil {
		t.Fatal("GetJob returned nil")
	}
	if got.Name != "test-job" {
		t.Errorf("GetJob().Name = %q, want %q", got.Name, "test-job")
	}
}

func TestAgentGetJobs(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store multiple jobs
	for i := 0; i < 3; i++ {
		agent.StoreJob(&types.Job{
			ID:      "job-" + string(rune('a'+i)),
			Name:    "test-job",
			Command: "echo",
		})
	}

	time.Sleep(10 * time.Millisecond)

	jobs := agent.GetJobs()
	if len(jobs) != 3 {
		t.Errorf("GetJobs() returned %d jobs, want 3", len(jobs))
	}
}

func TestAgentSyncJobs(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store initial job
	agent.StoreJob(&types.Job{
		ID:      "old-job",
		Name:    "old",
		Command: "old",
	})

	time.Sleep(10 * time.Millisecond)
	beforeSync := time.Now()

	// Sync with new jobs
	newJobs := []*types.Job{
		{ID: "new-job-1", Name: "new1", Command: "new1"},
		{ID: "new-job-2", Name: "new2", Command: "new2"},
	}

	agent.SyncJobs(newJobs, beforeSync)

	time.Sleep(10 * time.Millisecond)

	// Should have both old and new jobs
	jobs := agent.GetJobs()
	if len(jobs) != 3 {
		t.Errorf("GetJobs() returned %d jobs, want 3", len(jobs))
	}

	// State time should be updated (after sync)
	stateTime := agent.GetStateTime()
	if stateTime.IsZero() {
		t.Error("GetStateTime() should not be zero after SyncJobs")
	}
	if stateTime.Before(beforeSync) {
		t.Errorf("GetStateTime() = %v, should be after %v", stateTime, beforeSync)
	}
}

func TestAgentGetStateTime(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Initial state time should be zero
	stateTime := agent.GetStateTime()
	if !stateTime.IsZero() {
		t.Errorf("initial GetStateTime() = %v, want zero", stateTime)
	}
}

func TestAgentHasCapacity(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100
	cfg.Capacity.Memory = 1024

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Small job should fit
	smallJob := &types.Job{
		ID:          "small",
		CPUShares:   50,
		MemoryLimit: 512,
	}
	if !agent.hasCapacity(smallJob) {
		t.Error("hasCapacity should return true for small job")
	}

	// Large job should not fit
	largeJob := &types.Job{
		ID:          "large",
		CPUShares:   200,
		MemoryLimit: 2048,
	}
	if agent.hasCapacity(largeJob) {
		t.Error("hasCapacity should return false for job exceeding CPU")
	}
}

func TestAgentInit(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	// Init should call Cleanup on runner
	err := agent.Init()
	if err != nil {
		t.Errorf("Init() returned error: %v", err)
	}
}

func TestAgentConcurrentStateAccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			agent.StoreJob(&types.Job{
				ID:      "job-" + string(rune('0'+n)),
				Name:    "concurrent-job",
				Command: "echo",
			})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent.GetJobs()
			agent.GetStateTime()
		}()
	}

	wg.Wait()

	// Verify all jobs were stored
	jobs := agent.GetJobs()
	if len(jobs) != 10 {
		t.Errorf("GetJobs() returned %d jobs, want 10", len(jobs))
	}
}

// ============== EDGE CASE TESTS ==============

func TestAgentCapacityWithRunningTasks(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100
	cfg.Capacity.Memory = 1024

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store a job and create a running task
	job := &types.Job{
		ID:          "existing-job",
		Name:        "existing",
		Command:     "echo",
		CPUShares:   60,
		MemoryLimit: 600,
	}
	agent.StoreJob(job)

	// Add a running task that uses capacity
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:    "task-1",
			JobID: "existing-job",
			State: types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	// New job that would fit if alone, but not with existing task
	newJob := &types.Job{
		ID:          "new-job",
		CPUShares:   50,
		MemoryLimit: 500,
	}

	// Should not have capacity (60 + 50 > 100)
	if agent.hasCapacity(newJob) {
		t.Error("hasCapacity should return false when existing tasks consume capacity")
	}
}

func TestAgentCapacityIgnoresFailedTasks(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store a job and create a FAILED task (should not count against capacity)
	job := &types.Job{
		ID:        "failed-job",
		CPUShares: 80,
	}
	agent.StoreJob(job)

	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:    "task-1",
			JobID: "failed-job",
			State: types.TaskFailed, // Failed, not running
		}
	})

	time.Sleep(10 * time.Millisecond)

	// New job should fit because failed tasks don't count
	newJob := &types.Job{
		ID:        "new-job",
		CPUShares: 50,
	}

	if !agent.hasCapacity(newJob) {
		t.Error("hasCapacity should ignore failed tasks")
	}
}

func TestAgentCapacityNoLimitsConfigured(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 0 // No limit
	cfg.Capacity.Memory = 0    // No limit

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Any job should fit when no limits configured
	hugeJob := &types.Job{
		ID:          "huge-job",
		CPUShares:   1000000,
		MemoryLimit: 1000000000000,
	}

	if !agent.hasCapacity(hugeJob) {
		t.Error("hasCapacity should return true when no limits configured")
	}
}

func TestAgentCapacityJobWithNoLimits(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100
	cfg.Capacity.Memory = 1024

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job with no resource limits
	noLimitJob := &types.Job{
		ID:          "no-limit-job",
		CPUShares:   0,
		MemoryLimit: 0,
	}

	// Should always fit
	if !agent.hasCapacity(noLimitJob) {
		t.Error("hasCapacity should return true for job with no limits")
	}
}

func TestAgentGetJobNotFound(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := agent.GetJob("nonexistent")
	if job != nil {
		t.Error("GetJob should return nil for nonexistent job")
	}
}

func TestAgentStoreJobOverwrite(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store job
	agent.StoreJob(&types.Job{
		ID:      "job-1",
		Name:    "original",
		Command: "echo original",
	})

	time.Sleep(10 * time.Millisecond)

	// Overwrite with same ID
	agent.StoreJob(&types.Job{
		ID:      "job-1",
		Name:    "updated",
		Command: "echo updated",
	})

	time.Sleep(10 * time.Millisecond)

	job := agent.GetJob("job-1")
	if job.Name != "updated" {
		t.Errorf("Job name = %q, want %q (overwrite failed)", job.Name, "updated")
	}

	// Should still have only 1 job
	jobs := agent.GetJobs()
	if len(jobs) != 1 {
		t.Errorf("GetJobs() returned %d jobs, want 1", len(jobs))
	}
}

func TestMockRunnerError(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.runErr = &testError{"simulated runner error"}

	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		ID:      "failing-job",
		Name:    "test",
		Command: "echo",
	}

	task, err := agent.startJob(job)
	if err == nil {
		t.Error("startJob should fail when runner returns error")
	}
	if task != nil {
		t.Error("task should be nil when runner fails")
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestAgentStopAllTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Create some running tasks
	for i := 0; i < 3; i++ {
		job := &types.Job{
			ID:      "job-" + string(rune('a'+i)),
			Name:    "test",
			Command: "echo",
		}
		agent.StoreJob(job)
		agent.do(func(s *agentState) {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:    "task-" + string(rune('a'+i)),
				JobID: job.ID,
				State: types.TaskRunning,
			}
		})
	}

	time.Sleep(10 * time.Millisecond)

	// Stop all tasks
	agent.StopAllTasks()

	time.Sleep(10 * time.Millisecond)

	// All tasks should be stopped
	for i := 0; i < 3; i++ {
		taskID := "task-" + string(rune('a'+i))
		if !mockRunner.WasStopped(taskID) {
			t.Errorf("Task %s should be stopped", taskID)
		}
	}
}

func TestAgentConcurrentCapacityCheck(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	results := make(chan bool, 100)

	// Many concurrent capacity checks
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			job := &types.Job{
				ID:        "job-" + string(rune('0'+n%10)),
				CPUShares: 10,
			}
			results <- agent.hasCapacity(job)
		}(i)
	}

	wg.Wait()
	close(results)

	// All should succeed (no running tasks)
	for r := range results {
		if !r {
			t.Error("hasCapacity should return true for all concurrent checks when no tasks running")
		}
	}
}

func TestAgentNewWithNilRunner(t *testing.T) {
	cfg := testConfig()

	// New with nil runner should create default ProcessRunner
	agent := New(cfg, "test-agent", nil)

	if agent.runner == nil {
		t.Error("Agent runner should not be nil when created with nil runner")
	}
}
