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
