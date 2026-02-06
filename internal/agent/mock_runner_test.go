package agent

import (
	"errors"
	"sync"
	"time"

	"easyrun/internal/runner"
	"easyrun/internal/types"
)

// ErrSimulated is returned by mock runner when configured to fail
var ErrSimulated = errors.New("simulated runner error")

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

	// Hooks for testing
	onRun  func(job *types.Job) error
	onStop func(task *types.Task) error
}

// NewMockRunner creates a new mock runner
func NewMockRunner() *MockRunner {
	return &MockRunner{
		tasks:   make(map[string]*types.Task),
		stopped: make(map[string]bool),
		nextPid: 1000,
		stdout:  make(map[string]*runner.LogBroadcaster),
		stderr:  make(map[string]*runner.LogBroadcaster),
	}
}

// Run implements runner.Runner
func (m *MockRunner) Run(job *types.Job, ports map[string]int) (*types.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.onRun != nil {
		if err := m.onRun(job); err != nil {
			return nil, err
		}
	}

	if m.runErr != nil {
		return nil, m.runErr
	}

	taskID := "task-" + job.Name
	m.nextPid++

	task := &types.Task{
		ID:          taskID,
		JobID:       job.ID,
		JobName:     job.Name,
		Ports:       ports,
		Pid:         m.nextPid,
		State:       types.TaskRunning,
		StartedAt:   time.Now(),
		CPUShares:   job.CPUShares,
		MemoryLimit: job.MemoryLimit,
	}

	m.tasks[taskID] = task
	m.stdout[taskID] = runner.NewLogBroadcaster()
	m.stderr[taskID] = runner.NewLogBroadcaster()

	return task, nil
}

// Stop implements runner.Runner
func (m *MockRunner) Stop(task *types.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.onStop != nil {
		if err := m.onStop(task); err != nil {
			return err
		}
	}

	if m.stopErr != nil {
		return m.stopErr
	}

	m.stopped[task.ID] = true
	delete(m.tasks, task.ID)
	return nil
}

// Status implements runner.Runner
func (m *MockRunner) Status(task *types.Task) (types.TaskState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if t, ok := m.tasks[task.ID]; ok {
		return t.State, nil
	}
	return types.TaskFailed, nil
}

// GetStdout implements runner.Runner
func (m *MockRunner) GetStdout(taskID string) *runner.LogBroadcaster {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stdout[taskID]
}

// GetStderr implements runner.Runner
func (m *MockRunner) GetStderr(taskID string) *runner.LogBroadcaster {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stderr[taskID]
}

// Cleanup implements runner.Runner
func (m *MockRunner) Cleanup() error {
	return nil
}

// Test helpers

// WasStopped returns true if the task was stopped
func (m *MockRunner) WasStopped(taskID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped[taskID]
}

// SetRunError configures the runner to return an error on Run
func (m *MockRunner) SetRunError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runErr = err
}

// SetStopError configures the runner to return an error on Stop
func (m *MockRunner) SetStopError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopErr = err
}

// SetTaskState sets the state of a task (for testing status checks)
func (m *MockRunner) SetTaskState(taskID string, state types.TaskState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[taskID]; ok {
		task.State = state
	}
}

// GetTask returns a task by ID
func (m *MockRunner) GetTask(taskID string) *types.Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks[taskID]
}
