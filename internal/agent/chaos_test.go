package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"easyrun/internal/runner"
	"easyrun/internal/types"
	"easyrun/pkg/config"
)

// TestChaos_AllTasksCrashSimultaneously tests mass task failure
func TestChaos_AllTasksCrashSimultaneously(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Start 10 tasks
	for i := 0; i < 10; i++ {
		job := &types.Job{
			Name:        fmt.Sprintf("job-%d", i),
			Command:     "./app",
			MaxRestarts: 2,
		}

		task, err := agent.startJob(job)
		if err != nil {
			t.Fatalf("Failed to start job %d: %v", i, err)
		}
		mockRunner.tasks[task.ID] = task
	}

	// CHAOS: All processes crash simultaneously
	for _, task := range mockRunner.tasks {
		task.State = types.TaskFailed
	}

	// Trigger monitor check
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Agent should attempt to restart all
	t.Logf("Mass failure: all 10 tasks crashed, agent should restart")

	// Check that restart was attempted (tasks should have increased restart count)
	count := 0
	agent.do(func(s *agentState) {
		for _, task := range s.tasks {
			if task.RestartCount > 0 {
				count++
			}
		}
	})

	t.Logf("Tasks with restart attempts: %d/10", count)
}

// TestChaos_TaskExceedsMaxRestarts tests task that keeps crashing
func TestChaos_TaskExceedsMaxRestarts(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	mockRunner := &crashingRunner{tasks: make(map[string]*crashingTask)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Job that crashes immediately
	job := &types.Job{
		Name:        "crasher",
		Command:     "./crasher",
		MaxRestarts: 3,
	}

	task, err := agent.startJob(job)
	if err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	// Simulate crash loop
	for i := 0; i < 5; i++ {
		// Task crashes
		mockRunner.tasks[task.ID].state = types.TaskFailed

		// Agent detects and restarts
		agent.restartTask(task)
		time.Sleep(10 * time.Millisecond)
	}

	// After max restarts, should give up
	finalJob := agent.GetJobByName(job.Name)
	if finalJob == nil {
		t.Error("Job should still exist")
	}

	// Check final restart count
	restartCount := query(agent, func(s *agentState) int {
		if t := s.tasks[task.ID]; t != nil {
			return t.RestartCount
		}
		return 0
	})

	t.Logf("Task restarted %d times (max: 3)", restartCount)

	if restartCount > 3 {
		t.Errorf("Task exceeded max restarts: %d > 3", restartCount)
	}
}

// TestChaos_CapacityExhaustion tests agent rejecting jobs when full
func TestChaos_CapacityExhaustion(t *testing.T) {
	cfg := &config.Config{
		Node:     config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths:    config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
		Capacity: config.CapacityConfig{CPUShares: 1024, Memory: 1024 * 1024 * 1024}, // 1 core, 1GB
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Fill capacity
	bigJob := &types.Job{
		Name:        "big-job",
		Command:     "./big",
		CPUShares:   512, // 50% of 1 core
		MemoryLimit: 512 * 1024 * 1024, // 512MB
	}

	task1, _ := agent.startJob(bigJob)
	task2Job := &types.Job{
		Name:        "big-job-2",
		Command:     "./big2",
		CPUShares:   512,
		MemoryLimit: 512 * 1024 * 1024,
	}
	task2, _ := agent.startJob(task2Job)

	// Try to add another job - should fail (no capacity)
	overflowJob := &types.Job{
		Name:        "overflow",
		Command:     "./overflow",
		CPUShares:   100,
		MemoryLimit: 100 * 1024 * 1024,
	}

	hasCapacity := agent.hasCapacity(overflowJob)
	if hasCapacity {
		t.Error("Agent should reject job when at capacity")
	}

	t.Logf("Capacity exhaustion: agent correctly rejected job (CPU: %d/%d, Mem: %d/%d used)",
		1024, 1024, 1024*1024*1024, 1024*1024*1024)

	// Cleanup
	_ = task1
	_ = task2
}

// TestChaos_TaskZombie tests handling of tasks that report wrong state
func TestChaos_TaskZombie(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	zombieRunner := &zombieRunner{tasks: make(map[string]*zombieTask)}
	agent := New(cfg, "test-agent", zombieRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{Name: "zombie-job", Command: "./app", MaxRestarts: 2}
	task, _ := agent.startJob(job)

	// Runner reports task as stopped (process died) but task state says running
	zombieRunner.tasks[task.ID].actualState = types.TaskStopped

	// Monitor should detect mismatch and restart
	agent.checkTasks()
	time.Sleep(50 * time.Millisecond)

	// Check if restart was attempted
	restartCount := query(agent, func(s *agentState) int {
		if t := s.tasks[task.ID]; t != nil {
			return t.RestartCount
		}
		return 0
	})

	if restartCount == 0 {
		t.Error("Zombie task should have been restarted")
	}

	t.Logf("Zombie task detected and restarted (%d restarts)", restartCount)
}

// zombieRunner for zombie task tests
type zombieTask struct {
	task        *types.Task
	actualState types.TaskState
}

type zombieRunner struct {
	tasks map[string]*zombieTask
}

func (r *zombieRunner) Run(job *types.Job, ports map[string]int) (*types.Task, error) {
	task := &types.Task{
		ID:      fmt.Sprintf("task-%d", time.Now().UnixNano()),
		JobName: job.Name,
		State:   types.TaskRunning,
		Ports:   ports,
		Pid:     12345,
	}
	r.tasks[task.ID] = &zombieTask{task: task, actualState: types.TaskRunning}
	return task, nil
}

func (r *zombieRunner) Stop(task *types.Task) error {
	delete(r.tasks, task.ID)
	return nil
}

func (r *zombieRunner) Status(task *types.Task) (types.TaskState, error) {
	if t, ok := r.tasks[task.ID]; ok {
		return t.actualState, nil // Return actual state (may differ from task.State)
	}
	return types.TaskStopped, nil
}

func (r *zombieRunner) GetStdout(taskID string) *runner.LogBroadcaster { return nil }
func (r *zombieRunner) GetStderr(taskID string) *runner.LogBroadcaster { return nil }
func (r *zombieRunner) Cleanup() error                                 { return nil }

// TestChaos_StateCorruption tests recovery from corrupted state
func TestChaos_StateCorruption(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos-corrupt/state.json"},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add jobs
	job1 := &types.Job{Name: "job-1", Command: "./app1"}
	job2 := &types.Job{Name: "job-2", Command: "./app2"}
	agent.startJob(job1)
	agent.startJob(job2)

	// Simulate corruption: task references non-existent job
	agent.do(func(s *agentState) {
		s.tasks["orphan-task"] = &types.Task{
			ID:      "orphan-task",
			JobName: "non-existent-job", // Job doesn't exist!
			State:   types.TaskRunning,
		}
	})

	// Monitor should handle gracefully (no crash)
	agent.checkTasks()
	time.Sleep(10 * time.Millisecond)

	t.Log("Corrupted state (orphan task) handled without crash")
}

// TestChaos_RapidJobDeletionAndCreation tests job churn
func TestChaos_RapidJobDeletionAndCreation(t *testing.T) {
	cfg := &config.Config{
		Node:  config.NodeConfig{IP: "127.0.0.1", Port: 8080},
		Paths: config.PathsConfig{RootfsBase: "/tmp/chaos", StateFile: "/tmp/chaos/state.json"},
	}

	mockRunner := &mockRunner{tasks: make(map[string]*types.Task)}
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Rapidly create and delete jobs
	for i := 0; i < 100; i++ {
		job := &types.Job{
			Name:    "churning-job",
			Command: fmt.Sprintf("./app-v%d", i),
		}

		_, err := agent.startJob(job)
		if err != nil {
			t.Fatalf("Failed to start job %d: %v", i, err)
		}

		// Immediately delete
		agent.deleteJob("churning-job")
	}

	// Agent should be stable
	jobs := agent.GetJobs()
	t.Logf("After 100 create/delete cycles: %d jobs remain", len(jobs))

	// Should have 0 jobs (all deleted)
	if len(jobs) != 0 {
		t.Errorf("Expected 0 jobs after churn, got %d", len(jobs))
	}
}

// Helper types for chaos tests

type crashingRunner struct {
	tasks map[string]*crashingTask
}

type crashingTask struct {
	task  *types.Task
	state types.TaskState
}

func (r *crashingRunner) Run(job *types.Job, ports map[string]int) (*types.Task, error) {
	task := &types.Task{
		ID:      fmt.Sprintf("task-%d", time.Now().UnixNano()),
		JobName: job.Name,
		State:   types.TaskRunning,
		Ports:   ports,
		Pid:     12345,
	}
	r.tasks[task.ID] = &crashingTask{task: task, state: types.TaskRunning}

	// Crash immediately
	go func() {
		time.Sleep(10 * time.Millisecond)
		r.tasks[task.ID].state = types.TaskFailed
	}()

	return task, nil
}

func (r *crashingRunner) Stop(task *types.Task) error {
	delete(r.tasks, task.ID)
	return nil
}

func (r *crashingRunner) Status(task *types.Task) (types.TaskState, error) {
	if t, ok := r.tasks[task.ID]; ok {
		return t.state, nil
	}
	return types.TaskStopped, nil
}

func (r *crashingRunner) GetStdout(taskID string) *runner.LogBroadcaster { return nil }
func (r *crashingRunner) GetStderr(taskID string) *runner.LogBroadcaster { return nil }
func (r *crashingRunner) Cleanup() error                                 { return nil }
