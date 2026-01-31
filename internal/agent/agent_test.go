package agent

import (
	"context"
	"testing"
	"time"

	"easyrun/internal/types"
)

// ============== BASIC AGENT TESTS ==============

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

func TestAgentNewWithNilRunner(t *testing.T) {
	cfg := testConfig()

	// New with nil runner should create default ProcessRunner
	agent := New(cfg, "test-agent", nil)

	if agent.runner == nil {
		t.Error("Agent runner should not be nil when created with nil runner")
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

// ============== TASK MANAGEMENT TESTS ==============

func TestAgentStartJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := &types.Job{
		ID:      "test-job",
		Name:    "test",
		Command: "echo hello",
	}

	task, err := agent.startJob(job)
	if err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	if task.JobID != "test-job" {
		t.Errorf("task.JobID = %q, want %q", task.JobID, "test-job")
	}
	if task.State != types.TaskRunning {
		t.Errorf("task.State = %q, want %q", task.State, types.TaskRunning)
	}
}

func TestAgentStartJobRunnerError(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)

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

func TestAgentStopJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store job and task
	agent.StoreJob(&types.Job{ID: "job-1", Name: "test", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:    "task-1",
			JobID: "job-1",
			State: types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	stopped := agent.stopJob("job-1")

	if stopped != 1 {
		t.Errorf("stopJob returned %d, want 1", stopped)
	}

	// Job should be removed
	if agent.GetJob("job-1") != nil {
		t.Error("Job should be removed after stop")
	}
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

func TestAgentStopJobWithMultipleTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// One job, multiple tasks
	agent.StoreJob(&types.Job{ID: "job-1", Name: "test", Command: "echo"})
	agent.do(func(s *agentState) {
		for i := 0; i < 3; i++ {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:    "task-" + string(rune('a'+i)),
				JobID: "job-1",
				State: types.TaskRunning,
			}
		}
	})

	time.Sleep(10 * time.Millisecond)

	stopped := agent.stopJob("job-1")

	if stopped != 3 {
		t.Errorf("stopJob returned %d, want 3", stopped)
	}
}

func TestAgentStopJobNonExistent(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	stopped := agent.stopJob("nonexistent")

	if stopped != 0 {
		t.Errorf("stopJob returned %d, want 0", stopped)
	}
}
