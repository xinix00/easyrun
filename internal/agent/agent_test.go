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
		Name:    "test-job",
		Command: "echo hello",
	}

	task, err := agent.startJob(job)
	if err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	if task.JobName != "test-job" {
		t.Errorf("task.JobName = %q, want %q", task.JobName, "test-job")
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
		Name:    "failing-job",
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

func TestAgentDeleteJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store job and task
	agent.StoreJob(&types.Job{Name: "test-job", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "test-job",
			State:   types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("test-job")

	if deleted != 1 {
		t.Errorf("deleteJob returned %d, want 1", deleted)
	}

	// Job should be removed
	if agent.GetJobByName("test-job") != nil {
		t.Error("Job should be removed after delete")
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
		jobName := "job-" + string(rune('a'+i))
		job := &types.Job{
			Name:    jobName,
			Command: "echo",
		}
		agent.StoreJob(job)
		agent.do(func(s *agentState) {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:      "task-" + string(rune('a'+i)),
				JobName: jobName,
				State:   types.TaskRunning,
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

func TestAgentDeleteJobWithMultipleTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// One job, multiple tasks
	agent.StoreJob(&types.Job{Name: "test-job", Command: "echo"})
	agent.do(func(s *agentState) {
		for i := 0; i < 3; i++ {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:      "task-" + string(rune('a'+i)),
				JobName: "test-job",
				State:   types.TaskRunning,
			}
		}
	})

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("test-job")

	if deleted != 3 {
		t.Errorf("deleteJob returned %d, want 3", deleted)
	}
}

func TestAgentDeleteJobNonExistent(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJob("nonexistent")

	if deleted != 0 {
		t.Errorf("deleteJob returned %d, want 0", deleted)
	}
}
