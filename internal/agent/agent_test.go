package agent

import (
	"context"
	"fmt"
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

	// New with nil runner should create default ExecRunner
	agent := New(cfg, "test-agent", nil)

	if agent.execRunner == nil {
		t.Error("Agent execRunner should not be nil when created with nil runner")
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
	agent.StoreJob(&types.Job{ID: "test-job-id", Name: "test-job", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobID:   "test-job-id",
			JobName: "test-job",
			State:   types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJobByID("test-job-id")

	if deleted != 1 {
		t.Errorf("deleteJobByID returned %d, want 1", deleted)
	}

	// Job should be removed
	if agent.GetJob("test-job-id") != nil {
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
			ID:      "job-id-" + string(rune('a'+i)),
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

	// All tasks should be stopped via runner
	for i := 0; i < 3; i++ {
		taskID := "task-" + string(rune('a'+i))
		if !mockRunner.WasStopped(taskID) {
			t.Errorf("Task %s should be stopped", taskID)
		}
	}

	// Tasks should be removed from state
	taskCount := query(agent, func(s *agentState) int { return len(s.tasks) })
	if taskCount != 0 {
		t.Errorf("Expected 0 tasks in state after StopAllTasks, got %d", taskCount)
	}

	// Jobs should still be in state (definitions survive isolation)
	jobCount := query(agent, func(s *agentState) int { return len(s.jobs) })
	if jobCount != 3 {
		t.Errorf("Expected 3 jobs in state after StopAllTasks, got %d", jobCount)
	}

	// Placed task counts should be 0 (no tasks = nothing placed)
	placed := agent.GetPlacedTaskCounts()
	if len(placed) != 0 {
		t.Errorf("Expected empty placed counts after StopAllTasks, got %v", placed)
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
	agent.StoreJob(&types.Job{ID: "test-job-id", Name: "test-job", Command: "echo"})
	agent.do(func(s *agentState) {
		for i := 0; i < 3; i++ {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:      "task-" + string(rune('a'+i)),
				JobID:   "test-job-id",
				JobName: "test-job",
				State:   types.TaskRunning,
			}
		}
	})

	time.Sleep(10 * time.Millisecond)

	deleted := agent.deleteJobByID("test-job-id")

	if deleted != 3 {
		t.Errorf("deleteJobByID returned %d, want 3", deleted)
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

	deleted := agent.deleteJobByID("nonexistent")

	if deleted != 0 {
		t.Errorf("deleteJobByID returned %d, want 0", deleted)
	}
}

func TestDeleteJobRemovesTasksBeforeStop(t *testing.T) {
	// Verify that deleteJobByID removes tasks from state immediately,
	// so checkTasks() won't see them and try to restart during shutdown.
	// This prevents the race: monitor detects "crashed" tasks while
	// docker stop is still running on other tasks.
	for _, driver := range []string{types.DriverExec, types.DriverDocker} {
		t.Run(driver, func(t *testing.T) {
			cfg := testConfig()
			mockExec := NewMockRunner()
			mockDocker := NewMockRunner()

			agent := New(cfg, "test-agent", mockExec)
			agent.dockerRunner = mockDocker

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go agent.stateLoop(ctx)

			agent.StoreJob(&types.Job{ID: "job-1", Name: "del-test", Driver: driver, Command: "echo"})
			agent.do(func(s *agentState) {
				for i := 0; i < 3; i++ {
					tid := fmt.Sprintf("task-%d", i)
					s.tasks[tid] = &types.Task{
						ID:      tid,
						JobID:   "job-1",
						JobName: "del-test",
						Driver:  driver,
						State:   types.TaskRunning,
					}
				}
			})
			time.Sleep(10 * time.Millisecond)

			// Delete the job
			deleted := agent.deleteJobByID("job-1")
			if deleted != 3 {
				t.Fatalf("deleteJobByID returned %d, want 3", deleted)
			}

			// deleteJobByID blocks until all stops complete — tasks should be gone
			remainingTasks := query(agent, func(s *agentState) int {
				return len(s.tasks)
			})
			if remainingTasks != 0 {
				t.Errorf("Expected 0 tasks after delete, got %d", remainingTasks)
			}

			// Job should be gone
			if agent.GetJob("job-1") != nil {
				t.Error("Job should be removed from state after delete")
			}

			// checkTasks should be a no-op (nothing left)
			agent.checkTasks()

			// The correct runner should have been called to stop, not the other
			runner := mockExec
			other := mockDocker
			if driver == types.DriverDocker {
				runner = mockDocker
				other = mockExec
			}

			for i := 0; i < 3; i++ {
				tid := fmt.Sprintf("task-%d", i)
				if !runner.WasStopped(tid) {
					t.Errorf("Runner should have stopped %s", tid)
				}
				if other.WasStopped(tid) {
					t.Errorf("Other runner should NOT have stopped %s", tid)
				}
			}
		})
	}
}
