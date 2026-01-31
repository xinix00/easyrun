package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"easyrun/internal/types"
)

// ============== CAPACITY CHECK TESTS ==============

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

func TestAgentCapacityIgnoresStoppedTasks(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{
		ID:        "stopped-job",
		CPUShares: 80,
	}
	agent.StoreJob(job)

	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:    "task-1",
			JobID: "stopped-job",
			State: types.TaskStopped,
		}
	})

	time.Sleep(10 * time.Millisecond)

	newJob := &types.Job{
		ID:        "new-job",
		CPUShares: 50,
	}

	if !agent.hasCapacity(newJob) {
		t.Error("hasCapacity should ignore stopped tasks")
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

func TestAgentCapacityMemoryOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 0    // No CPU limit
	cfg.Capacity.Memory = 1024

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job exceeding memory should fail
	bigMemJob := &types.Job{
		ID:          "big-mem",
		CPUShares:   10000, // Should be ignored (no CPU limit)
		MemoryLimit: 2048,  // Exceeds limit
	}

	if agent.hasCapacity(bigMemJob) {
		t.Error("hasCapacity should fail on memory limit")
	}
}

func TestAgentCapacityCPUOnly(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100
	cfg.Capacity.Memory = 0    // No memory limit

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job exceeding CPU should fail
	bigCPUJob := &types.Job{
		ID:          "big-cpu",
		CPUShares:   200,
		MemoryLimit: 9999999999, // Should be ignored (no memory limit)
	}

	if agent.hasCapacity(bigCPUJob) {
		t.Error("hasCapacity should fail on CPU limit")
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

func TestAgentCapacityExactLimit(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100
	cfg.Capacity.Memory = 1024

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job exactly at limit should fit
	exactJob := &types.Job{
		ID:          "exact-job",
		CPUShares:   100,
		MemoryLimit: 1024,
	}

	if !agent.hasCapacity(exactJob) {
		t.Error("hasCapacity should return true for job at exact limit")
	}
}

func TestAgentCapacityMultipleRunningTasks(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 100

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add 3 jobs using 30 CPU each
	for i := 0; i < 3; i++ {
		jobID := "job-" + string(rune('a'+i))
		agent.StoreJob(&types.Job{
			ID:        jobID,
			CPUShares: 30,
		})
		agent.do(func(s *agentState) {
			s.tasks["task-"+string(rune('a'+i))] = &types.Task{
				ID:    "task-" + string(rune('a'+i)),
				JobID: jobID,
				State: types.TaskRunning,
			}
		})
	}

	time.Sleep(10 * time.Millisecond)

	// Used: 90, remaining: 10
	smallJob := &types.Job{ID: "small", CPUShares: 10}
	if !agent.hasCapacity(smallJob) {
		t.Error("hasCapacity should fit 10 CPU (90 used, 10 remaining)")
	}

	// This should not fit
	mediumJob := &types.Job{ID: "medium", CPUShares: 15}
	if agent.hasCapacity(mediumJob) {
		t.Error("hasCapacity should not fit 15 CPU (90 used, 10 remaining)")
	}
}
