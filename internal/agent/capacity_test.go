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
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// Set fake system info for predictable testing
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1 core = 1024 shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Small job should fit
	smallJob := &types.Job{
		ID:          "small",
		CPUShares:   500,
		MemoryLimit: 512,
	}
	if !agent.hasCapacity(smallJob) {
		t.Error("hasCapacity should return true for small job")
	}

	// Large job should not fit (exceeds 1024 CPU shares)
	largeJob := &types.Job{
		ID:          "large",
		CPUShares:   2000,
		MemoryLimit: 2048,
	}
	if agent.hasCapacity(largeJob) {
		t.Error("hasCapacity should return false for job exceeding capacity")
	}
}

func TestAgentCapacityWithRunningTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares, 1024 bytes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store a job and create a running task
	job := &types.Job{
		ID:          "existing-job",
		Name:        "existing",
		Command:     "echo",
		CPUShares:   600,
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
		CPUShares:   500,
		MemoryLimit: 500,
	}

	// Should not have capacity (600 + 500 > 1024)
	if agent.hasCapacity(newJob) {
		t.Error("hasCapacity should return false when existing tasks consume capacity")
	}
}

func TestAgentCapacityIgnoresFailedTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Store a job and create a FAILED task (should not count against capacity)
	job := &types.Job{
		ID:        "failed-job",
		CPUShares: 800,
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
		CPUShares: 500,
	}

	if !agent.hasCapacity(newJob) {
		t.Error("hasCapacity should ignore failed tasks")
	}
}

func TestAgentCapacityIgnoresStoppedTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	job := &types.Job{
		ID:        "stopped-job",
		CPUShares: 800,
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
		CPUShares: 500,
	}

	if !agent.hasCapacity(newJob) {
		t.Error("hasCapacity should ignore stopped tasks")
	}
}

func TestAgentCapacityUsesSystemDefaults(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 0 // Not configured - uses system default
	cfg.Capacity.Memory = 0    // Not configured - uses system default

	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// When config=0, system values are used as limits
	// A job within system capacity should fit
	smallJob := &types.Job{
		ID:          "small-job",
		CPUShares:   100,       // Should fit on any system
		MemoryLimit: 1024 * 1024, // 1MB - should fit on any system
	}

	if !agent.hasCapacity(smallJob) {
		t.Error("hasCapacity should return true for small job within system capacity")
	}

	// A job exceeding system capacity should NOT fit
	hugeJob := &types.Job{
		ID:          "huge-job",
		CPUShares:   1000000,        // More than any system has
		MemoryLimit: 1000000000000000, // 1PB - more than any system
	}

	if agent.hasCapacity(hugeJob) {
		t.Error("hasCapacity should return false for job exceeding system capacity")
	}
}

func TestAgentCapacityJobWithNoLimits(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job with no resource limits (0 means "don't check")
	noLimitJob := &types.Job{
		ID:          "no-limit-job",
		CPUShares:   0,
		MemoryLimit: 0,
	}

	// Should always fit (no limits to check)
	if !agent.hasCapacity(noLimitJob) {
		t.Error("hasCapacity should return true for job with no limits")
	}
}

func TestAgentCapacityExceedsMemory(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 100, MemoryBytes: 1024}) // lots of CPU, little memory

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job fits in CPU but exceeds memory
	bigMemJob := &types.Job{
		ID:          "big-mem",
		CPUShares:   100,  // Fits (100 cores * 1024 = 102400 shares)
		MemoryLimit: 2048, // Exceeds 1024 bytes
	}

	if agent.hasCapacity(bigMemJob) {
		t.Error("hasCapacity should fail when memory exceeds system limit")
	}
}

func TestAgentCapacityExceedsCPU(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024 * 1024}) // little CPU, lots of memory

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job fits in memory but exceeds CPU
	bigCPUJob := &types.Job{
		ID:          "big-cpu",
		CPUShares:   2000,       // Exceeds 1024 shares (1 core)
		MemoryLimit: 1024 * 1024, // Fits in 1GB
	}

	if agent.hasCapacity(bigCPUJob) {
		t.Error("hasCapacity should fail when CPU exceeds system limit")
	}
}

func TestAgentConcurrentCapacityCheck(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 10, MemoryBytes: 1024 * 1024})

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
				CPUShares: 100, // Fits in 10 cores * 1024 = 10240 shares
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
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares, 1024 bytes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Job exactly at limit should fit
	exactJob := &types.Job{
		ID:          "exact-job",
		CPUShares:   1024,
		MemoryLimit: 1024,
	}

	if !agent.hasCapacity(exactJob) {
		t.Error("hasCapacity should return true for job at exact limit")
	}
}

func TestAgentCapacityMultipleRunningTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024 * 1024}) // 1024 CPU shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add 3 jobs using 300 CPU each (total 900)
	for i := 0; i < 3; i++ {
		jobID := "job-" + string(rune('a'+i))
		agent.StoreJob(&types.Job{
			ID:        jobID,
			CPUShares: 300,
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

	// Used: 900, remaining: 124
	smallJob := &types.Job{ID: "small", CPUShares: 100}
	if !agent.hasCapacity(smallJob) {
		t.Error("hasCapacity should fit 100 CPU (900 used, 124 remaining)")
	}

	// This should not fit
	mediumJob := &types.Job{ID: "medium", CPUShares: 150}
	if agent.hasCapacity(mediumJob) {
		t.Error("hasCapacity should not fit 150 CPU (900 used, 124 remaining)")
	}
}
