package leader

import (
	"context"
	"math"
	"testing"
	"time"

	"easyrun/internal/types"
)

// ============== PRIORITY & PREEMPTION TESTS ==============

func prio(n int) *int { return &n }

// TestEffectivePriority verifies the priority ordering semantics.
// nil = not set = sorts last (math.MaxInt). 0 = top (most important).
func TestEffectivePriority(t *testing.T) {
	cases := []struct {
		p    *int
		want int
	}{
		{prio(0), 0},
		{prio(1), 1},
		{prio(5), 5},
		{prio(100), 100},
		{nil, math.MaxInt},
	}
	for _, c := range cases {
		if got := effectivePriority(c.p); got != c.want {
			t.Errorf("effectivePriority(%v) = %d, want %d", c.p, got, c.want)
		}
	}
}

// TestPreemptionHighPriorityEvictsLow verifies that a high-priority job preempts
// a low-priority job when an agent is at capacity.
func TestPreemptionHighPriorityEvictsLow(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	lowPrio := &types.Job{Name: "batch", Command: "./batch", Count: 1, Priority: prio(10)}
	if err := l.DispatchJob(lowPrio); err != nil {
		t.Fatalf("Dispatch low-prio job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task on agent, got %d", agent.TaskCount())
	}

	highPrio := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(highPrio); err != nil {
		t.Fatalf("Dispatch high-prio job failed (preemption should have succeeded): %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Errorf("Expected 1 task after preemption, got %d", agent.TaskCount())
	}
	jobs := agent.GetJobs()
	found := false
	for _, j := range jobs {
		if j.Name == "critical" {
			found = true
		}
	}
	if !found {
		t.Error("High-priority job 'critical' should be running after preemption")
	}
}

// TestPreemptionLowPriorityCannotEvictHigh verifies that a low-priority job
// does NOT evict a higher-priority job when an agent is at capacity.
func TestPreemptionLowPriorityCannotEvictHigh(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	highPrio := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(highPrio); err != nil {
		t.Fatalf("Dispatch high-prio job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	lowPrio := &types.Job{Name: "batch", Command: "./batch", Count: 1, Priority: prio(10)}
	if err := l.DispatchJob(lowPrio); err == nil {
		t.Error("Low-priority job should not preempt a higher-priority job")
	}

	for _, j := range agent.GetJobs() {
		if j.Name == "batch" {
			t.Error("Low-priority job 'batch' should not have been placed via preemption")
		}
	}
}

// TestPreemptionPrio0IsUnevictable verifies that a job with priority=0
// (most important) cannot be evicted by any other job.
func TestPreemptionPrio0IsUnevictable(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	first := &types.Job{Name: "first", Command: "./first", Count: 1, Priority: prio(0)}
	if err := l.DispatchJob(first); err != nil {
		t.Fatalf("Dispatch prio-0 job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	later := &types.Job{Name: "later", Command: "./later", Count: 1, Priority: prio(5)}
	if err := l.DispatchJob(later); err == nil {
		t.Error("Later job (prio=5) should not preempt prio=0 job")
	}
	for _, j := range agent.GetJobs() {
		if j.Name == "later" {
			t.Error("'later' (prio=5) should not have displaced 'first' (prio=0)")
		}
	}
}

// TestPreemptionNilPriorityIsLowest verifies that a job with nil priority
// (not set) sorts last and can be evicted by any job with explicit priority.
func TestPreemptionNilPriorityIsLowest(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	// Job with nil priority = lowest (auto/unset).
	bg := &types.Job{Name: "background", Command: "./bg", Count: 1} // Priority: nil
	if err := l.DispatchJob(bg); err != nil {
		t.Fatalf("Dispatch nil-prio job failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Any explicit priority (even the highest number) should preempt nil.
	fg := &types.Job{Name: "foreground", Command: "./fg", Count: 1, Priority: prio(99)}
	if err := l.DispatchJob(fg); err != nil {
		t.Fatalf("Explicit-prio job should preempt nil-prio job: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	found := false
	for _, j := range agent.GetJobs() {
		if j.Name == "foreground" {
			found = true
		}
	}
	if !found {
		t.Error("'foreground' should be running after preempting nil-priority job")
	}
}

// TestPreemptionAffinityAgentNeverEvicts verifies that an agent that returned 406
// (affinity mismatch) is never a candidate for preemption.
func TestPreemptionAffinityAgentNeverEvicts(t *testing.T) {
	agentA := newMockAgent()
	defer agentA.Close()
	agentA.SetRejectAffinity(true)

	agentB := newMockAgent()
	defer agentB.Close()
	agentB.SetFailRuns(true)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-a", agentA.URL(), "", nil)
	l.RegisterAgent("agent-b", agentB.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(job); err == nil {
		t.Error("Dispatch should fail: agent-a rejects via affinity, agent-b always 503 with no placed jobs")
	}
}

// TestPreemptionChoosesLowestPriority verifies that among multiple placed jobs,
// the one with the highest priority number (least important) is evicted first.
func TestPreemptionChoosesLowestPriority(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(2)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	medPrio := &types.Job{Name: "medium", Command: "./med", Count: 1, Priority: prio(5)}
	lowPrio := &types.Job{Name: "batch", Command: "./batch", Count: 1, Priority: prio(20)}
	if err := l.DispatchJob(medPrio); err != nil {
		t.Fatalf("Dispatch medium-prio job failed: %v", err)
	}
	if err := l.DispatchJob(lowPrio); err != nil {
		t.Fatalf("Dispatch low-prio job failed: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	if agent.TaskCount() != 2 {
		t.Fatalf("Expected 2 tasks on agent, got %d", agent.TaskCount())
	}

	highPrio := &types.Job{Name: "critical", Command: "./critical", Count: 1, Priority: prio(1)}
	if err := l.DispatchJob(highPrio); err != nil {
		t.Fatalf("High-priority dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	for _, j := range agent.GetJobs() {
		if j.Name == "batch" {
			t.Error("'batch' (prio=20) should have been evicted, not 'medium' (prio=5)")
		}
	}
	found := false
	for _, j := range agent.GetJobs() {
		if j.Name == "critical" {
			found = true
		}
	}
	if !found {
		t.Error("High-priority job 'critical' should be running")
	}
}

// TestPatchPriorityTriggersPreemption verifies that patching a job's priority
// triggers reconciliation and preempts lower-priority jobs to free capacity.
func TestPatchPriorityTriggersPreemption(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()
	agent.SetMaxCapacity(1)

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	// counter dispatched first, fills the only slot (prio=10, low importance)
	counter := &types.Job{Name: "counter", Command: "./counter", Count: 1, Priority: prio(10)}
	if err := l.DispatchJob(counter); err != nil {
		t.Fatalf("Dispatch counter failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// counter2 dispatched second — no capacity, stays pending (prio=20, even lower)
	counter2 := &types.Job{Name: "counter2", Command: "./counter2", Count: 1, Priority: prio(20)}
	_ = l.DispatchJob(counter2) // expected to fail (no capacity)
	time.Sleep(20 * time.Millisecond)

	// Patch counter2 to priority 0 (most important) — should trigger reconcile+preempt
	if err := l.PatchJobPriority("counter2", 0); err != nil {
		t.Fatalf("PatchJobPriority failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// counter2 should now be running, counter should have been evicted
	found := false
	for _, j := range agent.GetJobs() {
		if j.Name == "counter2" {
			found = true
		}
		if j.Name == "counter" {
			t.Error("'counter' (prio=10) should have been evicted after counter2 was patched to prio=0")
		}
	}
	if !found {
		t.Error("'counter2' should be running after priority patch triggered preemption")
	}
}
