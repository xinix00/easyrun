package leader

import (
	"context"
	"testing"
	"time"

	"easyrun/internal/types"
)

// TestGetPlacedByJobName_PreExistingJobs reproduces the bug:
// Leader starts with jobs already in the store (loaded from disk after restart).
// Agents register with placed counts. But GetPlacedByJobName() returned empty
// because nameToID was not seeded from the job store.
//
// Root cause: stateLoop initialized nameToID as empty map. It was only populated
// on DispatchJob (new job) or Heartbeat with newer stateTime (state sync).
// When leader loaded jobs from disk (same stateTime as agents), nameToID stayed empty.
// Fix: seed nameToID from jobStore.GetJobs() at stateLoop initialization.
func TestGetPlacedByJobName_PreExistingJobs(t *testing.T) {
	store := NewMockJobStore()

	// Pre-existing jobs in store (leader loaded from disk)
	jobs := []*types.Job{
		{ID: "webapp-id", Name: "webapp", Command: "./webapp", Count: 3},
		{ID: "api-id", Name: "api", Command: "./api", Count: 2},
		{ID: "worker-id", Name: "worker", Command: "./worker", Count: 1},
	}
	for _, j := range jobs {
		store.StoreJob(j)
	}
	store.stateTime = time.Now() // leader has current state

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Agents register with placed counts (ground truth from their running tasks)
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", map[string]int{
		"webapp-id": 2,
		"api-id":    1,
		"worker-id": 1,
	})
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", map[string]int{
		"webapp-id": 1,
		"api-id":    1,
	})
	time.Sleep(20 * time.Millisecond)

	// Heartbeat with same stateTime (NOT newer → nameToID NOT updated via heartbeat)
	l.Heartbeat("agent-1", "http://10.0.0.1:8080", jobs, store.stateTime, "")
	l.Heartbeat("agent-2", "http://10.0.0.2:8080", jobs, store.stateTime, "")
	time.Sleep(20 * time.Millisecond)

	// BUG: GetPlacedByJobName returned empty map because nameToID was empty
	placed := l.GetPlacedByJobName()

	if placed["webapp"] != 3 {
		t.Errorf("webapp placed = %d, want 3", placed["webapp"])
	}
	if placed["api"] != 2 {
		t.Errorf("api placed = %d, want 2", placed["api"])
	}
	if placed["worker"] != 1 {
		t.Errorf("worker placed = %d, want 1", placed["worker"])
	}

	// Also verify total
	total := 0
	for _, count := range placed {
		total += count
	}
	if total != 6 {
		t.Errorf("total placed = %d, want 6", total)
	}
}

// TestGetPlacedByJobName_AfterDispatch verifies placed counts work for newly dispatched jobs.
func TestGetPlacedByJobName_AfterDispatch(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	l.RegisterAgent("agent-1", agent.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		ID:      "job-id",
		Name:    "myapp",
		Command: "./myapp",
		Count:   3,
	}
	if err := l.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	placed := l.GetPlacedByJobName()
	if placed["myapp"] != 3 {
		t.Errorf("myapp placed = %d, want 3", placed["myapp"])
	}
}

// TestGetPlacedByJobName_MixedPreExistingAndNew verifies placed counts
// work correctly when some jobs are pre-existing and some are newly dispatched.
func TestGetPlacedByJobName_MixedPreExistingAndNew(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()

	// Pre-existing job
	existingJob := &types.Job{ID: "existing-id", Name: "existing", Command: "./existing", Count: 2}
	store.StoreJob(existingJob)
	store.stateTime = time.Now()

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Agent registers with placed count for existing job
	l.RegisterAgent("agent-1", agent.URL(), "", map[string]int{"existing-id": 2})
	time.Sleep(20 * time.Millisecond)

	// Dispatch a NEW job
	newJob := &types.Job{
		ID:      "new-id",
		Name:    "new-service",
		Command: "./new",
		Count:   1,
	}
	if err := l.DispatchJob(newJob); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	placed := l.GetPlacedByJobName()
	if placed["existing"] != 2 {
		t.Errorf("existing placed = %d, want 2", placed["existing"])
	}
	if placed["new-service"] != 1 {
		t.Errorf("new-service placed = %d, want 1", placed["new-service"])
	}
}

// TestGetPlacedByJobName_DaemonJob verifies placed counts for count=-1 (daemon) jobs.
func TestGetPlacedByJobName_DaemonJob(t *testing.T) {
	store := NewMockJobStore()

	daemonJob := &types.Job{ID: "daemon-id", Name: "easydns", Command: "./easydns", Count: -1}
	store.StoreJob(daemonJob)
	store.stateTime = time.Now()

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// 3 agents each running the daemon
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", map[string]int{"daemon-id": 1})
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", map[string]int{"daemon-id": 1})
	l.RegisterAgent("agent-3", "http://10.0.0.3:8080", "", map[string]int{"daemon-id": 1})
	time.Sleep(20 * time.Millisecond)

	placed := l.GetPlacedByJobName()
	if placed["easydns"] != 3 {
		t.Errorf("easydns placed = %d, want 3", placed["easydns"])
	}
}

// TestGetPlacedByJobName_UpdatedJob verifies that after a rolling update
// (old job ID → new job ID), placed counts still work correctly.
// Placed is tracked via trackPlacement (dispatch) and RegisterAgent, NOT heartbeat.
func TestGetPlacedByJobName_UpdatedJob(t *testing.T) {
	agent1 := newMockAgent()
	agent2 := newMockAgent()
	defer agent1.Close()
	defer agent2.Close()

	store := NewMockJobStore()
	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Register agents and dispatch v1
	l.RegisterAgent("agent-1", agent1.URL(), "", nil)
	l.RegisterAgent("agent-2", agent2.URL(), "", nil)
	time.Sleep(20 * time.Millisecond)

	jobV1 := &types.Job{ID: "v1-id", Name: "myapp", Command: "./myapp-v1", Count: 2}
	if err := l.DispatchJob(jobV1); err != nil {
		t.Fatalf("DispatchJob v1 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	placed := l.GetPlacedByJobName()
	if placed["myapp"] != 2 {
		t.Errorf("before update: myapp placed = %d, want 2", placed["myapp"])
	}

	// Rolling update: v1 → v2 (trackPlacement handles new placed counts)
	jobV2 := &types.Job{ID: "v2-id", Name: "myapp", Command: "./myapp-v2", Count: 2}
	if err := l.UpdateJob(jobV2); err != nil {
		t.Fatalf("UpdateJob v2 failed: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// After rolling update: nameToID points to v2-id, placed has v2-id counts
	placed = l.GetPlacedByJobName()
	if placed["myapp"] != 2 {
		t.Errorf("after update: myapp placed = %d, want 2", placed["myapp"])
	}
}

// TestGetPlacedByJobName_LeaderWithSettlePeriod verifies placed counts
// work during and after the settle period.
func TestGetPlacedByJobName_LeaderWithSettlePeriod(t *testing.T) {
	store := NewMockJobStore()

	jobs := []*types.Job{
		{ID: "job-a-id", Name: "job-a", Command: "./a", Count: 5},
		{ID: "job-b-id", Name: "job-b", Command: "./b", Count: 3},
	}
	for _, j := range jobs {
		store.StoreJob(j)
	}
	store.stateTime = time.Now()

	l := New("leader", store, nil)
	l.SetSettleDelay(200 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)

	// Agents register during settle period
	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", map[string]int{
		"job-a-id": 3,
		"job-b-id": 2,
	})
	l.RegisterAgent("agent-2", "http://10.0.0.2:8080", "", map[string]int{
		"job-a-id": 2,
		"job-b-id": 1,
	})
	time.Sleep(20 * time.Millisecond)

	// Even DURING settle, placed counts should be accurate
	placed := l.GetPlacedByJobName()
	if placed["job-a"] != 5 {
		t.Errorf("during settle: job-a placed = %d, want 5", placed["job-a"])
	}
	if placed["job-b"] != 3 {
		t.Errorf("during settle: job-b placed = %d, want 3", placed["job-b"])
	}

	// After settle, counts should still be correct
	time.Sleep(300 * time.Millisecond)

	placed = l.GetPlacedByJobName()
	if placed["job-a"] != 5 {
		t.Errorf("after settle: job-a placed = %d, want 5", placed["job-a"])
	}
	if placed["job-b"] != 3 {
		t.Errorf("after settle: job-b placed = %d, want 3", placed["job-b"])
	}
}

// TestNameToID_SeededFromJobStore verifies that nameToID is populated
// from the job store at stateLoop initialization time.
func TestNameToID_SeededFromJobStore(t *testing.T) {
	store := NewMockJobStore()

	jobs := []*types.Job{
		{ID: "id-1", Name: "svc-a", Command: "./a", Count: 1},
		{ID: "id-2", Name: "svc-b", Command: "./b", Count: 1},
		{ID: "id-3", Name: "svc-c", Command: "./c", Count: 1},
	}
	for _, j := range jobs {
		store.StoreJob(j)
	}

	l := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// FindJobByName uses nameToID internally
	for _, j := range jobs {
		found := l.FindJobByName(j.Name)
		if found == nil {
			t.Errorf("FindJobByName(%q) returned nil, want job with ID %q", j.Name, j.ID)
		} else if found.ID != j.ID {
			t.Errorf("FindJobByName(%q).ID = %q, want %q", j.Name, found.ID, j.ID)
		}
	}
}
