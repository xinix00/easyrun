package leader

import (
	"context"
	"testing"
	"time"

	"easyrun/internal/types"
)

func TestUpdateJobRolling(t *testing.T) {
	// Speed up tests by reducing update delay
	oldDelay := RollingUpdateDelay
	RollingUpdateDelay = 10 * time.Millisecond
	defer func() { RollingUpdateDelay = oldDelay }()

	// Create mock agents
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	// Register agents
	for i, agent := range agents {
		agentID := string(rune('a' + i))
		leader.Heartbeat("agent-"+agentID, agent.URL(), nil, time.Time{})
	}

	// Deploy initial version
	oldJob := &types.Job{
		ID:      "my-app-id",
		Name:    "my-app",
		Command: "./app-v1",
		Count:   3,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second, // Short timeout for tests
		},
	}

	if err := leader.DispatchJob(oldJob); err != nil {
		t.Fatalf("Failed to dispatch initial job: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify 3 instances running (by ID)
	placement := leader.GetPlacement("my-app-id")
	if len(placement) != 3 {
		t.Fatalf("Expected 3 instances, got %d", len(placement))
	}

	// Update to new version with rolling policy (new ID will be assigned)
	newJob := &types.Job{
		ID:           "my-app-id-v2",
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        3,
		UpdatePolicy: types.UpdateRolling,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify still 3 instances (but with new ID after rolling update)
	newPlacement := leader.GetPlacement("my-app-id-v2")
	if len(newPlacement) != 3 {
		t.Errorf("Expected 3 instances after update, got %d", len(newPlacement))
	}

	// Verify job definition was updated
	updatedJob := store.GetJob("my-app")
	if updatedJob.Command != "./app-v2" {
		t.Errorf("Job command should be updated to ./app-v2, got %s", updatedJob.Command)
	}
}

func TestUpdateJobRecreate(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.Heartbeat("agent-1", agent.URL(), nil, time.Time{})

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
	}

	leader.DispatchJob(oldJob)
	time.Sleep(50 * time.Millisecond)

	// Update with recreate policy
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateRecreate,
	}

	if err := leader.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify job was updated (GetJob uses name as key in mock)
	jobs := store.GetJobs()
	var updatedJob *types.Job
	for _, j := range jobs {
		if j.Name == "my-app" {
			updatedJob = j
			break
		}
	}
	if updatedJob == nil {
		t.Fatal("Job my-app should exist in store")
	}
	if updatedJob.Command != "./app-v2" {
		t.Errorf("Job command should be updated to ./app-v2, got %s", updatedJob.Command)
	}
}

func TestUpdateJobBlueGreen(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.Heartbeat("agent-1", agent.URL(), nil, time.Time{})

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	leader.DispatchJob(oldJob)
	time.Sleep(50 * time.Millisecond)

	initialRunCalls := agent.RunCallCount()

	// Update with blue-green policy
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateBlueGreen,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.UpdateJob(newJob); err != nil {
		t.Fatalf("UpdateJob failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Blue-green should have dispatched new version (run call count increases)
	if agent.RunCallCount() <= initialRunCalls {
		t.Error("Blue-green should dispatch new version")
	}

	// Verify job was updated (GetJob uses name as key in mock)
	jobs := store.GetJobs()
	var updatedJob *types.Job
	for _, j := range jobs {
		if j.Name == "my-app" {
			updatedJob = j
			break
		}
	}
	if updatedJob == nil {
		t.Fatal("Job my-app should exist in store")
	}
	if updatedJob.Command != "./app-v2" {
		t.Errorf("Job command should be updated to ./app-v2, got %s", updatedJob.Command)
	}
}

func TestUpdateJobRollingFailureKeepsOld(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.Heartbeat("agent-1", agent.URL(), nil, time.Time{})

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.DispatchJob(oldJob); err != nil {
		t.Fatalf("Failed to dispatch initial job: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify 1 instance running
	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// Make agent fail next dispatch
	agent.SetFailRuns(true)

	// Try rolling update - should fail
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateRolling,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 500 * time.Millisecond,
		},
	}

	err := leader.UpdateJob(newJob)
	if err == nil {
		t.Fatal("UpdateJob should have failed")
	}

	time.Sleep(50 * time.Millisecond)

	// OLD INSTANCE SHOULD STILL BE RUNNING (this is the key assertion)
	if agent.TaskCount() != 1 {
		t.Errorf("Old instance should still be running, got %d tasks", agent.TaskCount())
	}

	// Job definition should NOT be updated
	storedJob := store.GetJob("my-app")
	if storedJob.Command != "./app-v1" {
		t.Errorf("Job should still be v1, got %s", storedJob.Command)
	}
}

func TestUpdateJobBlueGreenFailureKeepsOld(t *testing.T) {
	agent := newMockAgent()
	defer agent.Close()

	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	leader.Heartbeat("agent-1", agent.URL(), nil, time.Time{})

	// Deploy initial version
	oldJob := &types.Job{
		Name:    "my-app",
		Command: "./app-v1",
		Count:   1,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 2 * time.Second,
		},
	}

	if err := leader.DispatchJob(oldJob); err != nil {
		t.Fatalf("Failed to dispatch initial job: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Verify 1 instance running
	if agent.TaskCount() != 1 {
		t.Fatalf("Expected 1 task, got %d", agent.TaskCount())
	}

	// Make agent fail next dispatch
	agent.SetFailRuns(true)

	// Try blue-green update - should fail
	newJob := &types.Job{
		Name:         "my-app",
		Command:      "./app-v2",
		Count:        1,
		UpdatePolicy: types.UpdateBlueGreen,
		HealthCheck: &types.HealthCheck{
			InitialTimeout: 500 * time.Millisecond,
		},
	}

	err := leader.UpdateJob(newJob)
	if err == nil {
		t.Fatal("UpdateJob should have failed")
	}

	time.Sleep(50 * time.Millisecond)

	// OLD INSTANCE SHOULD STILL BE RUNNING
	if agent.TaskCount() != 1 {
		t.Errorf("Old instance should still be running, got %d tasks", agent.TaskCount())
	}

	// Job definition should NOT be updated (blue-green only updates after success)
	storedJob := store.GetJob("my-app")
	if storedJob.Command != "./app-v1" {
		t.Errorf("Job should still be v1, got %s", storedJob.Command)
	}
}

func TestUpdateJobNotFound(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	// Try to update non-existent job
	newJob := &types.Job{
		Name:    "nonexistent",
		Command: "./app",
	}

	err := leader.UpdateJob(newJob)
	if err == nil {
		t.Error("UpdateJob should fail for non-existent job")
	}
}

func TestFindJobByName(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local", store, nil)

	ctx, cancel := newTestContext()
	defer cancel()
	go leader.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	// Store jobs
	store.StoreJob(&types.Job{Name: "app-1", Command: "echo"})
	store.StoreJob(&types.Job{Name: "app-2", Command: "echo"})

	// Find by name
	job := leader.FindJobByName("app-1")
	if job == nil {
		t.Fatal("FindJobByName should find app-1")
	}
	if job.Name != "app-1" {
		t.Errorf("Expected app-1, got %s", job.Name)
	}

	// Not found
	job = leader.FindJobByName("nonexistent")
	if job != nil {
		t.Error("FindJobByName should return nil for nonexistent job")
	}
}

// Helper for creating test context with timeout
func newTestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}
