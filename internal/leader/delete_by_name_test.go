package leader

import (
	"context"
	"testing"
	"time"

	"easyrun/internal/types"
)

// TestDeleteJobByName verifies delete by name works (API compatibility)
func TestDeleteJobByName(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Create job (Name is the unique key)
	job := &types.Job{
		Name:    "test-app",
		Command: "./test",
		Count:   1,
	}

	// Store via jobStore
	leader.jobStore.StoreJob(job)

	// Verify it exists via GetJob
	found := store.GetJob("test-app")
	if found == nil {
		t.Fatal("GetJob should find the job")
	}

	// Delete by name (like GUI does)
	leader.DeleteJobByName("test-app")
	time.Sleep(10 * time.Millisecond)

	// Verify deleted
	found = store.GetJob("test-app")
	if found != nil {
		t.Errorf("Job should be deleted, but found: Name=%s", found.Name)
	}

	jobs := store.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("Store should be empty, got %d jobs", len(jobs))
	}
}

// TestGetJobByName verifies jobStore.GetJob works with name-indexed store
func TestGetJobByName(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Store multiple jobs
	jobs := []*types.Job{
		{Name: "app-1", Command: "echo 1"},
		{Name: "app-2", Command: "echo 2"},
		{Name: "app-3", Command: "echo 3"},
	}

	for _, j := range jobs {
		store.StoreJob(j)
	}
	time.Sleep(10 * time.Millisecond)

	// GetJob should work
	found := store.GetJob("app-2")
	if found == nil {
		t.Fatal("GetJob should find app-2")
	}
	if found.Name != "app-2" {
		t.Errorf("Found job Name = %s, want app-2", found.Name)
	}

	// Non-existent name should return nil
	notFound := store.GetJob("nonexistent")
	if notFound != nil {
		t.Error("GetJob should return nil for nonexistent job")
	}
}
