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

	// Create job with ID
	job := &types.Job{
		ID:      "test-id-123",
		Name:    "test-app",
		Command: "./test",
		Count:   1,
	}

	// Store via leader's store (not direct store access)
	leader.jobStore.StoreJob(job)

	t.Logf("Stored job: ID=%s, Name=%s", job.ID, job.Name)
	t.Logf("Jobs in store via GetJobs: %d", len(store.GetJobs()))

	// Verify it exists via FindJobByName
	found := leader.FindJobByName("test-app")
	if found == nil {
		t.Fatal("FindJobByName should find the job")
	}
	if found.Name != "test-app" {
		t.Errorf("Job name = %s, want test-app", found.Name)
	}
	t.Logf("Found job before delete: ID=%s, Name=%s", found.ID, found.Name)

	// Delete by name (like GUI does)
	t.Log("Calling DeleteJob('test-app')")
	leader.DeleteJob("test-app")
	time.Sleep(10 * time.Millisecond)

	// Verify deleted
	found = leader.FindJobByName("test-app")
	if found != nil {
		t.Errorf("Job should be deleted, but found: ID=%s, Name=%s", found.ID, found.Name)
	}

	jobs := store.GetJobs()
	if len(jobs) != 0 {
		t.Errorf("Store should be empty, got %d jobs", len(jobs))
	}
}

// TestFindJobByNameWithIDBasedStore verifies FindJobByName works with ID-indexed store
func TestFindJobByNameWithIDBasedStore(t *testing.T) {
	store := NewMockJobStore()
	leader := New("leader", store, nil)

	// Store multiple jobs with different IDs but known names
	jobs := []*types.Job{
		{ID: "id-1", Name: "app-1", Command: "echo 1"},
		{ID: "id-2", Name: "app-2", Command: "echo 2"},
		{ID: "id-3", Name: "app-3", Command: "echo 3"},
	}

	for _, j := range jobs {
		store.StoreJob(j)
	}

	// FindJobByName should work
	found := leader.FindJobByName("app-2")
	if found == nil {
		t.Fatal("FindJobByName should find app-2")
	}
	if found.ID != "id-2" {
		t.Errorf("Found job ID = %s, want id-2", found.ID)
	}

	// Non-existent name should return nil
	notFound := leader.FindJobByName("nonexistent")
	if notFound != nil {
		t.Error("FindJobByName should return nil for nonexistent job")
	}
}
