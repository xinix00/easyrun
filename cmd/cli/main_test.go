package main

import "testing"

// TestContract_ApplyCount pins that `run apply --count` reaches Job.Count.
// The docs use --count everywhere; if the wiring is dropped this goes red.
func TestContract_ApplyCount(t *testing.T) {
	job := buildJob("web", "./app", "", "", 3, 0, "", -1,
		nil, nil, nil, nil, "", "", "", 0, "rolling")
	if job.Count != 3 {
		t.Fatalf("--count not wired to Job.Count: got %d, want 3", job.Count)
	}

	// count -1 (run on all agents) must survive too.
	daemon := buildJob("dns", "./dns", "", "", -1, 0, "", -1,
		nil, nil, nil, nil, "", "", "", 0, "rolling")
	if daemon.Count != -1 {
		t.Fatalf("--count -1 not preserved: got %d", daemon.Count)
	}
}
