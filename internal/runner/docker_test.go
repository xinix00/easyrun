//go:build !tamago

package runner

import (
	"testing"

	"github.com/xinix00/hop/internal/types"
)

func TestDockerRunnerRequiresImage(t *testing.T) {
	r := NewDockerRunner(nil, "")
	job := &types.Job{
		Name:    "no-image",
		Command: "echo hello",
	}

	task := &types.Task{ID: "test-task"}
	err := r.Run(job, task)
	if err == nil {
		t.Error("Run should fail without image")
	}
}

func TestDockerRunnerStatusNotFound(t *testing.T) {
	r := NewDockerRunner(nil, "")
	task := &types.Task{
		ID:    "nonexistent-container-id",
		Image: "nginx:latest",
	}

	state, err := r.Status(task)
	if err != nil {
		t.Fatalf("Status should not error: %v", err)
	}
	if state != types.TaskFailed {
		t.Errorf("Status = %q, want %q for nonexistent container", state, types.TaskFailed)
	}
}

func TestDockerRunnerStopNonExistent(t *testing.T) {
	r := NewDockerRunner(nil, "")
	task := &types.Task{
		ID:    "nonexistent-container-id",
		Image: "nginx:latest",
	}

	// Stop should not panic or return error for nonexistent container
	err := r.Stop(task)
	if err != nil {
		t.Errorf("Stop should not error for nonexistent container: %v", err)
	}
}

func TestDockerRunnerGetStdoutStderrNil(t *testing.T) {
	r := NewDockerRunner(nil, "")

	if r.GetStdout("nonexistent") != nil {
		t.Error("GetStdout should return nil for unknown task")
	}
	if r.GetStderr("nonexistent") != nil {
		t.Error("GetStderr should return nil for unknown task")
	}
}

func TestDockerRunnerCleanup(t *testing.T) {
	r := NewDockerRunner(nil, "")

	// Cleanup should not error even if docker is not available
	err := r.Cleanup()
	if err != nil {
		t.Errorf("Cleanup should not error: %v", err)
	}
}
