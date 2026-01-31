package runner

import (
	"easyrun/internal/types"
)

// Runner interface for executing jobs
type Runner interface {
	// Run starts a job and returns the task
	Run(job *types.Job, ports map[string]int) (*types.Task, error)

	// Stop stops a running task
	Stop(task *types.Task) error

	// Status returns the current state of a task
	Status(task *types.Task) (types.TaskState, error)
}

// Config holds configuration for the runner
type Config struct {
	// RootfsBase is the base path for task directories
	RootfsBase string

	// ArtifactsDir is where downloaded artifacts are stored
	ArtifactsDir string

	// MaxCPUShares is the total CPU shares for nice calculation
	MaxCPUShares int

	// Chroot enables chroot isolation (requires static binaries)
	Chroot bool
}
