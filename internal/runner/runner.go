package runner

import (
	"fmt"
	"strings"

	"easyrun/internal/types"
)

// PortEnvVars builds ER_PORT_<NAME>=<port> environment variables for all ports
func PortEnvVars(ports map[string]int) []string {
	vars := make([]string, 0, len(ports))
	for name, port := range ports {
		upper := strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' {
				return r - 32
			}
			if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				return r
			}
			return '_'
		}, name)
		vars = append(vars, fmt.Sprintf("ER_PORT_%s=%d", upper, port))
	}
	return vars
}

// AttrEnvVars builds ER_ATTR_<KEY>=<value> environment variables for node attributes
// Dots and dashes in keys are replaced with underscores (e.g. node.os → ER_ATTR_NODE_OS)
func AttrEnvVars(attrs map[string]string) []string {
	vars := make([]string, 0, len(attrs))
	for name, val := range attrs {
		upper := strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' {
				return r - 32
			}
			if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
				return r
			}
			return '_'
		}, name)
		vars = append(vars, fmt.Sprintf("ER_ATTR_%s=%s", upper, val))
	}
	return vars
}

// Runner interface for executing jobs
type Runner interface {
	// Run starts a job and returns the task
	Run(job *types.Job, ports map[string]int) (*types.Task, error)

	// Stop stops a running task
	Stop(task *types.Task) error

	// Status returns the current state of a task
	Status(task *types.Task) (types.TaskState, error)

	// GetStdout returns the stdout log broadcaster for a task (nil if not available)
	GetStdout(taskID string) *LogBroadcaster

	// GetStderr returns the stderr log broadcaster for a task (nil if not available)
	GetStderr(taskID string) *LogBroadcaster

	// Cleanup removes all task directories (called at startup)
	Cleanup() error
}

// Config holds configuration for the runner
type Config struct {
	// RootfsBase is the base path for task directories
	RootfsBase string

	// ArtifactsDir is where downloaded artifacts are stored
	ArtifactsDir string

	// MaxCPUShares is the total CPU shares for nice calculation
	MaxCPUShares int

	// Isolate enables process isolation (chroot on Linux, sandbox on macOS)
	// Default: true for security
	Isolate bool
}
