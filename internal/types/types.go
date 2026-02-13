package types

import "time"

// TaskState represents the current state of a task
type TaskState string

const (
	TaskRunning  TaskState = "running"
	TaskStopping TaskState = "stopping"
	TaskFailed   TaskState = "failed"
	TaskStopped  TaskState = "stopped"
)

// Artifact describes where to download the application binary/assets
type Artifact struct {
	URL     string            `json:"url"`               // http://, https://, s3://
	Headers map[string]string `json:"headers,omitempty"` // HTTP headers (Authorization, X-API-Key, etc.)
	Auth    map[string]string `json:"auth,omitempty"`    // other credentials (S3: access_key/secret_key/region)
	Extract string            `json:"extract,omitempty"` // "tar.gz", "zip", "" (empty = raw file, chmod +x, no extraction)
}

// HealthCheck configuration for a job
type HealthCheck struct {
	Path           string        `json:"path"`                      // e.g., "/health"
	Port           string        `json:"port,omitempty"`            // named port (default "http")
	Interval       time.Duration `json:"interval,omitempty"`        // check interval (default 10s)
	Timeout        time.Duration `json:"timeout,omitempty"`         // per-request timeout (default 5s)
	InitialTimeout time.Duration `json:"initial_timeout,omitempty"` // max time after start to become healthy (default 30s)
}

// UpdatePolicy defines how job updates are handled
type UpdatePolicy string

const (
	// UpdateRolling replaces instances one at a time (zero downtime)
	UpdateRolling UpdatePolicy = "rolling"

	// UpdateRecreate stops all instances, then starts new version (downtime but simple)
	UpdateRecreate UpdatePolicy = "recreate"

	// UpdateBlueGreen starts new version alongside old, then switches (zero downtime, 2x resources)
	UpdateBlueGreen UpdatePolicy = "blue-green"
)

// Driver identifies which runner executes a job/task
const (
	DriverExec   = "exec"
	DriverDocker = "docker"
)

// DriverFor returns the driver for a given image (empty = exec, non-empty = docker)
func DriverFor(image string) string {
	if image != "" {
		return DriverDocker
	}
	return DriverExec
}

// Job defines what the user wants to run
type Job struct {
	ID           string            `json:"id,omitempty"`         // unique ID (generated)
	Name         string            `json:"name"`                 // user-facing name (for upsert)
	Affinity     map[string]string `json:"affinity,omitempty"`   // node attribute constraints (AND logic, equality)
	Driver       string            `json:"driver,omitempty"`     // "exec" (default) or "docker"
	Image        string            `json:"image,omitempty"`    // Docker image (only for driver=docker)
	Artifact     *Artifact         `json:"artifact,omitempty"` // binary/assets to download
	Command      string            `json:"command,omitempty"`
	Count        int               `json:"count,omitempty"`          // number of instances (default 1)
	Ports        map[string]int    `json:"ports,omitempty"`          // process: port name -> host port (0 = dynamic), docker: port name -> container port
	CPUShares    int               `json:"cpu_shares,omitempty"`
	MemoryLimit  uint64            `json:"memory_limit,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`    // labels for discovery/grouping
	Volumes      map[string]string `json:"volumes,omitempty"` // host_path -> task_path (symlinked)
	HealthCheck  *HealthCheck      `json:"health_check,omitempty"`
	MaxRestarts  int               `json:"max_restarts,omitempty"`  // 0 = unlimited
	UpdatePolicy UpdatePolicy      `json:"update_policy,omitempty"` // rolling (default) | recreate | blue-green
}

// Task represents a running instance of a Job
type Task struct {
	ID           string         `json:"id"`
	JobID        string         `json:"job_id"`
	JobName      string         `json:"job_name"`
	Driver       string         `json:"driver"`          // "exec" or "docker"
	Image        string         `json:"image,omitempty"` // Docker image (only for driver=docker)
	Ports        map[string]int `json:"ports"`           // named port -> host port number
	Pid          int            `json:"pid"`
	State        TaskState      `json:"state"`
	StartedAt    time.Time      `json:"started_at"`
	RestartCount int            `json:"restart_count"`
	CPUShares    int            `json:"cpu_shares,omitempty"`
	MemoryLimit  uint64         `json:"memory_limit,omitempty"`
}

// Agent represents a registered agent
type Agent struct {
	ID       string    `json:"id"`
	Endpoint string    `json:"endpoint"` // http://ip:port
	Version  string    `json:"version"`  // Agent version (e.g., "v0.1.0")
	LastSeen time.Time `json:"last_seen"`
}
