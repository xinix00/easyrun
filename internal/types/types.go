package types

import "time"

// TaskState represents the current state of a task
type TaskState string

const (
	TaskRunning TaskState = "running"
	TaskFailed  TaskState = "failed"
	TaskStopped TaskState = "stopped"
)

// HealthCheck configuration for a job
type HealthCheck struct {
	Path     string        `json:"path"`               // e.g., "/health"
	Port     string        `json:"port,omitempty"`     // named port (default "http")
	Interval time.Duration `json:"interval,omitempty"` // default 10s
	Timeout  time.Duration `json:"timeout,omitempty"`  // default 5s
}

// Job defines what the user wants to run
type Job struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	ArtifactURL string            `json:"artifact_url,omitempty"`
	Command     string            `json:"command"`
	Count       int               `json:"count,omitempty"`         // number of instances (default 1)
	Ports       []string          `json:"ports,omitempty"`         // named ports (e.g., ["http", "grpc"])
	CPUShares   int               `json:"cpu_shares,omitempty"`
	MemoryLimit uint64            `json:"memory_limit,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`          // labels for discovery/grouping
	HealthCheck *HealthCheck      `json:"health_check,omitempty"`
	MaxRestarts int               `json:"max_restarts,omitempty"` // 0 = unlimited
}

// Task represents a running instance of a Job
type Task struct {
	ID           string         `json:"id"`
	JobID        string         `json:"job_id"`
	JobName      string         `json:"job_name"`
	Ports        map[string]int `json:"ports"`         // named port -> port number
	Pid          int            `json:"pid"`
	State        TaskState      `json:"state"`
	StartedAt    time.Time      `json:"started_at"`
	RestartCount int            `json:"restart_count"`
}

// Agent represents a registered agent
type Agent struct {
	ID       string    `json:"id"`
	Endpoint string    `json:"endpoint"` // http://ip:port
	LastSeen time.Time `json:"last_seen"`
}
