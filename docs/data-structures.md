# Data Structures

## Job

What the user wants to run.

```go
type Job struct {
    ID          string            // Unique identifier
    Name        string            // Human-readable name
    ArtifactURL string            // Download URL for binary/zip (optional)
    Command     string            // Command to execute
    Count       int               // Number of instances (default: 1)
    Ports       []string          // Named ports (["http", "grpc", "metrics"])
    CPUShares   int               // Relative CPU priority (0 = no limiting)
    MemoryLimit uint64            // Bytes (0 = no limiting)
    Env         map[string]string // Extra environment variables
    Tags        map[string]string // Labels for service discovery/grouping
    HealthCheck *HealthCheck      // HTTP health check config (optional)
    MaxRestarts int               // Max restart attempts (0=default 5, -1=unlimited)
}
```

### HealthCheck

```go
type HealthCheck struct {
    Path     string        // HTTP path (e.g., "/health")
    Port     string        // Named port (default "http")
    Interval time.Duration // Check interval (default 10s)
    Timeout  time.Duration // Request timeout (default 5s)
}
```

## Task

A running instance of a Job.

```go
type Task struct {
    ID           string         // Unique identifier
    JobID        string         // Which job this is
    JobName      string         // Job name (for display)
    Ports        map[string]int // Named port -> port number
    Pid          int            // Process ID
    State        TaskState      // running, stopped, failed
    StartedAt    time.Time
    RestartCount int            // Number of times restarted
}
```

**Named Ports:** Task gets ENV vars `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. for all ports in the map.

### Task States

| State | Meaning |
|-------|---------|
| `running` | Process is running |
| `stopped` | Intentionally stopped |
| `failed` | Crashed, OOM killed, etc |

## Agent

A registered agent with the leader.

```go
type Agent struct {
    ID       string    // Unique identifier
    Endpoint string    // HTTP endpoint (http://ip:port)
    LastSeen time.Time // Last heartbeat
}
```

## Leader State (in-memory)

```go
type Leader struct {
    agents    map[string]*Agent     // Registered agents
    jobs      map[string]*Job       // All jobs (shared with agent)
    placement map[string][]string   // jobID -> []agentID (multiple instances)
}
```

The leader tracks:
- Which agents are online (via heartbeats)
- Which jobs exist
- Which job instances run on which agents

**Multi-instance support:** A job with Count=3 has 3 entries in placement, spread across agents via round-robin.

On agent failure, only instances on that agent are redispatched.
