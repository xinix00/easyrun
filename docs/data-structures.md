# Data Structures

## Job

What the user wants to run.

```go
type Job struct {
    ID          string            // Unique identifier
    Name        string            // Human-readable name
    Artifact    *Artifact         // Binary/assets to download (optional)
    Command     string            // Command to execute
    Count       int               // Number of instances (default: 1)
    Ports       map[string]int    // Port name -> fixed port (0 = dynamic)
    CPUShares   int               // Relative CPU priority (0 = no limiting)
    MemoryLimit uint64            // Bytes (0 = no limiting)
    Env         map[string]string // Extra environment variables
    Tags        map[string]string // Labels for service discovery/grouping
    HealthCheck *HealthCheck      // HTTP health check config (optional)
    MaxRestarts int               // Max restart attempts (0=default 5, -1=unlimited)
}
```

### Ports

Ports can be dynamic (assigned at runtime) or fixed:

```json
{
  "ports": {
    "http": 0,      // Dynamic - system assigns a free port
    "grpc": 0,      // Dynamic
    "metrics": 9090 // Fixed - must use port 9090
  }
}
```

**Fixed ports:** If the specified port is already in use, the job will be rejected with an error.

**Environment variables:** Task gets `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. for all ports.

### Artifact

```go
type Artifact struct {
    URL     string            // Download URL (http://, https://, s3://)
    Headers map[string]string // HTTP headers (Authorization, X-API-Key, etc.)
    Auth    map[string]string // Other credentials (S3, helpers)
}
```

**URL scheme determines which downloader to use.**

**HTTP/HTTPS downloaders:**
- Use `headers` for custom HTTP headers (direct pass-through)
- Or use `auth` helpers: `username`/`password` → generates Basic Auth header

**S3 downloader:**
- Use `auth` for S3 credentials: `access_key`, `secret_key`, `region`

**Examples:**

Custom headers:
```json
{
  "url": "https://artifacts.example.com/app.tar.gz",
  "headers": {
    "Authorization": "Bearer token123",
    "X-API-Key": "secret",
    "X-Tenant-ID": "123"
  }
}
```

Basic Auth helper:
```json
{
  "url": "https://artifacts.example.com/app.zip",
  "auth": {
    "username": "deploy",
    "password": "secret"
  }
}
```

S3:
```json
{
  "url": "s3://bucket/key",
  "auth": {
    "access_key": "AKIA...",
    "secret_key": "...",
    "region": "eu-west-1"
  }
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

**Ports:** Task gets ENV vars `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. for all allocated ports.

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
