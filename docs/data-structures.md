# Data Structures

## Job

What the user wants to run.

```go
type Job struct {
    ID           string            // Unique identifier (auto-generated)
    Name         string            // Human-readable name (UNIQUE KEY for upsert)
    AgentID      string            // Pin to specific agent (optional)
    Driver       string            // "exec" (default) or "docker"
    Image        string            // Docker image (only for driver=docker)
    Artifact     *Artifact         // Binary/assets to download (optional)
    Command      string            // Command to execute (required for process, optional for Docker)
    Count        int               // Number of instances (see below)
    Ports        map[string]int    // Process: port name → host port (0=dynamic). Docker: port name → container port
    CPUShares    int               // Relative CPU priority (0 = no limiting)
    MemoryLimit  uint64            // Bytes (0 = no limiting)
    Env          map[string]string // Extra environment variables
    Tags         map[string]string // Labels for service discovery/grouping
    Volumes      map[string]string // host_path → task_path (symlinked / Docker -v)
    HealthCheck  *HealthCheck      // HTTP health check config (optional)
    MaxRestarts  int               // Max restart attempts (0=default 5, -1=unlimited)
    UpdatePolicy UpdatePolicy      // How to update: rolling | recreate | blue-green
}
```

### Image (Docker Support)

Run a Docker container instead of a process:

```json
{
  "name": "redis",
  "image": "redis:7",
  "count": 3,
  "ports": {"redis": 6379}
}
```

- If `image` is set, the agent uses the DockerRunner instead of ExecRunner
- `command` is optional for Docker (overrides the image's CMD if provided)
- `ports` values are **container ports** (host ports are always dynamically allocated)
- All other fields (env, volumes, cpu_shares, memory_limit, tags, health_check) work identically

### AgentID (Node Pinning)

Pin a job to a specific agent:

```json
{
  "name": "monitoring",
  "agent_id": "node-1",
  "command": "./monitor",
  "count": 1
}
```

If the pinned agent is not found, dispatch returns an error.

### UpdatePolicy

How job updates are rolled out when POST /v1/jobs is called with existing job name:

```go
type UpdatePolicy string

const (
    UpdateRolling   UpdatePolicy = "rolling"    // Replace 1 at a time (default)
    UpdateRecreate  UpdatePolicy = "recreate"   // Stop all, then start new
    UpdateBlueGreen UpdatePolicy = "blue-green" // Start new alongside old
)
```

| Policy | Downtime | Resources | Use Case |
|--------|----------|-----------|----------|
| `rolling` | None | Normal | Standard deployments |
| `recreate` | Yes | Minimal | Breaking changes, DB migrations |
| `blue-green` | None | 2x (temporary) | Canary testing, instant rollback |

### Count

| Value | Behavior |
|-------|----------|
| `count: 3` | Run 3 instances, spread via round-robin |
| `count: 0` or omitted | Default to 1 instance |
| `count: -1` | **Run on ALL agents** |

`count: -1` is useful for node-level services like easydns or monitoring agents. New agents automatically receive all `count: -1` jobs on registration.

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

**Fixed ports (process jobs):** If the specified port is already in use, the job will be rejected with an error.

**Docker jobs:** Port values are container ports. Host ports are always dynamically allocated. Example: `{"http": 80}` → `-p <random>:80`.

**Environment variables:** Task gets `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. for all ports (host ports).

### Volumes

Mount host directories into the task's working directory via symlinks:

```json
{
  "volumes": {
    "/data/shared": "data",
    "/etc/ssl/certs": "certs"
  }
}
```

- Host paths must exist (validation at task start)
- Target paths are relative to task directory
- Mounted as symlinks, unmounted on task cleanup

### Artifact

```go
type Artifact struct {
    URL     string            // Download URL (http://, https://, s3://)
    Headers map[string]string // HTTP headers (Authorization, X-API-Key, etc.)
    Auth    map[string]string // Other credentials (S3, helpers)
    Extract string            // "tar.gz", "tar.bz2", "zip", "" (empty = raw file)
}
```

**URL scheme determines which downloader to use.**

**Extract field:**
- `"tar.gz"` or `"tgz"` — extract tar.gz archive
- `"tar.bz2"` or `"tbz2"` — extract tar.bz2 archive
- `"zip"` — extract zip archive
- `""` (empty) — raw file, automatically `chmod +x`

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
    "X-API-Key": "secret"
  },
  "extract": "tar.gz"
}
```

Raw binary (no extraction):
```json
{
  "url": "https://releases.example.com/myapp-v1.0",
  "headers": { "Authorization": "Bearer token123" }
}
```
File is downloaded, `chmod +x`, ready to run.

S3:
```json
{
  "url": "s3://bucket/key.tar.gz",
  "auth": {
    "access_key": "AKIA...",
    "secret_key": "...",
    "region": "eu-west-1"
  },
  "extract": "tar.gz"
}
```

### HealthCheck

```go
type HealthCheck struct {
    Path           string        // HTTP path (e.g., "/health")
    Port           string        // Named port (default "http")
    Interval       time.Duration // Check interval (default 10s)
    Timeout        time.Duration // Request timeout (default 5s)
    InitialTimeout time.Duration // Max time after start to become healthy (default 30s)
}
```

**InitialTimeout:** Allows slow-starting services time to initialize before health checks begin failing.

## Task

A running instance of a Job.

```go
type Task struct {
    ID           string         // Unique identifier
    JobID        string         // Job ID (which version of the job)
    JobName      string         // Job name (which job this task belongs to)
    Driver       string         // "exec" or "docker"
    Image        string         // Docker image (only for driver=docker)
    Ports        map[string]int // Named port -> host port number
    Pid          int            // Process ID (0 for Docker tasks)
    State        TaskState      // running, stopped, failed
    StartedAt    time.Time
    RestartCount int            // Number of times restarted
}
```

**Note:** Task has both `JobID` and `JobName`. Use `task.JobName` to look up the job by name, `task.JobID` to reference the specific version.

**Note:** `task.Driver` determines which runner manages this task. `"exec"` = ExecRunner, `"docker"` = DockerRunner.

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
    Version  string    // Agent version (e.g., "v0.5.8")
    LastSeen time.Time // Last heartbeat
}
```

## Leader State (in-memory)

```go
type leaderState struct {
    agents      map[string]*Agent           // Registered agents
    placed      map[string]map[string]int   // agentID → jobID → count
    dispatching map[string]bool             // jobID → true if being dispatched
    settled     bool                        // false during settle period
    roundRobin  int                         // Counter for round-robin
}
```

Jobs are stored in the shared `JobStore` (owned by Agent, referenced by Leader).

All state access goes through a single goroutine via the `ops` channel, using `do()` (fire-and-forget) and `query()` (blocking with result) helpers.

The leader tracks:
- Which agents are online (via heartbeats)
- Which job instances run on which agents (`placed`: agentID → jobID → count)
- Which jobs are being actively dispatched (`dispatching`: prevents double dispatch)
- Whether the settle period has elapsed (`settled`: defers reconciliation until agents register)
- Round-robin counter for deterministic agent selection (agents sorted by ID)

**Settle period:** After becoming leader, the leader waits for `agentTimeout` (30s) before reconciling. This allows agents to register with their `placed` counts, preventing duplicate dispatches.

**Placement tracking:** `placed[agentID][jobID] = count` tracks how many instances of each job are on each agent. Updated on dispatch, cleared on agent death/unregister.

**Reconciliation:** After agent changes, `reconcileJob` compares desired vs actual state and dispatches the difference. Single code path for daemon (count=-1) and regular jobs. Skips jobs that are actively being dispatched.

**Delete:** `DeleteJobByID` uses two-phase approach (placement + cluster status) to catch orphaned tasks.
