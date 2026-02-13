# HTTP API

There are two APIs: the **Leader API** (port+1000) and the **Agent API** (port).

## Leader API

Runs on the node that is leader (via easyraft). Default port: 9080.

### Health

```
GET /health
```

Returns `{"status": "ok"}`.

### Cluster Status

```
GET /v1/status
```

Returns cluster overview:
```json
{
  "agents": 3,
  "total_tasks": 5,
  "running_tasks": 5,
  "settling": false,
  "tasks_by_agent": {
    "agent-1": [{"id": "abc", "job_id": "def", "job_name": "web", "state": "running", ...}],
    "agent-2": [...]
  }
}
```

**settling:** `true` during the settle period after leader election (30s). During this period, jobs are stored but not dispatched until agents have registered with their placed counts.

### Agents

```
GET  /v1/agents            # All registered agents
POST /v1/agents            # Register agent (with placed counts)
DELETE /v1/agents/{id}     # Unregister agent (triggers reconciliation)
```

#### Register Agent

```
POST /v1/agents
```

Called on agent startup and on leader change:
```json
{
  "id": "agent-1",
  "endpoint": "http://10.0.0.5:8080",
  "version": "v0.5.8",
  "placed": {
    "job-id-abc": 2,
    "job-id-def": 1
  }
}
```

**placed:** Map of jobID → count, telling the leader what this agent is already running. This prevents duplicate dispatches during leader failover.

**Response:**
```json
{
  "status": "registered",
  "jobs": [...],
  "state_time": "2025-01-31T12:00:00Z"
}
```

### Heartbeat

```
POST /v1/heartbeat
```

Agents send this every 10s to stay registered:
```json
{
  "id": "agent-1",
  "endpoint": "http://10.0.0.5:8080",
  "version": "v0.5.8",
  "jobs": [...],
  "state_time": "2025-01-31T12:00:00Z"
}
```

**Response (known agent):**
```json
{
  "status": "ok",
  "jobs": [...],
  "state_time": "2025-01-31T12:00:00Z"
}
```

**Response (unknown agent):** `404 Not Found` — agent should re-register via POST /v1/agents.

**State sync:** If the agent's `state_time` is newer than the leader's, the leader syncs jobs from the agent. The response always includes the leader's current jobs for the agent to sync.

### Jobs

```
GET    /v1/jobs            # All jobs
POST   /v1/jobs            # Run or update job (upsert based on name)
DELETE /v1/jobs/{name}     # Delete job and all its tasks
```

#### Run or Update Job (Upsert)

**POST /v1/jobs performs upsert:**
- If job with this `name` exists → **UPDATE** (according to `update_policy`)
- If job doesn't exist → **INSERT** (dispatch new job)

**Full example:**
```bash
curl -X POST http://localhost:9080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api",
    "affinity": {"node.arch": "amd64"},
    "artifact": {
      "url": "s3://mybucket/api-v2.0.tar.gz",
      "auth": {
        "access_key": "AKIAIOSFODNN7EXAMPLE",
        "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region": "eu-west-1"
      },
      "extract": "tar.gz"
    },
    "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
    "count": 3,
    "ports": {"http": 0, "grpc": 0, "metrics": 9090},
    "cpu_shares": 2048,
    "memory_limit": 536870912,
    "env": {
      "DB_HOST": "postgres.internal",
      "LOG_LEVEL": "info"
    },
    "tags": {
      "service": "api",
      "loadbalancer_domain": "*.example.com"
    },
    "volumes": {
      "/data/shared": "data"
    },
    "health_check": {
      "path": "/health",
      "port": "http",
      "interval": "10s",
      "timeout": "5s",
      "initial_timeout": "30s"
    },
    "max_restarts": 5,
    "update_policy": "rolling"
  }'
```

**Fields:**
- `name` (string, **required**): Job name — **unique key for upsert**
- `driver` (string): `"exec"` (default) or `"docker"` — auto-derived from `image` if not set
- `image` (string): Docker image (sets driver to `"docker"` automatically)
- `affinity` (map): Node attribute constraints, AND logic (optional). Example: `{"node.arch": "arm64"}`. Agent rejects with 406 if no match.
- `artifact` (object): Binary/assets to download (optional)
  - `url` (string): URL with scheme — determines downloader (http://, s3://)
  - `headers` (map): HTTP headers (Authorization, X-API-Key, etc.)
  - `auth` (map): Credentials (S3: access_key/secret_key/region, HTTP: username/password)
  - `extract` (string): Archive type — `tar.gz`, `tar.bz2`, `zip`, or `""` (raw binary, auto chmod +x)
- `command` (string): Command to execute (required for process jobs, optional for Docker — overrides CMD)
- `count` (int): Number of instances (default 1, -1 = all agents)
- `ports` (map): Process: port name → host port (0=dynamic). Docker: port name → container port (host always dynamic). ENV vars `ER_PORT_<NAME>`
- `cpu_shares` (int): CPU priority (nice-based)
- `memory_limit` (uint64): Memory limit in bytes
- `env` (map): Environment variables
- `tags` (map): Labels for service discovery
- `volumes` (map): Host path → task path (symlinked)
- `health_check`: HTTP health monitoring
  - `port` (string): Named port to check (default "http")
  - `initial_timeout` (duration): Grace period for slow-starting services (default 30s)
- `max_restarts` (int): Max restart attempts (0=default 5, -1=unlimited)
- `update_policy` (string): `rolling` (default), `recreate`, or `blue-green`

**Artifact Downloaders:**

URL scheme → downloader:
- `http://`, `https://` → HTTP downloader
  - Uses `headers` for custom headers
  - Or `auth.username`/`auth.password` → generates Basic Auth header
- `s3://bucket/key` → S3 downloader
  - Uses `auth.access_key`, `auth.secret_key`, `auth.region`

**Response (INSERT):**
```json
{
  "id": "abc123",
  "name": "api",
  "status": "dispatched"
}
```

**Response (UPDATE):**
```json
{
  "id": "def456",
  "name": "api",
  "status": "updated",
  "policy": "rolling"
}
```

**Note:** When updating, a **new Job ID is generated** (old and new version coexist temporarily during update). Job **name** is the unique key for upsert.

**Update Policies:**

| Policy | Downtime | Behavior |
|--------|----------|----------|
| `rolling` (default) | None | Start new → stop old, 1 at a time with 2s delay |
| `recreate` | Yes | Stop all → start new version |
| `blue-green` | None | Start all new → stop all old (2x resources during switch) |

## Agent API

Runs on each node. Default port: 8080.

Agents also proxy `/v1/*` endpoints to the leader for cluster-wide operations.

### Health

```
GET /health
```

### Leader

```
GET /leader
```

Returns the current leader address:
```json
{"leader": "10.0.0.5:9080"}
```

### Capacity

```
GET /capacity
```

Returns detected system resources and node attributes:
```json
{
  "cpu_cores": 14,
  "memory_bytes": 51539607552,
  "attributes": {
    "node.id": "agent-1",
    "node.arch": "arm64",
    "node.os": "linux"
  }
}
```

### Tasks

```
GET /tasks                 # All tasks on this agent
```

### Run (internal, called by leader)

```
POST /run
```

Start a job. Returns 202 Accepted (fire-and-forget, artifact download + start happens async):
```json
{
  "status": "accepted",
  "job": "api",
  "message": "job accepted, starting in background"
}
```

Returns 406 if affinity mismatch. Returns 503 if no capacity.

### Delete (internal, called by leader)

```
DELETE /delete/{job_name}
```

Deletes a job and cleans up all its tasks on this agent.

### Logs (streaming)

```
GET /logs/{task_id}/stdout   # Stream stdout (SSE)
GET /logs/{task_id}/stderr   # Stream stderr (SSE)
```

Live stream of task output. Server-Sent Events (SSE) format.

**Example:**
```bash
curl http://agent:8080/logs/abc123/stdout

# SSE output:
data: [2025-01-31 12:00:00] Server starting...
data: [2025-01-31 12:00:01] Listening on port 8080
```

**No persistence** — logs are streamed live only.

### Proxy Endpoints

The agent proxies these paths to the current leader:
- `/v1/agents`
- `/v1/jobs`
- `/v1/jobs/{name}`
- `/v1/status`

This means easydns/easylb/easyprom can query their local agent and automatically get cluster-wide data.
