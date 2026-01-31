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
  "tasks_by_agent": {
    "agent-1": [{"id": "abc", "job_name": "web", "state": "running", ...}],
    "agent-2": [...]
  }
}
```

### Agents

```
GET  /v1/agents            # All registered agents
DELETE /v1/agents/{id}     # Delete agent (redispatches jobs)
```

### Heartbeat

```
POST /v1/heartbeat
```

Agents send this every 10s to register/renew themselves:
```json
{
  "id": "agent-1",
  "endpoint": "http://10.0.0.5:8080"
}
```

### Jobs

```
POST   /v1/jobs            # Run job (fire & forget, round robin)
DELETE /v1/jobs/{id}       # Stop job
```

#### Run Job

**Simple example:**
```bash
curl -X POST http://localhost:9080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api",
    "command": "./my-binary",
    "cpu_shares": 2000,
    "memory_limit": 536870912
  }'
```

**With all features:**
```bash
curl -X POST http://localhost:9080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api",
    "artifact": {
      "url": "s3://mybucket/api-v2.0.tar.gz",
      "auth": {
        "access_key": "AKIAIOSFODNN7EXAMPLE",
        "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "region": "eu-west-1"
      }
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
      "loadbalancer_domain": "*.example.com",
      "env": "production"
    },
    "health_check": {
      "path": "/health",
      "port": "http",
      "interval": "10s",
      "timeout": "5s"
    },
    "max_restarts": 5
  }'
```

**Fields:**
- `artifact` (object): Binary/assets to download (optional)
  - `url` (string): URL with scheme - determines downloader (http://, s3://, file://)
  - `headers` (map): HTTP headers (Authorization, X-API-Key, etc.)
  - `auth` (map): Other credentials (S3: access_key/secret_key/region, HTTP helper: username/password)
- `count` (int): Number of instances (default 1)
- `ports` (map): Port name → fixed port (0 = dynamic). ENV vars `ER_PORT_<NAME>`
- `tags` (map): Labels for service discovery
- `health_check`: HTTP health monitoring
  - `port` (string): Named port to check (default "http")
- `max_restarts` (int): Max restart attempts (0=default 5, -1=unlimited)

**Artifact Downloaders:**

URL scheme → downloader:
- `http://`, `https://` → HTTP downloader
  - Uses `headers` for custom headers
  - Or `auth.username`/`auth.password` → generates Basic Auth header
- `s3://bucket/key` → S3 downloader
  - Uses `auth.access_key`, `auth.secret_key`, `auth.region`
- `file://path` → Local file copier

Response:
```json
{
  "id": "abc123",
  "status": "dispatched"
}
```

**Scheduling:**
- Count=3 → 3 instances via round-robin spreading
- Agent returns 503 if no capacity → leader tries next agent

## Agent API

Runs on each node. Default port: 8080.

### Health

```
GET /health
```

### Tasks

```
GET /tasks                 # All tasks on this agent
```

### Run (internal, called by leader)

```
POST /run
```

Start a job:
```json
{
  "id": "abc123",
  "name": "api",
  "command": "./my-binary"
}
```

### Stop (internal, called by leader)

```
DELETE /stop/{job_id}
```

Stops all tasks of a job on this agent.

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

**Usage with CLI:**
```bash
orch logs abc123                    # Stream stdout
orch logs abc123 --stream stderr    # Stream stderr
```

**No persistence** - logs are streamed live only. For permanent logging, pipe to external logger:
```bash
orch logs abc123 | tee /var/log/myapp.log
orch logs abc123 | ./log-forwarder --destination loki://...
```
