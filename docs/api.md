# HTTP API

Er zijn twee APIs: de **Leader API** (port+1000) en de **Agent API** (port).

## Leader API

Draait op de node die leader is (via easyraft). Default poort: 9080.

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
GET  /v1/agents            # Alle geregistreerde agents
DELETE /v1/agents/{id}     # Verwijder agent (redispatcht jobs)
```

### Heartbeat

```
POST /v1/heartbeat
```

Agents sturen dit elke 10s om zich te registreren/vernieuwen:
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

**Simpel voorbeeld:**
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

**Met alle features:**
```bash
curl -X POST http://localhost:9080/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api",
    "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
    "count": 3,
    "ports": ["http", "grpc", "metrics"],
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
- `count` (int): Number of instances (default 1)
- `ports` ([]string): Named ports → ENV vars `ER_PORT_<NAME>`
- `tags` (map): Labels voor service discovery
- `health_check`: HTTP health monitoring
  - `port` (string): Named port to check (default "http")
- `max_restarts` (int): Max restart attempts (0=default 5, -1=unlimited)

Response:
```json
{
  "id": "abc123",
  "status": "dispatched"
}
```

**Scheduling:**
- Count=3 → 3 instances via round-robin spreading
- Agent returns 503 als geen capacity → leader probeert next agent

## Agent API

Draait op elke node. Default poort: 8080.

### Health

```
GET /health
```

### Tasks

```
GET /tasks                 # Alle tasks op deze agent
```

### Run (intern, aangeroepen door leader)

```
POST /run
```

Start een job:
```json
{
  "id": "abc123",
  "name": "api",
  "command": "./my-binary"
}
```

### Stop (intern, aangeroepen door leader)

```
DELETE /stop/{job_id}
```

Stopt alle tasks van een job op deze agent.
