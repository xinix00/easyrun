# Easyrun

Lightweight cluster orchestrator in Go. Simple alternative to Nomad.

## Features

- **Multi-instance jobs**: Deploy N copies with automatic spreading
- **Smart scheduling**: Round-robin with capacity-aware placement
- **Agent pinning**: Pin jobs to specific nodes via `agent_id`
- **Job updates**: Rolling, recreate, or blue-green deployments
- **Named ports**: Flexible port allocation per service
- **Service discovery**: Tags for external load balancers
- **Health checks**: HTTP-based monitoring with auto-restart and initial grace period
- **Live log streaming**: Real-time stdout/stderr via SSE (no persistence)
- **Artifact downloads**: HTTP and S3 with extraction support (tar.gz, tar.bz2, zip, raw binary)
- **Volume mounts**: Host paths symlinked into task directories
- **Fault tolerance**: Automatic failover on node crashes with settle period
- **Resource limits**: CPU shares and memory limiting
- **State persistence**: Jobs survive agent restarts
- **Process isolation**: Optional chroot (Linux) / sandbox (macOS)

## Quick Start

### Build

```bash
go build -o bin/agent ./cmd/agent
go build -o bin/run ./cmd/cli
```

### Run Standalone

```bash
./bin/agent --standalone --cluster=dev
```

### Deploy Job

```bash
# Single instance
./bin/run deploy--name web --command "python app.py"

# Multiple instances with spreading
./bin/run deploy--name api --command "./server" --count 3

# With artifact and resource limits
./bin/run deploy\
  --name api \
  --command "./server" \
  --artifact "s3://bucket/app-v1.2.tar.gz" \
  --cpu 2000 \
  --memory 512M
```

### Update Job (Upsert)

**Same command** - POST with existing job name triggers update:

```bash
# Update to new version (rolling by default - zero downtime)
./bin/run deploy--name api --command "./server-v2"

# Update with specific policy
./bin/run deploy--name api --command "./server-v2" --update-policy recreate

# Blue-green deployment
./bin/run deploy--name api --command "./server-v2" --update-policy blue-green
```

#### Update Policies

| Policy | Downtime | Resources | Use Case |
|--------|----------|-----------|----------|
| **rolling** (default) | None | Normal | Standard updates, zero downtime |
| **recreate** | Yes | Minimal | Database migrations, breaking changes |
| **blue-green** | None | 2x during switch | Canary testing, instant rollback |

## Architecture

```
┌─────────┐      ┌─────────┐      ┌─────────┐
│ Agent A │      │ Agent B │      │ Agent C │
│ (Leader)│◄────►│         │◄────►│         │
└─────────┘      └─────────┘      └─────────┘
     │                │                │
     └────────────────┴────────────────┘
              Heartbeats (10s)
```

- **Leader**: Dispatches jobs, monitors agent health, reconciles on changes
- **Agents**: Run tasks, report status, auto-restart failures
- **Registration**: Agents register with `placed` counts on startup/leader change
- **Settle period**: New leader waits 30s before reconciling to let agents register
- **Deterministic round-robin**: Spreading across nodes (agents sorted by ID)
- **Capacity-aware**: Agents reject when full, leader tries next

## Job Spec

```json
{
  "name": "api-service",
  "command": "./server --http=$ER_PORT_HTTP",
  "count": 3,
  "agent_id": "",
  "artifact": {
    "url": "s3://bucket/app.tar.gz",
    "extract": "tar.gz",
    "auth": {"access_key": "...", "secret_key": "...", "region": "eu-west-1"}
  },
  "ports": {"http": 0, "grpc": 0},
  "cpu_shares": 2048,
  "memory_limit": 536870912,
  "env": {"DB_HOST": "postgres.internal"},
  "tags": {"service": "api", "urlprefix": "urlprefix:*.api.example.com"},
  "volumes": {"/data/shared": "data"},
  "health_check": {
    "path": "/health",
    "port": "http",
    "interval": "10s",
    "timeout": "5s",
    "initial_timeout": "30s"
  },
  "max_restarts": 0,
  "update_policy": "rolling"
}
```

### Fields

- **name**: Job identifier (unique key for upsert)
- **command**: Command to execute (shell syntax supported)
- **count**: Number of instances (default 1, -1 = all agents)
- **agent_id**: Pin to specific agent (optional)
- **artifact**: Binary/assets to download (optional)
  - **url**: Download URL - scheme determines downloader (http://, https://, s3://)
  - **extract**: Extraction format: `tar.gz`, `tar.bz2`, `zip`, or empty for raw binary (chmod +x)
  - **headers**: HTTP headers map (Authorization, X-API-Key, etc.)
  - **auth**: Credentials (S3: access_key/secret_key/region, HTTP: username/password for Basic Auth)
- **ports**: Port name -> fixed port (0 = dynamic) - generates ENV vars `ER_PORT_HTTP`, etc.
- **cpu_shares**: CPU priority (higher = more CPU time)
- **memory_limit**: Memory limit in bytes
- **env**: Environment variables
- **tags**: Labels for service discovery / grouping
- **volumes**: Host path -> task path mappings (symlinked into task directory)
- **health_check**: HTTP health check (optional)
  - **initial_timeout**: Grace period after start to become healthy (default 30s)
- **max_restarts**: Max restart attempts (0 = unlimited)
- **update_policy**: rolling (default), recreate, or blue-green

## Named Ports

```json
{
  "command": "./server --http=:$ER_PORT_HTTP --metrics=:$ER_PORT_METRICS",
  "ports": {"http": 0, "metrics": 0}
}
```

Task gets:
```bash
ER_PORT_HTTP=54321
ER_PORT_METRICS=54322
```

**No ports = no ports:** Jobs without `ports` field get no port ENV vars.

## Artifact Downloads

URL scheme determines downloader. `extract` field determines extraction method.

```json
{"artifact": {"url": "s3://bucket/app.tar.gz", "extract": "tar.gz"}}
{"artifact": {"url": "https://example.com/app.zip", "extract": "zip"}}
{"artifact": {"url": "https://example.com/binary", "extract": ""}}
```

Empty `extract` = raw file download with chmod +x (single binary).

## Volume Mounts

```json
{
  "volumes": {
    "/host/data": "data",
    "/host/config": "config"
  }
}
```

Host paths are symlinked into the task's working directory.

## Live Log Streaming

```bash
# Via CLI
./bin/run logs <task-id>                    # stdout
./bin/run logs <task-id> --stream stderr    # stderr

# Via API
curl http://agent:8080/logs/{task-id}/stdout
```

SSE format, live stream only, no storage. Pipe to external tools for persistence.

## Fault Tolerance

### Task Failures
- Agent detects crash
- Auto-restart locally (up to max_restarts, 0 = unlimited)
- Health check failures -> kill + restart

### Agent Failures
- Leader detects missing heartbeat (30s timeout)
- Cleans stale placement entries for dead agent
- Reconciles: compares desired vs actual, dispatches missing

### Leader Failover
- New leader enters settle period (30s)
- Agents register with placed counts during settle
- After settle, leader reconciles with accurate placement data

## Resource Limits

- **CPU shares**: Nice-based priority (higher shares = lower nice = more CPU time)
- **Memory limit**: Linux cgroups v2, macOS ulimit

Capacity-aware: agents have `cpu_cores * 1024` total shares and total system memory. Requests exceeding available resources are rejected (503).

## Documentation

See `/docs` for details:

- [architecture.md](docs/architecture.md) - System design
- [data-structures.md](docs/data-structures.md) - Core types
- [api.md](docs/api.md) - HTTP API reference
- [cli.md](docs/cli.md) - CLI commands
- [configuration.md](docs/configuration.md) - Config file options
- [development.md](docs/development.md) - Development setup

## Design Principles

- **Simplicity over features** - KISS
- **One ExecRunner** with optional limits
- **States**: running, stopped, failed
- **Polling over events** - 10s heartbeat, simple and robust
- **Channel-based state** - Single goroutine owns mutable state

## License

MIT
