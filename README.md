# Easyrun

Lightweight cluster orchestrator in Go. Simpel alternatief voor Nomad.

## Features

- **Multi-instance jobs**: Deploy N copies met automatische spreading
- **Smart scheduling**: Round-robin met capacity-aware placement
- **Named ports**: Flexibele port allocation per service
- **Service discovery**: Tags voor externe load balancers
- **Health checks**: HTTP-based monitoring met auto-restart
- **Fault tolerance**: Automatic failover bij node crashes
- **Resource limits**: CPU shares en memory limiting
- **State persistence**: Jobs overleven agent restarts

## Quick Start

### Build

```bash
go build -o bin/agent ./cmd/agent
go build -o bin/orch ./cmd/cli
```

### Run Standalone

```bash
./bin/agent --standalone --config ./dev-config.yaml
```

### Deploy Job

```bash
# Single instance
./bin/orch job run --name web --command "python app.py"

# Multiple instances with spreading
./bin/orch job run --name api --command "./server" --count 3

# With ports and tags
./bin/orch job run \
  --name api \
  --command "./server --http=\$ER_PORT_HTTP --grpc=\$ER_PORT_GRPC" \
  --ports http,grpc \
  --tags service=api,env=prod \
  --count 3
```

## Architecture

```
┌─────────┐      ┌─────────┐      ┌─────────┐
│ Agent A │      │ Agent B │      │ Agent C │
│ (Leader)│◄────►│         │◄────►│         │
└─────────┘      └─────────┘      └─────────┘
     │                │                │
     └────────────────┴────────────────┘
              Heartbeats
```

- **Leader**: Dispatches jobs, monitors agent health
- **Agents**: Run tasks, report status, auto-restart failures
- **Round-robin**: Automatic spreading across nodes
- **Capacity-aware**: Agents reject when full, leader tries next

## Job Spec

```json
{
  "name": "api-service",
  "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
  "count": 3,
  "ports": ["http", "grpc", "metrics"],
  "cpu_shares": 2048,
  "memory_limit": 536870912,
  "env": {
    "DB_HOST": "postgres.internal"
  },
  "tags": {
    "service": "api",
    "loadbalancer_domain": "*.example.com"
  },
  "health_check": {
    "path": "/health",
    "port": "http",
    "interval": "10s",
    "timeout": "5s"
  },
  "max_restarts": 5
}
```

### Fields

- **name**: Job identifier
- **command**: Command to execute
- **count**: Number of instances (default: 1)
- **ports**: Named ports array - krijg ENV vars `ER_PORT_HTTP`, etc.
- **cpu_shares**: CPU priority (higher = more CPU time)
- **memory_limit**: Memory limit in bytes
- **env**: Environment variables
- **tags**: Labels for service discovery / grouping
- **health_check**: HTTP health check configuration
  - **port**: Named port to check (e.g., "http")
- **max_restarts**: Max restart attempts (0 = default 5, -1 = unlimited)

## Scheduling

### Multi-instance Spreading

```bash
# Deploy 3 instances
orch job run --name web --command "..." --count 3

# Automatic round-robin spreading:
# - Instance 1 → Agent A
# - Instance 2 → Agent B
# - Instance 3 → Agent C
```

### Capacity Checking

Agents check resources before accepting:
```
Leader dispatches → Agent A (full) → 503
                 → Agent B (space) → 200 ✓
```

## Named Ports

```json
{
  "command": "cloudflared --url http://localhost:$ER_PORT_HTTP --metrics :$ER_PORT_METRICS",
  "ports": ["http", "metrics"]
}
```

Task gets:
```bash
ER_PORT_HTTP=8080
ER_PORT_METRICS=9091
```

**No ports = no ports:** Jobs without `ports` field krijgen geen port ENV vars.

## Service Discovery via Tags

```json
{
  "name": "api",
  "ports": ["http"],
  "tags": {
    "loadbalancer_domain": "*.example.com",
    "service": "api"
  }
}
```

External tooling queries leader API:
```bash
curl http://leader:8080/v1/status | jq '.tasks_by_agent'
# Generate load balancer config based on tags
```

## Fault Tolerance

### Task Failures
- Agent detects crash
- Auto-restart lokaal (up to max_restarts)
- Health check failures → kill + restart

### Agent Failures
- Leader detects missing heartbeat (30s timeout)
- Redispatch alle jobs naar andere agents
- Count behouden: 3 instances blijven 3 instances

## Resource Limits

```json
{
  "cpu_shares": 2048,
  "memory_limit": 536870912
}
```

### CPU Shares
- Nice-based priority (Linux/macOS)
- Higher shares = lower nice = more CPU time

### Memory Limit
- **Linux**: cgroups v2
- **macOS**: ulimit

## Documentation

Zie `/docs` voor details:

- [architecture.md](docs/architecture.md) - System design
- [data-structures.md](docs/data-structures.md) - Core types
- [api.md](docs/api.md) - HTTP API reference
- [cli.md](docs/cli.md) - CLI commands
- [configuration.md](docs/configuration.md) - Config file options
- [development.md](docs/development.md) - Development setup

## Design Principles

- **Simpliciteit boven features**
- **KISS**: Keep It Simple, Stupid
- **Één ProcessRunner** - geen aparte runner types
- **States**: running, stopped, failed (details in logs)
- **Explicit > implicit**: Geen defaults, WYSIWYG

## License

MIT
