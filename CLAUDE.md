# Easyrun

Lightweight cluster orchestrator in Go. Simple alternative to Nomad.

## Documentation

See `/docs` for detailed documentation:

- `architecture.md` - Architecture overview (state loop, registration, settle period)
- `data-structures.md` - Core data types and states
- `api.md` - HTTP API endpoints
- `cli.md` - CLI commands
- `configuration.md` - Config file options
- `development.md` - Build and development setup

**Important:** Keep the docs up-to-date when making changes to the architecture or API.

## Quick Start

```bash
# Build
go build -o bin/agent ./cmd/agent
go build -o bin/easyrun ./cmd/cli

# Run standalone
./bin/agent --standalone --cluster=dev

# Deploy job
./bin/easyrun run --name test --command "echo hello"
```

## Design Principles

- Simplicity over features
- One ExecRunner with optional limits (no separate runner types)
- State = `running`, `stopped`, `failed` (details in logs)
- Limits only when set (`CPUShares > 0`, `MemoryLimit > 0`)
- Single goroutine owns mutable state via ops channel (`do()` and `query()` helpers)
- Registration protocol separate from heartbeat (placed counts for accurate state)
- Settle period on new leader before reconciliation

## Key Architecture Details

- **Agent port**: 8080 (configurable), **Leader port**: agent port + 1000
- **Heartbeat**: 10s interval, agents send jobs + state_time
- **Registration**: POST /v1/agents with placed counts (jobID -> count)
- **Heartbeat**: POST /v1/heartbeat, returns 404 for unknown agents (triggers re-register)
- **Settle period**: 30s after becoming leader, no reconciliation during this time
- **State persistence**: ./data/state-{cluster}.json (debounced save, 5s)
- **Node ID**: persisted in data/node-id, survives restarts
- **MaxRestarts**: 0 = unlimited
- **Version**: v0.5.8
