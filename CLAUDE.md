# Easyrun

Lightweight cluster orchestrator in Go. Simple alternative to Nomad.

## Documentation

See `/docs` for detailed documentation:

- `architecture.md` - Architecture overview
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
go build -o bin/orch ./cmd/cli

# Run standalone
./bin/agent --standalone --config ./dev-config.yaml

# Deploy job
./bin/orch job run --name test --command "echo hello"
```

## Design Principles

- Simplicity over features
- One ProcessRunner with optional limits (no separate runner types)
- State = `running`, `stopped`, `failed` (details in logs)
- Limits only when set (`CPUShares > 0`, `MemoryLimit > 0`)
