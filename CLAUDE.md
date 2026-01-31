# Easyrun

Lichtgewicht cluster orchestrator in Go. Simpel alternatief voor Nomad.

## Documentatie

Zie `/docs` voor gedetailleerde documentatie:

- `architecture.md` - Architectuur overzicht
- `data-structures.md` - Core data types en states
- `api.md` - HTTP API endpoints
- `cli.md` - CLI commando's
- `configuration.md` - Config file opties
- `development.md` - Build en development setup

**Belangrijk:** Houd de docs up-to-date bij wijzigingen aan de architectuur of API.

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

## Design Principes

- Simpliciteit boven features
- Één ProcessRunner met optionele limits (geen aparte runner types)
- State = `running`, `stopped`, `failed` (details in logs)
- Limits alleen als ze gezet zijn (`CPUShares > 0`, `MemoryLimit > 0`)
