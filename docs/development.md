# Development

## Build

```bash
# Binaries bouwen
go build -o bin/agent ./cmd/agent
go build -o bin/easyrun ./cmd/cli

# Cross compile voor Linux
GOOS=linux GOARCH=amd64 go build -o bin/agent-linux ./cmd/agent
```

## Test Lokaal

1. Start EasyRaft (in easyraft/ folder):
```bash
cd easyraft
go run ./cmd/election -http-port 7080 -raft-port 7946
```

2. Start agent:
```bash
./bin/agent --config ./dev-config.yaml
```

3. Test CLI:
```bash
./bin/easyrun --leader localhost:9080 status
./bin/easyrun --leader localhost:9080 run --name test --command "sleep 60"
./bin/easyrun --leader localhost:9080 delete <job-name>
```

## Project Structuur

```
/cmd
    /agent/main.go         # Agent + leader binary
    /cli/main.go           # CLI tool
/internal
    /agent/
        agent.go           # Agent HTTP server + task management
        handlers.go        # Agent HTTP handlers
        monitor.go         # Task monitoring (health checks, restarts)
    /leader/
        leader.go          # State management, heartbeat, single-goroutine loop
        dispatch.go        # Job dispatch, round-robin, placement tracking, delete
        health.go          # Reconciliation (reconcileJob/reconcileJobs), dead agent check, cluster status
        update.go          # Update policies: rolling, recreate, blue-green
    /runner/
        runner.go          # Runner interface
        process.go         # Process runner
        process_linux.go   # Linux: cgroups
        process_darwin.go  # macOS: ulimit
        download.go        # Artifact download (HTTP, S3)
        logs.go            # Log broadcasting (SSE)
    /api/
        server.go          # Leader HTTP API
    /discovery/
        discovery.go       # EasyRaft client
    /types/
        types.go           # Core data types
/pkg
    /config/
        config.go          # Configuratie laden
    /httputil/
        response.go        # JSON response helpers
/docs                      # Documentatie
```

## Poorten

| Service | Poort | Beschrijving |
|---------|-------|--------------|
| Agent | 8080 | Agent HTTP API |
| Leader | 9080 | Leader HTTP API (port+1000) |
| EasyRaft HTTP | 7080 | EasyRaft HTTP API |
| EasyRaft UDP | 7946 | EasyRaft verkiezing |
