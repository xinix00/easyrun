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
./bin/agent --cluster=dev --standalone
# of met config:
./bin/agent --config ./dev-config.yaml
```

3. Test CLI:
```bash
./bin/easyrun --leader localhost:9080 status
./bin/easyrun --leader localhost:9080 run --name test --command "sleep 60"
./bin/easyrun --leader localhost:9080 delete test
./bin/easyrun --leader localhost:9080 agents
./bin/easyrun --leader localhost:9080 logs <task-id>
```

## Project Structuur

```
/cmd
    /agent/main.go         # Agent + leader binary (v0.5.8)
    /cli/main.go           # CLI tool
/internal
    /agent/
        agent.go           # Agent state loop, JobStore, state persistence
        handlers.go        # Agent HTTP handlers, proxy to leader, CORS
        monitor.go         # Task monitoring (health checks, restarts, debounced save)
        sysinfo.go         # System info interface
        sysinfo_darwin.go  # macOS: CPU/memory detection
        sysinfo_linux.go   # Linux: CPU/memory detection
    /leader/
        leader.go          # State management, heartbeat, settle period, registration
        dispatch.go        # Job dispatch, round-robin, agent pinning, placement, delete
        health.go          # Reconciliation (reconcileJob/reconcileJobs), dead agent check, cluster status
        update.go          # Update policies: rolling, recreate, blue-green
    /runner/
        runner.go          # Runner interface + Config
        process.go         # Process runner (start, stop, status, limits, volumes)
        process_linux.go   # Linux: cgroups, chroot
        process_darwin.go  # macOS: ulimit, sandbox
        docker.go          # Docker runner (via docker CLI, no SDK)
        download.go        # Artifact download router + extraction (tar.gz, tar.bz2, zip)
        download_http.go   # HTTP/HTTPS downloader
        download_s3.go     # S3 downloader
        logs.go            # Log broadcasting (SSE)
    /api/
        server.go          # Leader HTTP API
    /discovery/
        discovery.go       # EasyRaft client
    /types/
        types.go           # Core data types (Job, Task, Agent, etc.)
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

## Tests

```bash
# Alle tests
go test ./...

# Met race detector
go test -race ./...

# Specifiek package
go test -v ./internal/leader
go test -v ./internal/agent
go test -v ./internal/api
go test -v ./internal/runner

# Chaos tests
go test -run=TestChaos -v ./internal/...

# Benchmarks
go test -bench=. -benchmem ./internal/...
```

## Dependencies

- `github.com/google/uuid` — UUID generation
- `github.com/urfave/cli/v2` — CLI framework (only in cmd/cli)
- `gopkg.in/yaml.v3` — YAML config parsing (only in pkg/config)

All core logic uses Go stdlib only.
