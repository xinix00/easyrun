# Development

## Build

```bash
# Binaries bouwen
go build -o bin/agent ./cmd/agent
go build -o bin/run ./cmd/cli

# Cross compile voor Linux
GOOS=linux GOARCH=amd64 go build -o bin/agent-linux ./cmd/agent
```

## Test Lokaal

1. Start agent (standalone = in-memory lock backend, geen extra services):
```bash
./bin/agent --cluster=dev --standalone
# of met config:
./bin/agent --config ./dev-config.yaml
```

Multi-node lokaal? Start hoplockserver en geef elke agent `--lock`:
```bash
../bin/hoplockserver -listen :8090 -data ./data/lock   # vanuit de monorepo
./bin/agent --cluster=dev --lock http://127.0.0.1:8090 --config dev-node1.yaml
```

2. Test CLI:
```bash
./bin/run --leader localhost:9080 status
./bin/run --leader localhost:9080 apply --name test --command "sleep 60"
./bin/run --leader localhost:9080 delete test
./bin/run --leader localhost:9080 agents
./bin/run --leader localhost:9080 logs <task-id>
```

## Project Structuur

```
/cmd
    /agent/main.go         # Agent + leader binary (version injected at build time)
    /cli/main.go           # CLI tool
/internal
    /agent/
        agent.go           # Agent state loop, JobStore, state persistence
        handlers.go        # Agent HTTP handlers, proxy to leader, CORS
        monitor.go         # Task monitoring (health checks, restarts)
        sysinfo.go         # System info interface
        sysinfo_darwin.go  # macOS: CPU/memory detection
        sysinfo_linux.go   # Linux: CPU/memory detection
    /leader/
        leader.go          # State management, heartbeat, settle period, registration
        dispatch.go        # Job dispatch, round-robin, agent pinning, placement, delete
        health.go          # Reconciliation (reconcileJob/reconcileJobs), dead agent check, cluster status
        update.go          # Update policies: rolling, recreate, blue-green
    /leader/
        events.go          # EventBus for SSE notifications
    /runner/
        runner.go          # Runner interface + Config + ENV var helpers
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
        discovery.go       # hoplock backends (hoplockserver / S3 / mem) + leader discovery
    /types/
        types.go           # Core data types (Job, Task, Agent, etc.)
/pkg
    /config/
        config.go          # Configuratie laden
    /httputil/
        auth.go            # HMAC request auth (X-Hop-Auth): RequireHMAC + SignRequest
        response.go        # JSON response helpers
/docs                      # Documentatie
```

Daarnaast: `internal/leader/persist.go` (committed cluster state naar S3) en
`internal/runner/hopos.go` (HopRunner voor HopOS-nodes, driver=`hop`).

## Poorten

| Service | Poort | Beschrijving |
|---------|-------|--------------|
| Agent | 8080 | Agent HTTP API |
| Leader | 9080 | Leader HTTP API (port+1000) |
| hoplockserver | 8090 | CAS lease store (default lock backend) |

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
- `github.com/xinix00/hoplock` — lease-based leader election (CAS over blob store)
- `github.com/xinix00/hoplockserver` — client for the hoplockserver backend
- `gopkg.in/yaml.v3` — YAML config parsing (only in pkg/config)

All core logic uses Go stdlib only (the CLI uses stdlib `flag`).
