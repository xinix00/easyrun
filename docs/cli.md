# CLI

## Configuratie

```bash
export EASYRUN_LEADER=localhost:9080
# of
./bin/run --leader localhost:9080 ...
```

## Cluster Status

```bash
./bin/run status
```

Output:
```
Leader:  localhost:9080
Agents:  3
Tasks:   5 running / 5 total

NAME    RUNNING   STATUS
web     3 / 3     OK
api     2 / 3     DEGRADED

AGENT     TASK      JOB    PORTS              STATE
agent-1   abc123    web    http:54321         running
agent-1   def456    web    http:54322         running
agent-2   ghi789    api    http:8080,grpc:9090 running
```

Shows jobs with expected vs running counts. Daemon jobs (count=-1) show `all(N)` where N = number of agents.

## Jobs

### Job starten of updaten (Upsert)

**Same command for deploy and update** — detects automatically based on job name:

```bash
# Deploy process job
./bin/run deploy\
    --name api \
    --command "./api-binary" \
    --cpu 2000 \
    --memory 512M \
    --env "LOG_LEVEL=info"

# Output (INSERT):
# Job 'api' dispatched with ID abc123

# Update to new version (rolling by default)
./bin/run deploy\
    --name api \
    --command "./api-binary-v2"

# Output (UPDATE):
# Job 'api' updated (ID def456, policy=rolling)

# Deploy Docker container (only on nodes with Docker)
./bin/run deploy--name redis --image redis:7 --affinity node.docker=true
./bin/run deploy--name my-app --image myapp:v2 --command "python serve.py"

# Deploy with affinity (only on arm64 nodes)
./bin/run deploy--name api --command "./api" --affinity node.arch=arm64

# Pin to specific node
./bin/run deploy--name monitor --command "./monitor" --affinity node.id=node-1

# Platform-specific artifacts (agent picks first matching)
./bin/run deploy--name tailscale --command "./tailscale" \
  --artifact "node.arch=amd64::https://pkgs.tailscale.com/stable/tailscale_amd64.tar.gz" \
  --artifact "node.arch=arm64::https://pkgs.tailscale.com/stable/tailscale_arm64.tar.gz"

# Simple artifact (no match = catch-all)
./bin/run deploy--name app --command "./app" --artifact "https://example.com/app.tar.gz"
```

### Deploy Flags

| Flag | Description |
|------|-------------|
| `--name` | Job name (required, unique key for upsert) |
| `--command` | Command to execute (required for process jobs, optional for Docker) |
| `--image` | Docker image (uses Docker instead of process) |
| `--artifact` | Artifact URL (repeatable, with optional match: `key=val::URL`) |
| `--cpu` | CPU shares |
| `--memory` | Memory limit (e.g., 512M, 1G) |
| `--env` | Environment variables (KEY=VALUE, repeatable) |
| `--affinity` | Node affinity constraints (key=value, repeatable, e.g. `node.arch=arm64`) |
| `--update-policy` | Update policy: rolling (default), recreate, or blue-green |

Either `--command` or `--image` (or both) is required.

### Update Policies

Control how updates are rolled out:

```bash
# Rolling update (default) - zero downtime
./bin/run deploy--name api --command "./v2" --update-policy rolling

# Recreate - downtime but fast
./bin/run deploy--name api --command "./v2" --update-policy recreate

# Blue-green - zero downtime, 2x resources during switch
./bin/run deploy--name api --command "./v2" --update-policy blue-green
```

| Policy | Downtime | Resources | Behavior |
|--------|----------|-----------|----------|
| `rolling` (default) | None | Normal | Start new → stop old, 1 at a time, 2s delay |
| `recreate` | Yes | Minimal | Stop all → start new version |
| `blue-green` | None | 2x during switch | Start all new → stop all old |

### Job verwijderen

```bash
./bin/run delete <job-name>
```

Verwijdert de job en stopt alle bijbehorende tasks.

## Agents

```bash
# List all agents
./bin/run agents

# Show agent details (capacity, resource usage)
./bin/run agents <agent-id>
```

List output:
```
ID        ENDPOINT              LAST SEEN
agent-1   http://10.0.0.1:8080  15:04:05
agent-2   http://10.0.0.2:8080  15:04:03
agent-3   http://10.0.0.3:8080  15:04:07
```

Detail output:
```
Agent:    agent-1
Endpoint: http://10.0.0.1:8080
LastSeen: 15:04:05

Tasks:    3 running
CPU:      2.0 / 14 cores (2048 / 14336 shares)
Memory:   0.5 / 48.0 GB

Attributes:
  node.arch = arm64
  node.docker = true
  node.id = agent-1
  node.os = linux
```

## Logs

Stream task logs in real-time:

```bash
# Stream stdout (default)
./bin/run logs <task-id>

# Stream stderr
./bin/run logs <task-id> --stream stderr
```

Finds the agent running the task automatically via cluster status.
