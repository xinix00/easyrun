# CLI

## Configuratie

```bash
export EASYRUN_LEADER=localhost:9080
# of
./bin/easyrun --leader localhost:9080 ...
```

## Cluster Status

```bash
./bin/easyrun status
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
# Deploy initial version
./bin/easyrun run \
    --name api \
    --command "./api-binary" \
    --cpu 2000 \
    --memory 512M \
    --env "LOG_LEVEL=info"

# Output (INSERT):
# Job 'api' dispatched with ID abc123

# Update to new version (rolling by default)
./bin/easyrun run \
    --name api \
    --command "./api-binary-v2"

# Output (UPDATE):
# Job 'api' updated (ID def456, policy=rolling)
```

### Run Flags

| Flag | Description |
|------|-------------|
| `--name` | Job name (required, unique key for upsert) |
| `--command` | Command to execute (required) |
| `--artifact` | Artifact URL to download |
| `--cpu` | CPU shares |
| `--memory` | Memory limit (e.g., 512M, 1G) |
| `--env` | Environment variables (KEY=VALUE, repeatable) |
| `--update-policy` | Update policy: rolling (default), recreate, or blue-green |

### Update Policies

Control how updates are rolled out:

```bash
# Rolling update (default) - zero downtime
./bin/easyrun run --name api --command "./v2" --update-policy rolling

# Recreate - downtime but fast
./bin/easyrun run --name api --command "./v2" --update-policy recreate

# Blue-green - zero downtime, 2x resources during switch
./bin/easyrun run --name api --command "./v2" --update-policy blue-green
```

| Policy | Downtime | Resources | Behavior |
|--------|----------|-----------|----------|
| `rolling` (default) | None | Normal | Start new → stop old, 1 at a time, 2s delay |
| `recreate` | Yes | Minimal | Stop all → start new version |
| `blue-green` | None | 2x during switch | Start all new → stop all old |

### Job verwijderen

```bash
./bin/easyrun delete <job-name>
```

Verwijdert de job en stopt alle bijbehorende tasks.

## Agents

```bash
# List all agents
./bin/easyrun agents

# Show agent details (capacity, resource usage)
./bin/easyrun agents <agent-id>
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

CPU:      2.0 / 14 cores (2048 / 14336 shares)
Memory:   0.5 / 48.0 GB
```

## Logs

Stream task logs in real-time:

```bash
# Stream stdout (default)
./bin/easyrun logs <task-id>

# Stream stderr
./bin/easyrun logs <task-id> --stream stderr
```

Finds the agent running the task automatically via cluster status.
