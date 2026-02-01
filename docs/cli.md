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

AGENT     TASK      JOB    PORT   STATE
agent-1   abc123    web    54321  running
agent-1   def456    web    54322  running
agent-2   ghi789    api    54323  running
```

## Jobs

### Job starten of updaten (Upsert)

**Same command for deploy and update** - detects automatically based on job name:

```bash
# Deploy initial version
./bin/easyrun run \
    --name api \
    --command "./api-binary" \
    --count 3 \
    --cpu 2000 \
    --memory 512M \
    --env "LOG_LEVEL=info"

# Output (INSERT):
# Job 'api' dispatched with ID abc123

# Update to new version (rolling by default)
./bin/easyrun run \
    --name api \
    --command "./api-binary-v2" \
    --count 3

# Output (UPDATE):
# Job 'api' updated (ID abc123, policy=rolling)
```

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
| `rolling` (default) | ❌ | Normal | Replace 1 instance at a time, 2s delay |
| `recreate` | ✅ | Minimal | Stop all → start new version |
| `blue-green` | ❌ | 2x during switch | Start new alongside old → switch after 5s |

### Run op alle nodes

Use `--count -1` to run on every agent:

```bash
./bin/easyrun run \
    --name easydns \
    --command "/usr/local/bin/easydns" \
    --count -1
```

New agents automatically receive the job on registration.

### Job stoppen

```bash
./bin/easyrun stop <job-id>
```

## Agents

```bash
./bin/easyrun agents
```

Output:
```
ID        ENDPOINT              LAST SEEN
agent-1   http://10.0.0.1:8080  15:04:05
agent-2   http://10.0.0.2:8080  15:04:03
agent-3   http://10.0.0.3:8080  15:04:07
```
