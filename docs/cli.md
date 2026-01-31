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

### Job starten

```bash
./bin/easyrun run \
    --name api \
    --command "./api-binary" \
    --cpu 2000 \
    --memory 512M \
    --env "LOG_LEVEL=info"
```

Output:
```
Job 'api' dispatched with ID abc123
```

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
