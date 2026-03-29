# Architecture

```
┌─────────────────────────────────────────────────────┐
│                     HopRaft                        │
│              (leader election via HTTP)             │
└─────────────────────────────────────────────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
    ┌─────────┐      ┌─────────┐      ┌─────────┐
    │  Node 1 │      │  Node 2 │      │  Node 3 │
    │ (Agent) │      │ (Agent) │      │ (Agent) │
    │ :8080   │      │ + LEADER│      │ :8080   │
    └─────────┘      │ :8080   │      └─────────┘
         │           │ :9080   │           │
         │           └─────────┘           │
         │                │                │
         └────heartbeat───┼────heartbeat───┘
                          │
                    round robin
                    job dispatch
```

## Node with Leader Role (Shared State)

```
┌─────────────────────────────────────────────┐
│              Node 2 (leader node)           │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │              Agent                   │   │
│  │  ┌─────────────────────────────┐    │   │
│  │  │     jobs map (JobStore)     │◄───┼───┼─── shared!
│  │  │  - job-1: nginx             │    │   │
│  │  │  - job-2: redis             │    │   │
│  │  │  - job-3: api (op node 1)   │    │   │
│  │  └─────────────────────────────┘    │   │
│  │              ▲                       │   │
│  │              │ direct reference      │   │
│  │  ┌───────────┴───────────────┐      │   │
│  │  │         Leader            │      │   │
│  │  │  - agents map             │      │   │
│  │  │  - placed map             │      │   │
│  │  │  - dispatching map        │      │   │
│  │  │  - settled flag           │      │   │
│  │  │  - round robin state      │      │   │
│  │  └───────────────────────────┘      │   │
│  └─────────────────────────────────────┘   │
│                                             │
│    :8080 (agent API)                       │
│    :9080 (leader API)                      │
└─────────────────────────────────────────────┘
```

## Leader Failover (no bootstrap needed)

```
BEFORE:                         AFTER:
Node 2 = Leader                 Node 1 = New Leader

Node 1 (agent)                  Node 1 (agent + leader)
┌──────────────┐                ┌──────────────────────┐
│ jobs:        │                │ jobs: ◄──────────────┼─── SAME DATA!
│  - job-3     │   ────────►    │  - job-3             │
└──────────────┘                │                      │
                                │ Leader:              │
                                │  - uses jobs         │
                                │    directly          │
                                └──────────────────────┘

No sync needed! The agent BECOMES leader, not a separate entity.
```

## Job Sync via Heartbeat

```
Leader has ALL jobs (single source of truth)

Agent 1 ──heartbeat──► Leader
         {my jobs: [...], state_time: T}

         ◄────────────────────
         response: {all jobs: [...], state_time: T}

         Agent 1 saves new jobs via SyncJobs()

Each agent has a COPY of all jobs.
When an agent becomes leader, it already knows them!
```

## Registration Protocol

```
Agent startup or leader change:

Agent ──POST /v1/agents──► Leader
        {id, endpoint, version, placed: {jobID: count}}

        ◄──────────────────────
        {status: "registered", jobs: [...], state_time: T}

Subsequent heartbeats:

Agent ──POST /v1/heartbeat──► Leader
        {id, endpoint, version, jobs: [...], state_time: T}

        ◄────────────────────────
        {status: "ok", jobs: [...], state_time: T}

If leader returns 404: agent is unknown → re-register next tick
```

**Registration vs Heartbeat:**
- Registration (POST /v1/agents): includes `placed` counts, triggers reconciliation
- Heartbeat (POST /v1/heartbeat): updates LastSeen, syncs job state, no reconciliation

## Settle Period

```
Leader elected at T=0

T=0s:  Leader starts, settled=false
       Agents register with placed counts
       Jobs stored but NOT dispatched

T=30s: Settle delay expires, settled=true
       reconcileJobs() runs
       Compares desired vs actual (from placed counts)
       Only dispatches truly missing instances

Without settle:
  Leader doesn't know what agents are running → dispatches everything → duplicates!
```

## Components

### HopRaft
- Separate service for leader election
- Runs on 3+ nodes for HA
- Uses UDP for internal election (lowest IP wins)
- HTTP API for lease management

### Leader
- Node that has lease via HopRaft
- Receives heartbeats from agents
- Dispatches regular jobs via deterministic round-robin (agents sorted by ID)
- Dispatches daemon jobs (count=-1) via reconcile-based dispatch
- Tracks which job instances run on which agents (placed map: agentID → jobID → count)
- On agent failure: cleans stale placement, reconciles all jobs
- Runs on port+1000 (default 9080)

**Multi-instance Scheduling:**
- Job with Count=3 → dispatches 3x via deterministic round-robin (agents sorted by ID)
- Agent checks capacity (CPU/memory) before accepting
- On 503 (full) → leader tries next agent
- Automatic spreading over agents

**Affinity (agent-side):**
- Jobs can have `affinity` constraints (e.g., `{"node.arch": "arm64"}`)
- Leader dispatches to all agents — agent checks affinity and rejects with 406 if no match
- Leader stays unaware of node attributes (KISS)

**Daemon Scheduling (count=-1):**
- Uses reconcile-based dispatch (same code path as periodic reconciliation)
- Checks which agents already run the job, dispatches to missing agents
- Rebuilds placement atomically

**Reconciliation:**
- Triggered after agent death, new agent registration, or agent unregister
- Skips jobs that are actively being dispatched (prevents double dispatch)
- `reconcileJob` is the single function for both daemon and regular job reconciliation
- Daemon jobs: check all agents, dispatch to missing, rebuild placement atomically
- Regular jobs: sum placed counts across live agents, dispatch missing via round-robin

### Agent
- Runs on each node (including leader node)
- Sends heartbeat to leader every 10s
- First contact after startup or leader change: registers with placed counts
- Receives jobs from leader, starts processes
- On leader failure (3 ticks): try to become leader itself
- On isolation (6 ticks, no leader, can't become leader): stop all tasks
- Runs on port 8080
- CORS enabled for browser access
- Has node attributes (auto-detected: `node.id`, `node.arch`, `node.os`, `node.docker` + custom via config)
- Checks job affinity constraints before accepting — rejects with 406 on mismatch
- Resolves platform-specific artifacts: picks first artifact whose `match` constraints match node attributes

### Runner Selection
- Agent has both `ExecRunner` and `DockerRunner`
- `job.Driver` / `task.Driver` determines which runner is used (`"exec"` or `"docker"`)
- Driver is derived from `image` field if not set explicitly (`image != ""` → `"docker"`)
- All other systems (scheduling, health checks, service discovery) are runner-agnostic

### ExecRunner
- Starts processes with optional resource limits
- Each task gets its own directory with:
  - `tmp/` - temporary files
  - `resolv.conf` - DNS
  - Volume mounts (symlinked from host paths)
- CPU limiting via nice (if `CPUShares > 0`)
- Memory limiting:
  - Linux: cgroups v2
  - macOS: ulimit -v wrapper
- Optional isolation (chroot on Linux)
- Artifact download with extraction support (tar.gz, tar.bz2, zip, raw binary)

### DockerRunner
- Runs Docker containers via `docker` CLI (no SDK, no external dependencies)
- Container naming: `hop-<taskID>` for predictable lifecycle management
- Port mapping: `-p hostPort:containerPort` (host ports always dynamic)
- Resource limits: `--memory`, `--cpu-shares` (maps directly from Job fields)
- Volumes: `-v hostPath:containerPath`
- Environment: `-e KEY=VAL` + `ER_PORT_<NAME>` vars
- Logs: `docker logs -f` piped to LogBroadcaster (same SSE streaming as processes)
- Stop: `docker stop` (SIGTERM → 10s → SIGKILL) + `docker rm`
- Status: `docker inspect -f '{{.State.Running}}'`
- Cleanup at startup: `docker rm -f` all `hop-*` containers

## Named Ports

Jobs can request multiple named ports:

```json
{
  "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
  "ports": {"http": 0, "grpc": 0, "metrics": 9090}
}
```

**Per task:**
1. Agent allocates free port for each dynamic port (value=0)
2. Fixed ports (value>0) are used directly after availability check
3. Sets ENV vars: `ER_PORT_HTTP=8080`, `ER_PORT_GRPC=9090`, etc.
4. Sets ENV vars: `ER_ATTR_NODE_OS=linux`, `ER_ATTR_NODE_ARCH=arm64`, etc. for all node attributes
5. Task struct has `Ports map[string]int`

**No ports = no ports:**
- Jobs without `ports` field get empty ports map
- No default ports (KISS)
- Batch jobs / workers often don't need ports

## Volumes

Jobs can mount host directories:

```json
{
  "volumes": {
    "/data/shared": "data",
    "/etc/ssl/certs": "certs"
  }
}
```

- Host paths are validated (must exist)
- Target paths are relative to task directory
- Implemented via symlinks (platform-agnostic)
- Cleaned up (unmounted) on task stop

## Service Discovery via Tags

Jobs have `tags` field for external tooling:

```json
{
  "name": "api",
  "ports": {"http": 0},
  "tags": {
    "loadbalancer_domain": "*.example.com",
    "service": "api",
    "env": "production"
  }
}
```

Hop only stores tags - external tooling does the discovery logic.

## Health Checks

Three check types: HTTP, TCP, and file-based.

```json
// HTTP (default) — GET request, 200-399 = healthy
{"health_check": {"path": "/health", "port": "http"}}

// TCP — connect to port, success = healthy
{"health_check": {"type": "tcp", "port": "redis"}}

// FILE — check file mtime since last check, modified = healthy
{"health_check": {"type": "file", "path": "/tmp/worker-alive"}}
```

**Agent monitoring loop (5s):**
1. Check if process is still alive
2. If health_check configured:
   - `http`: HTTP GET to `http://127.0.0.1:{port}{path}`, status 200-399 = healthy
   - `tcp`: TCP connect to `127.0.0.1:{port}`, connection success = healthy
   - `file`: `os.Stat(path)`, file modified since last check = healthy
3. On failure: increment consecutive failure count
4. After `failure_threshold` (default 3) consecutive failures: kill + restart

**Initial timeout:** New tasks get `initial_timeout` (default 30s) grace period before health checks start.

**Failure threshold:** Prevents flapping from transient failures. Default 3 = task must fail 3 consecutive checks (15s with 5s monitor interval) before being marked unhealthy.

## Failure Scenarios

### Agent fails
1. Leader sees no heartbeat (30s timeout)
2. Leader marks agent as dead and cleans its placement entries
3. Leader reconciles all jobs: compares placed vs desired, dispatches missing
4. For daemon jobs: dispatches to all agents missing the job
5. For regular jobs: sums placed across live agents, dispatches the difference

### Leader fails
1. Agents get heartbeat timeout
2. After 3 failures: agents try to become leader via HopRaft
3. First to get lease becomes new leader with settle period (30s)
4. Agents re-register with placed counts (leader returns 404 → triggers re-registration)
5. After settle: reconciliation dispatches only truly missing instances

### Task fails (process crash)
1. Agent detects via monitor loop (5s)
2. Agent restarts task **locally** (same agent)
3. Max restart limit prevents infinite loops (default 5, -1 = unlimited)
4. On health check failure (after failure_threshold consecutive failures): kill + restart

### Agent isolated (network partition)
1. Agent can't reach leader
2. Agent can't become leader (no HopRaft quorum)
3. After 6 ticks (60s): agent stops all tasks
4. Prevents duplicate running tasks
