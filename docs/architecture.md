# Architecture

```
┌─────────────────────────────────────────────────────┐
│                     EasyRaft                        │
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
│  │  │  - placement map          │      │   │
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
         {my jobs: [job-3]}

         ◄────────────────────
         response: {all jobs: [job-1, job-2, job-3]}

         Agent 1 saves job-1, job-2 via SyncJobs()


Each agent has a COPY of all jobs.
When an agent becomes leader, it already knows them!
```

## Leader Failover

```
BEFORE:                            AFTER:
Leader on Node 2                   Leader on Node 1

Node 1 (agent)                     Node 1 (agent + leader)
┌────────────────────┐             ┌────────────────────┐
│ jobs (via sync):   │             │ jobs:              │
│  - job-1           │  ────────►  │  - job-1           │
│  - job-2           │             │  - job-2           │
│  - job-3           │             │  - job-3           │
└────────────────────┘             │                    │
                                   │ Leader uses        │
Already knows ALL jobs!            │ same jobs map      │
                                   └────────────────────┘

No bootstrap, no recovery delay. Ready immediately.
```

## Components

### EasyRaft
- Separate service for leader election
- Runs on 3+ nodes for HA
- Uses UDP for internal election (lowest IP wins)
- HTTP API for lease management

### Leader
- Node that has lease via EasyRaft
- Receives heartbeats from agents
- Dispatches regular jobs via deterministic round-robin (agents sorted by ID)
- Dispatches daemon jobs (count=-1) via reconcile-based dispatch
- Tracks which job instances run on which agents (placement map)
- On agent failure: cleans stale placement, reconciles all jobs
- Runs on port+1000 (default 9080)

**Multi-instance Scheduling:**
- Job with Count=3 → dispatches 3x via deterministic round-robin (agents sorted by ID)
- Agent checks capacity (CPU/memory) before accepting
- On 503 (full) → leader tries next agent
- Automatic spreading over agents

**Daemon Scheduling (count=-1):**
- Uses reconcile-based dispatch (same code path as periodic reconciliation)
- Checks which agents already run the job, dispatches to missing agents
- Rebuilds placement atomically

**Reconciliation:**
- Triggered after agent death, new agent registration, or agent unregister
- `reconcileJob` is the single function for both daemon and regular job reconciliation
- Daemon jobs: check all agents, dispatch to missing, rebuild placement atomically
- Regular jobs: count running instances, dispatch missing via round-robin

### Agent
- Runs on each node (including leader node)
- Sends heartbeat to leader every 10s
- Receives jobs from leader, starts processes
- On leader failure: try to become leader itself
- On isolation (no leader, can't become leader): stop all tasks
- Runs on port 8080

### ProcessRunner
- Starts processes with optional resource limits
- Each task gets its own directory with:
  - `app/` - application files
  - `tmp/` - temporary files
  - `resolv.conf` - DNS
- CPU limiting via nice (if `CPUShares > 0`)
- Memory limiting:
  - Linux: cgroups v2
  - macOS: ulimit -v wrapper
- Optional chroot isolation

## Named Ports

Jobs can request multiple named ports:

```json
{
  "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
  "ports": ["http", "grpc", "metrics"]
}
```

**Per task:**
1. Agent allocates free port for each named port
2. Sets ENV vars: `ER_PORT_HTTP=8080`, `ER_PORT_GRPC=9090`, etc.
3. Task struct has `Ports map[string]int`

**No ports = no ports:**
- Jobs without `ports` field get empty ports map
- No default ports (KISS)
- Batch jobs / workers often don't need ports

## Service Discovery via Tags

Jobs have `tags` field for external tooling:

```json
{
  "name": "api",
  "ports": ["http"],
  "tags": {
    "loadbalancer_domain": "*.example.com",
    "service": "api",
    "env": "production"
  }
}
```

**External load balancer:**
```bash
curl http://leader:9080/v1/status | jq '.tasks_by_agent'
# Parse tasks with tag loadbalancer_domain
# Generate Nginx/Caddy upstream config
```

Easyrun only stores tags - external tooling does the discovery logic.

## Health Checks

```json
{
  "health_check": {
    "path": "/health",
    "port": "http",
    "interval": "10s",
    "timeout": "5s"
  }
}
```

**Agent monitoring loop (5s):**
1. Check if process is still alive
2. If health_check: HTTP GET to `http://localhost:{port}{path}`
3. On failure: kill + restart (max_restarts limit)

**Named port support:** Health check uses `port` field for which port to check.

## Failure Scenarios

### Agent fails
1. Leader sees no heartbeat (30s timeout)
2. Leader marks agent as dead and cleans its placement entries (`cleanPlacementForAgent`)
3. Leader reconciles all jobs: queries actual cluster state, dispatches missing instances
4. For daemon jobs: dispatches to all agents missing the job
5. For regular jobs: counts running instances, dispatches the difference

**Example:** Job with Count=5 on [A,B,B,C,C]. Agent B fails:
- Leader cleans B from all placement entries
- Reconciliation sees 3 running (on A,C,C), dispatches 2 more via round-robin
- Result: Job now runs on [A,C,C,D,E] (still 5 instances)

**Delete robustness:** `DeleteJobByID` uses two-phase approach:
1. Read + clear placement entries (atomic)
2. Check `GetClusterStatus()` for orphaned tasks not in placement
3. Send delete to the union of both sets (catches stale placement)

### Leader fails
1. Agents get heartbeat timeout
2. After 3 failures: agents try to become leader via EasyRaft
3. First to get lease becomes new leader
4. Other agents send heartbeat to new leader

### Task fails (process crash)
1. Agent detects via monitor loop (5s)
2. Agent restarts task **locally** (same agent)
3. Max restart limit prevents infinite loops
4. On health check failure: kill + restart

**Local restart is faster and preserves locality.**

### Agent isolated (network partition)
1. Agent can't reach leader
2. Agent can't become leader (no EasyRaft quorum)
3. After 6 ticks (60s): agent stops all tasks
4. Prevents duplicate running tasks
