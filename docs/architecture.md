# Architectuur

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

## Node met Leader Rol (Shared State)

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

## Leader Failover (geen bootstrap nodig)

```
VOOR:                           NA:
Node 2 = Leader                 Node 1 = Nieuwe Leader
                                
Node 1 (agent)                  Node 1 (agent + leader)
┌──────────────┐                ┌──────────────────────┐
│ jobs:        │                │ jobs: ◄──────────────┼─── ZELFDE DATA!
│  - job-3     │   ────────►    │  - job-3             │
└──────────────┘                │                      │
                                │ Leader:              │
                                │  - gebruikt jobs     │
                                │    direct            │
                                └──────────────────────┘

Geen sync nodig! De agent WORDT leader, niet een aparte entiteit.
```

## Job Sync via Heartbeat

```
Leader heeft ALLE jobs (single source of truth)

Agent 1 ──heartbeat──► Leader
         {mijn jobs: [job-3]}
         
         ◄────────────────────
         response: {alle jobs: [job-1, job-2, job-3]}
         
         Agent 1 slaat job-1, job-2 op via SyncJobs()


Elke agent heeft dus een KOPIE van alle jobs.
Wanneer een agent leader wordt, kent hij ze al!
```

## Leader Failover

```
VOOR:                              NA:
Leader op Node 2                   Leader op Node 1

Node 1 (agent)                     Node 1 (agent + leader)
┌────────────────────┐             ┌────────────────────┐
│ jobs (via sync):   │             │ jobs:              │
│  - job-1           │  ────────►  │  - job-1           │
│  - job-2           │             │  - job-2           │
│  - job-3           │             │  - job-3           │
└────────────────────┘             │                    │
                                   │ Leader gebruikt    │
Kent al ALLE jobs!                 │ zelfde jobs map    │
                                   └────────────────────┘

Geen bootstrap, geen recovery delay. Direct klaar.
```

## Componenten

### EasyRaft
- Aparte service voor leader election
- Draait op 3+ nodes voor HA
- Gebruikt UDP voor interne verkiezing (lowest IP wins)
- HTTP API voor lease management

### Leader
- Node die lease heeft via EasyRaft
- Ontvangt heartbeats van agents
- Dispatcht jobs round-robin naar agents met capacity checking
- Houdt bij welke job instances op welke agents draaien
- Bij agent failure: redispatch alleen lost instances naar andere agents
- Draait op poort+1000 (default 9080)

**Multi-instance Scheduling:**
- Job met Count=3 → dispatcht 3x via round-robin
- Agent checks capaciteit (CPU/memory) voor accepteren
- Bij 503 (vol) → leader probeert next agent
- Automatic spreading over agents

### Agent
- Draait op elke node (inclusief leader node)
- Stuurt heartbeat naar leader elke 10s
- Ontvangt jobs van leader, start processen
- Bij leader failure: probeer zelf leader te worden
- Bij isolatie (geen leader, kan niet leader worden): stop alle tasks
- Draait op poort 8080

### ProcessRunner
- Start processen met optionele resource limits
- Elke task krijgt eigen directory met:
  - `app/` - applicatie bestanden
  - `tmp/` - tijdelijke bestanden
  - `resolv.conf` - DNS
- CPU limiting via nice (als `CPUShares > 0`)
- Memory limiting:
  - Linux: cgroups v2
  - macOS: ulimit -v wrapper
- Optionele chroot isolatie

## Named Ports

Jobs kunnen multiple named ports aanvragen:

```json
{
  "command": "./server --http=$ER_PORT_HTTP --grpc=$ER_PORT_GRPC",
  "ports": ["http", "grpc", "metrics"]
}
```

**Per task:**
1. Agent alloceert free port voor elke named port
2. Zet ENV vars: `ER_PORT_HTTP=8080`, `ER_PORT_GRPC=9090`, etc.
3. Task struct heeft `Ports map[string]int`

**Geen ports = geen ports:**
- Jobs zonder `ports` field krijgen lege ports map
- Geen default ports (KISS)
- Batch jobs / workers hebben vaak geen ports nodig

## Service Discovery via Tags

Jobs hebben `tags` field voor external tooling:

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
# Parse tasks met tag loadbalancer_domain
# Genereer Nginx/Caddy upstream config
```

Easyrun slaat alleen tags op - externe tooling doet de discovery logica.

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
1. Check of process nog leeft
2. Als health_check: HTTP GET naar `http://localhost:{port}{path}`
3. Bij failure: kill + restart (max_restarts limiet)

**Named port support:** Health check gebruikt `port` field voor welke port te checken.

## Failure Scenarios

### Agent faalt
1. Leader ziet geen heartbeat (30s timeout)
2. Leader markeert agent als dood
3. Leader telt hoeveel instances per job op failed agent draaiden
4. Leader redispatcht alleen die instances via round-robin

**Voorbeeld:** Job met Count=5 op [A,B,B,C,C]. Agent B faalt:
- Leader dispatcht 2 nieuwe instances (lost van B)
- Result: Job draait nu op [A,C,C,D,E] (nog steeds 5 instances)

### Leader faalt
1. Agents krijgen heartbeat timeout
2. Na 3 failures: agents proberen leader te worden via EasyRaft
3. Eerste die lease krijgt wordt nieuwe leader
4. Andere agents sturen heartbeat naar nieuwe leader

### Task faalt (process crash)
1. Agent detecteert via monitor loop (5s)
2. Agent restart task **lokaal** (same agent)
3. Max restart limiet voorkomt infinite loops
4. Bij health check failure: kill + restart

**Lokale restart is sneller en behoudt locality.**

### Agent geïsoleerd (network partition)
1. Agent kan leader niet bereiken
2. Agent kan niet leader worden (geen EasyRaft quorum)
3. Na 6 ticks (60s): agent stopt alle tasks
4. Voorkomt dubbel draaiende tasks
