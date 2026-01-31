# Data Structures

## Job

Wat de gebruiker wil draaien.

```go
type Job struct {
    ID          string            // Unieke identifier
    Name        string            // Menselijke naam
    ArtifactURL string            // Download URL voor binary/zip (optioneel)
    Command     string            // Uit te voeren command
    Count       int               // Aantal instances (default: 1)
    Ports       []string          // Named ports (["http", "grpc", "metrics"])
    CPUShares   int               // Relatieve CPU priority (0 = geen limiting)
    MemoryLimit uint64            // Bytes (0 = geen limiting)
    Env         map[string]string // Extra environment variables
    Tags        map[string]string // Labels voor service discovery/grouping
    HealthCheck *HealthCheck      // HTTP health check config (optioneel)
    MaxRestarts int               // Max restart pogingen (0=default 5, -1=unlimited)
}
```

### HealthCheck

```go
type HealthCheck struct {
    Path     string        // HTTP path (e.g., "/health")
    Port     string        // Named port (default "http")
    Interval time.Duration // Check interval (default 10s)
    Timeout  time.Duration // Request timeout (default 5s)
}
```

## Task

Een draaiende instance van een Job.

```go
type Task struct {
    ID           string         // Unieke identifier
    JobID        string         // Welke job dit is
    JobName      string         // Job naam (voor display)
    Ports        map[string]int // Named port -> port number
    Pid          int            // Process ID
    State        TaskState      // running, stopped, failed
    StartedAt    time.Time
    RestartCount int            // Aantal keer gerestart
}
```

**Named Ports:** Task krijgt ENV vars `ER_PORT_HTTP`, `ER_PORT_GRPC`, etc. voor alle ports in de map.

### Task States

| State | Betekenis |
|-------|-----------|
| `running` | Process draait |
| `stopped` | Bewust gestopt |
| `failed` | Crashed, OOM killed, etc |

## Agent

Een geregistreerde agent bij de leader.

```go
type Agent struct {
    ID       string    // Unieke identifier
    Endpoint string    // HTTP endpoint (http://ip:port)
    LastSeen time.Time // Laatste heartbeat
}
```

## Leader State (in-memory)

```go
type Leader struct {
    agents    map[string]*Agent     // Geregistreerde agents
    jobs      map[string]*Job       // Alle jobs (shared met agent)
    placement map[string][]string   // jobID -> []agentID (multiple instances)
}
```

De leader houdt bij:
- Welke agents online zijn (via heartbeats)
- Welke jobs er zijn
- Welke job instances op welke agents draaien

**Multi-instance support:** Een job met Count=3 heeft 3 entries in placement, verspreid over agents via round-robin.

Bij agent failure worden alleen de instances op die agent opnieuw gedispatcht.
