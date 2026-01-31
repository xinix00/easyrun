# EasyRaft Architecture

EasyRaft is een lightweight leader election service via HTTP.

## Componenten

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   EasyRaft #1   │     │   EasyRaft #2   │     │   EasyRaft #3   │
│   raft1.io      │     │   raft2.io      │     │   raft3.io      │
│                 │     │                 │     │                 │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │    Raft     │◄──────►│    Raft     │◄──────►│    Raft     │ │
│ │   (HTTP)    │ │     │ │   (HTTP)    │ │     │ │   (HTTP)    │ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
│        │        │     │        │        │     │        │        │
│ ┌─────────────┐ │     │ ┌─────────────┐ │     │ ┌─────────────┐ │
│ │Lease Manager│ │     │ │Lease Manager│ │     │ │Lease Manager│ │
│ └─────────────┘ │     │ └─────────────┘ │     │ └─────────────┘ │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         ▲
         │ Lease requests
    ┌────┴────┐
    │ EasyRun │
    │ Agents  │
    └─────────┘
```

## Leader Election (Raft Layer)

Pure HTTP, geen UDP:

1. **Deterministic leader**: Node met laagste URL wordt leader
2. **Heartbeats**: Leader stuurt heartbeats naar alle peers (elke 3s)
3. **Election timeout**: Geen heartbeat voor 10s → start election
4. **Vote requests**: Kandidaat vraagt votes via HTTP POST
5. **API key**: Alle interne raft communicatie is beveiligd met API key

## Lease Layer

Clients (EasyRun agents) claimen leadership per cluster:

1. **Claim**: POST naar `/leader/{cluster}` met IP en TTL
2. **Renew**: Zelfde endpoint, zelfde IP verlengt lease
3. **Release**: DELETE naar `/leader/{cluster}`
4. **Query**: GET `/leader/{cluster}` geeft huidige leader

Alleen de actieve EasyRaft leader handelt lease requests af.

## Config

```yaml
self: "https://raft1.example.com"
peers:
  - "https://raft1.example.com"
  - "https://raft2.example.com"
  - "https://raft3.example.com"
api_key: "your-secret-key"
port: 8080
```

## Failure Scenarios

### EasyRaft node faalt
- Andere nodes detecteren geen heartbeat
- Node met laagste URL claimt leadership
- Leases blijven in-memory (TODO: replicatie)

### Network partition
- Nodes met quorum (2 van 3) kiezen leader
- Minderheid kan geen leader kiezen
- Clients krijgen 503 van minority nodes
