# EasyRaft Design

## Waarom HTTP ipv UDP?

| UDP | HTTP |
|-----|------|
| Alleen LAN (broadcast) | Werkt over internet |
| Firewall issues | Standaard poort 80/443 |
| Geen encryptie | HTTPS mogelijk |
| Complex peer discovery | Config met URLs |

## Deterministic Leader Election

Geen random election timeouts zoals echte Raft. In plaats daarvan:

1. Sorteer alle peer URLs alfabetisch
2. Laagste URL wordt leader
3. Geen split votes, geen election storms

```
Peers: [raft1.io, raft2.io, raft3.io]
        ↑
        Altijd leader (als online)
```

**Trade-off**: Dezelfde node is altijd leader (als ie online is). 
Acceptabel voor onze use case - we willen simpelheid boven load balancing.

## API Key Beveiliging

Interne raft communicatie (`/raft/vote`, `/raft/heartbeat`) is beveiligd:

```
POST /raft/heartbeat
X-API-Key: shared-secret-123
```

Dit voorkomt:
- Ongeautoriseerde nodes die zich voordoen als leader
- Externe partijen die elections verstoren

Lease endpoints (`/leader/*`) zijn **niet** beveiligd - EasyRun agents moeten deze kunnen aanroepen.

## Lease Management

Twee lagen van leadership:

1. **Raft layer**: Welke EasyRaft node is actief?
2. **Lease layer**: Welke EasyRun agent is leader?

```
EasyRaft cluster          EasyRun cluster
┌───────────────┐         ┌──────────────┐
│ raft1 = leader│◄────────│ agent1       │
│ raft2         │         │ agent2=leader│
│ raft3         │         │ agent3       │
└───────────────┘         └──────────────┘
        │                         │
        └─────── lease ───────────┘
          "production" -> agent2
```

## Failure Modes

### EasyRaft leader faalt

```
1. raft1 (leader) crashes
2. raft2, raft3 krijgen geen heartbeat (10s timeout)
3. raft2 heeft laagste URL → start election
4. raft2 vraagt vote aan raft3
5. raft3 stemt voor raft2 (want laagste URL)
6. raft2 wordt leader
7. EasyRun agents krijgen 503 van raft1, proberen raft2
```

### Network partition

```
[raft1] | [raft2, raft3]
   ↑    |      ↑
minority|  majority

- raft2 wordt leader (heeft quorum)
- raft1 kan geen leader worden (geen quorum)
- Clients die alleen raft1 kunnen bereiken krijgen 503
```

## Geen Persistence

Leases zijn in-memory:
- Bij restart: leases weg, EasyRun agents claimen opnieuw
- TTL is kort (30s), dus snel hersteld

Future: lease replicatie naar andere raft nodes.
