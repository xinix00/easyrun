# Configuratie

## Config File

```yaml
node:
  id: ""                    # Auto-generate UUID als leeg
  ip: ""                    # Auto-detect als leeg (advertised IP voor cluster comms)
  network: ""               # Optioneel CIDR (bv. "10.0.0.0/24") — als `ip` leeg is
                            # pakt hop het eerste interface-IP binnen deze range.
                            # Handig voor multi-homed nodes: HTTP luistert nog
                            # steeds op 0.0.0.0, alleen de advertised IP wordt
                            # vastgepind op de LAN/VPN.
  port: 8080                # Agent port (leader draait op port+1000)
  attributes:               # Custom node attributes (merged with auto-detected)
    # region: eu-west-1
    # gpu: "true"

cluster:
  name: "my-cluster"
  lock:                     # Leader election backend (hoplock)
    type: "hoplockserver"   # "hoplockserver" (default), "s3", of "mem"
    url: "http://10.0.0.1:8090"   # hoplockserver base URL (type=hoplockserver)
    api_key: ""             # hoplockserver API key (aparte key, optioneel)
    # key: ""               # lease object key (default: clusters/<name>/lease.json)
    # s3:                   # type=s3: AWS / Cloudflare R2 / MinIO / B2
    #   endpoint: "https://s3.eu-west-1.amazonaws.com"
    #   bucket: "hop-cluster"
    #   region: "eu-west-1"
    #   access_key_id: "..."
    #   secret_access_key: "..."
    #   use_path_style: false
  # init_jobs:              # Baseline die een leeg cluster bij clean boot krijgt
  #   - name: hopdns        # (zie "Init jobs" hieronder)
  #     command: /usr/local/bin/hopdns
  #     count: -1

api_key: ""                 # Gedeelde secret voor HMAC request-auth (X-Hop-Auth)
                            # over alle hop endpoints. Leeg = auth uit (dev).

capacity:
  cpu_shares: 14000         # Relatieve CPU capaciteit
  memory: 8589934592        # 8GB in bytes

paths:
  state_file: "/var/lib/hop/state.json"
  rootfs_base: "/var/lib/hop/rootfs"
  artifacts: "/var/lib/hop/artifacts"
  cache: "/var/lib/hop/cache"

runner:
  isolate: true             # Enable process isolation (chroot on Linux)

timeouts:
  health_check_interval: 10s
  health_check_timeout: 5s
  node_dead_threshold: 30s
  leader_lease: 30s
```

## Init jobs

`cluster.init_jobs` is de baseline die een **leeg** cluster automatisch krijgt.
Een leader die start zonder committed snapshot én zonder lokale jobs (een
"clean boot") seedt deze jobs eenmalig via het normale upsert-pad — alsof een
operator ze met `run apply` indiende. Zo komt een kale node (Pi, HopOS) uit de
doos met zijn taken, zonder dat iemand iets hoeft te deployen.

```yaml
cluster:
  name: "my-cluster"
  init_jobs:
    - name: hopdns
      command: /usr/local/bin/hopdns
      count: -1               # op elke node
      ports:
        dns: 5353
    - name: my-app
      image: myapp:v1
      count: 2
```

**Semantiek:**

- Veldnamen zijn het **job JSON-schema** (zelfde als `POST /v1/jobs` /
  [data-structures.md](data-structures.md)) — een spec is copy-pastbaar
  tussen config en API.
- **Alleen bij clean boot**: geen snapshot in de state store (of geen store
  geconfigureerd) én een lege job store. Init jobs zijn géén continue
  enforcement — een geseedde job verwijderen blijft verwijderd tot de
  volgende clean boot (deletion is absence).
- **Storing ≠ leeg**: is de state store onbereikbaar, dan wordt er nooit
  geseed — anders zou een S3-storing het cluster naar de baseline resetten.
- Een bestaande jobnaam wordt overgeslagen; een seed overschrijft nooit
  operator-state.
- Typo's zijn boot-fouten: onbekende velden, een ontbrekende `name` of een
  job zonder `command`/`image` stoppen de agent bij het starten.
- **Factory reset**: verwijder het `state/<cluster>`-object uit de bucket
  (en op de node z'n lokale state) → volgende leader-start is een clean
  boot → de baseline komt terug.

## CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to config file | (none, uses defaults) |
| `--node` | Node name/ID (overrides config) | (from config) |
| `--cluster` | Cluster name | (from config) |
| `--lock` | hoplockserver URL (overrides config) | (from config) |
| `--standalone` | Run without a lock backend (single-node, in-memory) | false |
| `--api-key` | Shared secret for HMAC request auth (X-Hop-Auth); overrides config | (from config) |

No lock backend configured → hop runs standalone automatically.

## Development Config

```yaml
# dev-config.yaml
node:
  id: "dev-node"
  ip: "127.0.0.1"
  port: 8080

cluster:
  name: "dev"
  # Geen lock config → standalone (in-memory).
  # Multi-node lokaal: lock: { url: "http://127.0.0.1:8090" } + hoplockserver starten.

capacity:
  cpu_shares: 14000
  memory: 8589934592

paths:
  state_file: "./data/state.json"
  rootfs_base: "./data/rootfs"
  artifacts: "./data/artifacts"
  cache: "./data/cache"

runner:
  isolate: false
```

**Note:** State file path is automatically adjusted per cluster name (`./data/state-{cluster}.json`) when using the default path.

## Resource Limiting

### CPU Shares

Relatieve waarde. Hoe meer shares, hoe hoger de prioriteit.

- `0` = geen limiting (default nice value)
- `1000` = lage prioriteit
- `14000` = hoogste prioriteit

Intern wordt dit vertaald naar nice values (0-19).

Capacity check: agents have `cpu_cores * 1024` total shares. Requests exceeding available shares are rejected (503).

### Memory Limit

In bytes.

- `0` = geen limiting
- `536870912` = 512MB
- `1073741824` = 1GB

Platform-specifieke implementatie:
- **Linux**: cgroups v2 (na process start, OOM killer integration)
- **macOS**: ulimit -v wrapper (voor exec)

Capacity check: agents check total system memory. Requests exceeding available memory are rejected (503).

## Process Isolation

```yaml
runner:
  isolate: true
```

Met isolation enabled:
- Elke task draait in een chroot jail (Linux)
- Minimale shell environment wordt automatisch gelinkt (`/bin/sh`, libraries)
- Command is relatief aan chroot root (bv. `/app/mybin`)

Zonder isolation (default in dev):
- Tasks draaien in eigen werkdirectory maar niet geïsoleerd

In beide modes:
- Command kan shell syntax gebruiken
- Memory limiting via ulimit (macOS) of cgroups (Linux)
- CPU limiting via nice
- Volume mounts via symlinks

**Default:** `isolate: true` (security by default in production)

## Auto-Detection

| Setting | Auto-Detected | Override |
|---------|---------------|----------|
| Node IP | Outbound interface | `node.ip` in config |
| Node Port | 8080 | `node.port` in config |
| Node ID | UUID (persisted in data/node-id) | `node.id` in config |
| Node Attributes | `node.id`, `node.arch`, `node.os`, `node.docker` | `node.attributes` in config (merges) |
| Capacity | System CPU/RAM | `capacity.*` in config |
| Paths | ./data/* | `paths.*` in config |
| Leader Port | node.port + 1000 | N/A |
| Timeouts | Smart defaults | `timeouts.*` in config |
| State File | ./data/state-{cluster}.json | `paths.state_file` in config |
