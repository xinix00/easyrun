# Configuratie

## Config File

```yaml
node:
  id: ""                    # Auto-generate UUID als leeg
  ip: ""                    # Auto-detect als leeg
  port: 8080                # Agent port (leader draait op port+1000)
  attributes:               # Custom node attributes (merged with auto-detected)
    # region: eu-west-1
    # gpu: "true"

cluster:
  name: "my-cluster"
  raft_endpoints:           # HopRaft endpoints
    - "http://10.0.0.1:7080"
    - "http://10.0.0.2:7080"
    - "http://10.0.0.3:7080"

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

## CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to config file | (none, uses defaults) |
| `--cluster` | Cluster name | (from config) |
| `--raft` | HopRaft endpoint | (from config) |
| `--standalone` | Run without hopraft (single-node mode) | false |
| `--api-key` | API key for authentication (overrides config) | (from config) |

## Development Config

```yaml
# dev-config.yaml
node:
  id: "dev-node"
  ip: "127.0.0.1"
  port: 8080

cluster:
  name: "dev"
  raft_endpoints:
    - "http://127.0.0.1:7080"  # Lokale hopraft instance

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
