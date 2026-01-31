# Configuratie

## Config File

```yaml
node:
  id: ""                    # Auto-generate UUID als leeg
  ip: ""                    # Auto-detect als leeg
  port: 8080                # Agent port (leader draait op port+1000)

cluster:
  name: "my-cluster"
  raft_endpoints:           # EasyRaft endpoints
    - "http://10.0.0.1:8080"
    - "http://10.0.0.2:8080"
    - "http://10.0.0.3:8080"

capacity:
  cpu_shares: 14000         # Relatieve CPU capaciteit
  memory: 8589934592        # 8GB in bytes

paths:
  state_file: "/var/lib/easyrun/state.json"
  rootfs_base: "/var/lib/easyrun/rootfs"
  artifacts: "/var/lib/easyrun/artifacts"
  cache: "/var/lib/easyrun/cache"

runner:
  chroot: false             # Enable chroot isolation

timeouts:
  health_check_interval: 10s
  health_check_timeout: 5s
  node_dead_threshold: 30s
  leader_lease: 30s
```

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
    - "http://127.0.0.1:7080"  # Lokale easyraft instance

capacity:
  cpu_shares: 14000
  memory: 8589934592

paths:
  state_file: "./data/state.json"
  rootfs_base: "./data/rootfs"
  artifacts: "./data/artifacts"
  cache: "./data/cache"

runner:
  chroot: false
```

## Resource Limiting

### CPU Shares

Relatieve waarde. Hoe meer shares, hoe hoger de prioriteit.

- `0` = geen limiting (default nice value)
- `1000` = lage prioriteit
- `14000` = hoogste prioriteit

Intern wordt dit vertaald naar nice values (0-19).

### Memory Limit

In bytes.

- `0` = geen limiting
- `536870912` = 512MB
- `1073741824` = 1GB

Platform-specifieke implementatie:
- **Linux**: cgroups v2 (na process start, OOM killer integration)
- **macOS**: ulimit -v wrapper (voor exec)

## Chroot Isolation

```yaml
runner:
  chroot: true
```

Met chroot enabled:
- Elke task draait in een chroot jail
- Minimale shell environment wordt automatisch gelinkt (`/bin/sh`, libraries)
- Command is relatief aan chroot root (bv. `/app/mybin`)

Zonder chroot (default):
- Tasks draaien in eigen werkdirectory maar niet geïsoleerd

In beide modes:
- Command kan shell syntax gebruiken
- Memory limiting via ulimit (macOS) of cgroups (Linux)
- CPU limiting via nice
