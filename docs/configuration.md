# Configuration

## Config File

```yaml
node:
  id: ""                    # Auto-generate a UUID when empty
  ip: ""                    # Auto-detect when empty (advertised IP for cluster comms)
  network: ""               # Optional CIDR (e.g. "10.0.0.0/24") — when `ip` is
                            # empty, hop takes the first interface IP inside
                            # this range. Handy on multi-homed nodes: HTTP still
                            # listens on 0.0.0.0, only the advertised IP is
                            # pinned to the LAN/VPN.
  port: 8080                # Agent port (the leader runs on port+1000)
  attributes:               # Custom node attributes (merged with auto-detected)
    # region: eu-west-1
    # gpu: "true"

cluster:
  name: "my-cluster"
  lock:                     # Leader election backend (hoplock)
    type: "hoplockserver"   # "hoplockserver" (default), "s3" or "mem"
    url: "http://10.0.0.1:8090"   # hoplockserver base URL (type=hoplockserver)
    api_key: ""             # hoplockserver API key (a separate key, optional)
    # key: ""               # lease object key (default: clusters/<name>/lease.json)
    # s3:                   # type=s3: AWS / Cloudflare R2 / MinIO / B2
    #   endpoint: "https://s3.eu-west-1.amazonaws.com"
    #   bucket: "hop-cluster"
    #   region: "eu-west-1"
    #   access_key_id: "..."
    #   secret_access_key: "..."
    #   use_path_style: false
  # init_jobs:              # Baseline an empty cluster gets on a clean boot
  #   - name: hopdns        # (see "Init jobs" below)
  #     command: /usr/local/bin/hopdns
  #     count: -1

api_key: ""                 # Shared secret for HMAC request auth (X-Hop-Auth)
                            # on every hop endpoint. Empty = auth off (dev).

capacity:
  cpu_shares: 14000         # Relative CPU capacity
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

`cluster.init_jobs` is the baseline an **empty** cluster gets by itself. A
leader that starts without a committed snapshot *and* without local jobs (a
"clean boot") seeds these jobs once through the normal upsert path — exactly as
if an operator had submitted them with `run apply`. That is how a blank node
(a Pi, a HopOS node) comes up with its work already on it, with nobody
deploying anything.

```yaml
cluster:
  name: "my-cluster"
  init_jobs:
    - name: hopdns
      command: /usr/local/bin/hopdns
      count: -1               # on every node
      ports:
        dns: 5353
    - name: my-app
      image: myapp:v1
      count: 2
```

**Semantics:**

- Field names are the **job JSON schema** (the same as `POST /v1/jobs` /
  [data-structures.md](data-structures.md)) — a spec is copy-pastable between
  config and API.
- **Clean boot only**: no snapshot in the state store (or no store configured)
  *and* an empty job store. Init jobs are not continuous enforcement — delete a
  seeded job and it stays deleted until the next clean boot (deletion is
  absence).
- **An outage is not an empty cluster**: if the state store is unreachable,
  nothing is ever seeded — otherwise an S3 outage would reset the cluster to
  the baseline.
- An existing job name is skipped; a seed never overwrites operator state.
- Typos are boot errors: unknown fields, a missing `name`, or a job without
  `command`/`image` stop the agent at startup.
- **Factory reset**: delete the `state/<cluster>` object from the bucket (and
  the node's local state) → the next leader start is a clean boot → the
  baseline comes back.

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
  # No lock config → standalone (in-memory).
  # Multi-node locally: lock: { url: "http://127.0.0.1:8090" } + run hoplockserver.

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

A relative value: more shares means higher priority.

- `0` = no limiting (default nice value)
- `1000` = low priority
- `14000` = highest priority

Internally this is translated to nice values (0-19).

Capacity check: agents have `cpu_cores * 1024` total shares. Requests exceeding available shares are rejected (503).

### Memory Limit

In bytes.

- `0` = no limiting
- `536870912` = 512MB
- `1073741824` = 1GB

Per-platform implementation:
- **Linux**: cgroups v2 (after process start, OOM killer integration)
- **macOS**: an `ulimit -v` wrapper (before exec)

Capacity check: agents check total system memory. Requests exceeding available memory are rejected (503).

## Process Isolation

```yaml
runner:
  isolate: true
```

With isolation enabled:
- Every task runs in a chroot jail (Linux)
- A minimal shell environment is linked in automatically (`/bin/sh`, libraries)
- The command is relative to the chroot root (e.g. `/app/mybin`)

Without isolation (the dev default):
- Tasks run in their own working directory, but are not isolated

In both modes:
- The command may use shell syntax
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
