# Quick Start

Get easyrun running in 30 seconds.

## Build

```bash
cd easyrun
go build -o ../bin/agent ./cmd/agent
go build -o ../bin/run ./cmd/cli
```

## Standalone Mode (No Raft)

```bash
# Single-node dev/testing - agent becomes leader automatically
../bin/agent --cluster=dev --standalone

# Output:
# Starting easyrun agent v0.5.8
# Node a1b2c3d4 on 192.168.1.5:8080
# Cluster: dev
# Running in standalone mode (no raft)
# Became leader!
```

## Deploy a Job

```bash
export EASYRUN_LEADER=localhost:9080

# Deploy
../bin/run deploy--name nginx --command "nginx -g 'daemon off;'"

# Check status
../bin/run status

# View agents
../bin/run agents
```

## Multi-Node Cluster

```bash
# 1. Start EasyRaft (leader election)
# See easyraft/ for setup

# 2. Start agents on each node
../bin/agent --cluster=my-cluster --raft http://raft-server:7080

# 3. Deploy with spreading
../bin/run deploy--name api --command "./server" --count 3
```

## With Config File

Only needed for custom requirements:

```yaml
# config.yaml
cluster:
  name: "my-cluster"
  raft_endpoints:
    - "http://10.0.0.1:7080"

node:
  port: 8080

paths:
  state_file: /var/lib/easyrun/state.json

runner:
  isolate: true  # chroot on Linux, sandbox on macOS
```

```bash
../bin/agent --config=config.yaml
```

## Production (systemd)

```bash
sudo tee /etc/systemd/system/easyrun.service <<EOF
[Unit]
Description=EasyRun Agent
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/agent --cluster=my-cluster --raft http://raft-server:7080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable --now easyrun
```

## What Gets Auto-Detected

| Setting | Auto-Detected | Override |
|---------|---------------|----------|
| Node IP | Outbound interface | `node.ip` in config |
| Node Port | 8080 | `node.port` in config |
| Node ID | UUID (persisted in data/node-id) | `node.id` in config |
| Capacity | System CPU/RAM | `capacity.*` in config |
| Paths | ./data/* | `paths.*` in config |
| Leader Port | node.port + 1000 | N/A |
| Timeouts | Smart defaults | `timeouts.*` in config |
| State File | ./data/state-{cluster}.json | `paths.state_file` in config |

## Troubleshooting

```bash
# Check agent health
curl http://localhost:8080/health

# Check leader status
curl http://localhost:9080/v1/status

# View agent capacity
curl http://localhost:8080/capacity

# Who is leader?
curl http://localhost:8080/leader
```

## Next Steps

- Read [docs/architecture.md](docs/architecture.md) for system design
- See [docs/api.md](docs/api.md) for HTTP API reference
- See [BENCHMARKS.md](BENCHMARKS.md) for performance characteristics
- See [CHAOS.md](CHAOS.md) for failure handling
