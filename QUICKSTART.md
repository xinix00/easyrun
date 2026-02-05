# Quick Start

Get easyrun running in 30 seconds.

## macOS Development

```bash
# 1. Build
cd easyrun
go build -o ../bin/agent ./cmd/agent

# 2. Run (that's it!)
../bin/agent --cluster=easyflor-prod

# Output:
# Starting node a1b2c3d4 on 192.168.1.5:8080
# Cluster: easyflor-prod
# Using easyraft: [https://server-raft.easyflor.net:7080]
# Agent listening on 192.168.1.5:8080
```

**Everything auto-configured:**
- IP: Auto-detected from network interface
- Port: 8080 (agent), 9080 (leader if elected)
- Capacity: Detected from system (CPU cores, RAM)
- Paths: ./data/* for state/tasks/artifacts
- Raft: https://server-raft.easyflor.net:7080 (default)

## Production Linux

```bash
# With systemd
sudo tee /etc/systemd/system/easyrun.service <<EOF
[Unit]
Description=EasyRun Agent
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/agent --cluster=easyflor-prod
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable easyrun
sudo systemctl start easyrun
```

## Deploy a Job

```bash
# Build CLI
go build -o ../bin/orch ./cmd/cli

# Deploy
../bin/orch run --name nginx --command "nginx -g 'daemon off;'" --count 3

# Check status
../bin/orch status
```

## Standalone Mode (No Raft)

```bash
# Single-node dev/testing
../bin/agent --cluster=dev --standalone

# Agent becomes leader automatically
# No easyraft needed
```

## Advanced: Custom Config

Only needed for production with custom requirements:

```yaml
# /etc/easyrun/config.yaml
cluster:
  name: "easyflor-prod"

node:
  port: 80  # Custom port

paths:
  state_file: /var/lib/easyrun/state.json  # Persistent disk

runner:
  isolate: true  # Enable chroot (Linux only)
```

```bash
../bin/agent --config=/etc/easyrun/config.yaml
```

## Minimal Config Example

Most deployments only need cluster name:

```yaml
cluster:
  name: "easyflor-prod"
```

That's it! Everything else auto-configured.

## What Gets Auto-Detected

| Setting | Auto-Detected | Override |
|---------|---------------|----------|
| Node IP | ✅ Outbound interface | `--config` with `node.ip` |
| Node Port | ✅ 8080 | `node.port` in config |
| Capacity | ✅ System CPU/RAM | `capacity.*` in config |
| Paths | ✅ ./data/* | `paths.*` in config |
| Leader Port | ✅ node.port + 1000 | N/A |
| Timeouts | ✅ Smart defaults | `timeouts.*` in config |
| Raft Endpoint | ✅ https://server-raft.easyflor.net:7080 | `--raft` flag |

## Production Checklist

```bash
# 1. Deploy easyraft (3 nodes for HA)
# See easyraft/DOCKER.md

# 2. Start easyrun agents on all nodes
ssh node1 '/usr/local/bin/agent --cluster=easyflor-prod'
ssh node2 '/usr/local/bin/agent --cluster=easyflor-prod'
ssh node3 '/usr/local/bin/agent --cluster=easyflor-prod'

# 3. Deploy your apps
orch run --name api --command "./api" --count 3
orch run --name worker --command "./worker" --count 5

# Done! 🎉
```

## Troubleshooting

```bash
# Check if agent is running
curl http://localhost:8080/health

# Check if it's leader
curl http://localhost:9080/v1/agents
# (only works if this node is leader)

# View logs (standalone mode)
../bin/agent --cluster=dev --standalone
```

## Next Steps

- Read [docs/architecture.md](docs/architecture.md) to understand how it works
- See [BENCHMARKS.md](BENCHMARKS.md) for performance characteristics
- See [CHAOS.md](CHAOS.md) for failure handling
