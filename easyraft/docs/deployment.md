# EasyRaft Deployment

## Config File

```yaml
# /etc/easyraft/config.yaml

self: "https://raft1.example.com"

peers:
  - "https://raft1.example.com"
  - "https://raft2.example.com"
  - "https://raft3.example.com"

api_key: "your-secret-api-key-here"

port: 8080

heartbeat_interval_ms: 3000   # 3 seconden
election_timeout_ms: 10000    # 10 seconden
```

## Systemd Service

```ini
# /etc/systemd/system/easyraft.service

[Unit]
Description=EasyRaft Leader Election Service
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/easyraft -config /etc/easyraft/config.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Drie Nodes Opzetten

### Node 1 (raft1.example.com)

```yaml
self: "https://raft1.example.com"
peers:
  - "https://raft1.example.com"
  - "https://raft2.example.com"
  - "https://raft3.example.com"
api_key: "shared-secret-123"
port: 8080
```

### Node 2 (raft2.example.com)

```yaml
self: "https://raft2.example.com"
peers:
  - "https://raft1.example.com"
  - "https://raft2.example.com"
  - "https://raft3.example.com"
api_key: "shared-secret-123"
port: 8080
```

### Node 3 (raft3.example.com)

```yaml
self: "https://raft3.example.com"
peers:
  - "https://raft1.example.com"
  - "https://raft2.example.com"
  - "https://raft3.example.com"
api_key: "shared-secret-123"
port: 8080
```

## EasyRun Config

In je EasyRun agents, verwijs naar de raft endpoints:

```yaml
cluster:
  name: "production"
  raft_endpoints:
    - "https://raft1.example.com"
    - "https://raft2.example.com"
    - "https://raft3.example.com"
```

## Health Checks

```bash
# Check node health
curl https://raft1.example.com/health

# Check cluster status
curl https://raft1.example.com/status

# Check wie leader is voor een cluster
curl https://raft1.example.com/leader/production
```

## Monitoring

| Check | Alert wanneer |
|-------|---------------|
| `/health` | != 200 |
| `/status` `is_leader` | Geen leader in cluster voor > 15s |
| `/status` `peers` | Minder dan verwacht |

## Security

- **API Key**: Alle interne raft traffic (`/raft/*`) vereist de API key
- **HTTPS**: Gebruik HTTPS in productie (via reverse proxy of direct)
- **Firewall**: Alleen EasyRun agents hoeven `/leader/*` te bereiken
