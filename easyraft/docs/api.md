# EasyRaft HTTP API

## Public Endpoints

### Health Check

```
GET /health
```

```json
{"status": "ok"}
```

### Cluster Status

```
GET /status
```

```json
{
  "term": 5,
  "leader": "https://raft1.example.com",
  "is_leader": true,
  "self": "https://raft1.example.com",
  "peers": 3
}
```

### Get Leader (voor cluster)

```
GET /leader/{cluster}
```

**Response (200):**
```json
{
  "cluster": "production",
  "leader": "10.0.0.5:9080",
  "expires": "2024-01-15T10:30:00Z"
}
```

**Response (404):** Geen leader
```json
{"error": "no leader"}
```

**Response (503):** Deze node is niet de raft leader
```json
{
  "error": "not leader",
  "leader": "https://raft2.example.com"
}
```

### Claim Leadership

```
POST /leader/{cluster}
```

```json
{
  "ip": "10.0.0.5:9080",
  "ttl_seconds": 30
}
```

**Response:**
```json
{
  "success": true,
  "leader": "10.0.0.5:9080",
  "expires": "2024-01-15T10:30:00Z"
}
```

### Release Leadership

```
DELETE /leader/{cluster}
```

```json
{"ip": "10.0.0.5:9080"}
```

**Response:**
```json
{"released": true}
```

## Internal Endpoints (API Key Required)

Deze endpoints zijn voor raft-interne communicatie. 
Vereisen `X-API-Key` header.

### Vote Request

```
POST /raft/vote
X-API-Key: your-secret-key
```

```json
{
  "term": 5,
  "candidate": "https://raft1.example.com"
}
```

**Response:**
```json
{"granted": true}
```

### Heartbeat

```
POST /raft/heartbeat
X-API-Key: your-secret-key
```

```json
{
  "term": 5,
  "leader": "https://raft1.example.com"
}
```

**Response:**
```json
{"status": "ok"}
```
