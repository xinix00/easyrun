# Performance Benchmarks

Comprehensive performance tests for easyrun's critical components.

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./internal/...

# Run specific component
go test -bench=. -benchmem ./internal/leader
go test -bench=. -benchmem ./internal/agent
go test -bench=. -benchmem ./internal/api

# Run with CPU profiling
go test -bench=BenchmarkDispatchJob -cpuprofile=cpu.prof ./internal/leader
go tool pprof cpu.prof

# Run with memory profiling
go test -bench=BenchmarkGetJobs -memprofile=mem.prof ./internal/agent
go tool pprof mem.prof

# Compare before/after (save baseline first)
go test -bench=. -benchmem ./internal/leader > old.txt
# Make changes...
go test -bench=. -benchmem ./internal/leader > new.txt
benchstat old.txt new.txt
```

## Latest Results

Measured on Apple M4 Pro (14 cores, 48GB RAM), Go 1.24.3.

### Agent Benchmarks (`internal/agent/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| TaskCreation | 1,534,959 | 786 | 845 | 11 |
| GetJobs (1k) | 137,226 | 8,405 | 8,360 | 4 |
| SyncJobs (100) | 947,427 | 1,347 | 64 | 1 |
| CapacityCheck (50 tasks) | 120,933 | 10,283 | 168 | 3 |
| StateQuery | 3,534,181 | 337 | 144 | 2 |
| ConcurrentStateAccess | 1,830,178 | 664 | 184 | 5 |

### Leader Benchmarks (`internal/leader/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| FindJobByName (10k) | 15,805 | 74,228 | 81,996 | 3 |
| JobLookupScale/100 | 2,005,771 | 586 | 904 | 2 |
| JobLookupScale/1k | 175,122 | 6,888 | 8,211 | 2 |
| JobLookupScale/10k | 15,748 | 75,821 | 81,996 | 3 |
| JobLookupScale/100k | 1,864 | 624,754 | 802,906 | 3 |

### API Benchmarks (`internal/api/benchmark_test.go`)

| Benchmark | ops/sec | ns/op | B/op | allocs/op |
|-----------|---------|-------|------|-----------|
| GetJobsEndpoint (1k) | 7,902 | 151,287 | 67,397 | 11 |
| JSONEncoding (100 jobs) | 45,487 | 25,761 | 17,113 | 102 |
| JSONDecoding | 990,346 | 1,346 | 1,168 | 20 |

**Note:** Some API benchmarks (DispatchJob, Heartbeat, Status) require mock agents with real HTTP endpoints and may time out in isolated test environments.

## Benchmark Descriptions

### Agent

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkTaskCreation` | Task creation and state tracking overhead |
| `BenchmarkGetJobs` | Job list retrieval with 1000 jobs |
| `BenchmarkSyncJobs` | Job synchronization overhead (100 jobs) |
| `BenchmarkCapacityCheck` | Capacity checking with 50 running tasks |
| `BenchmarkStateQuery` | State query via ops channel |
| `BenchmarkConcurrentStateAccess` | Concurrent state access (read-only) |

### Leader

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkDispatchJob` | Job dispatch throughput |
| `BenchmarkHeartbeat` | Heartbeat processing |
| `BenchmarkGetAgents` | Agent list retrieval with 1000 agents |
| `BenchmarkFindJobByName` | Job lookup with 10k jobs (O(n) scan) |
| `BenchmarkJobLookupScale` | Job lookup at different scales |
| `BenchmarkRoundRobinSelection` | Round-robin agent selection |
| `BenchmarkPlacementUpdate` | Placement tracking overhead |
| `BenchmarkConcurrentHeartbeats` | Concurrent heartbeat handling |

### API

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkGetAgentsEndpoint` | GET /v1/agents throughput |
| `BenchmarkGetJobsEndpoint` | GET /v1/jobs throughput (1k jobs) |
| `BenchmarkPostJobEndpoint` | POST /v1/jobs throughput |
| `BenchmarkHeartbeatEndpoint` | POST /v1/heartbeat throughput |
| `BenchmarkStatusEndpoint` | GET /v1/status throughput |
| `BenchmarkConcurrentRequests` | Concurrent API request handling |
| `BenchmarkJSONEncoding` | JSON serialization overhead |
| `BenchmarkJSONDecoding` | JSON deserialization overhead |

## Performance Targets

### Latency (p99)

| Operation | Target | Acceptable |
|-----------|--------|------------|
| Job dispatch | <10ms | <50ms |
| Heartbeat | <1ms | <5ms |
| API requests | <10ms | <50ms |
| State queries | <100us | <1ms |

### Throughput

| Component | Target | Scale |
|-----------|--------|-------|
| Leader | 10k jobs/sec | Horizontal (multiple clusters) |
| Agent | 1k tasks/node | Vertical (bigger nodes) |
| API | 50k req/sec | Horizontal (load balancer) |

## Profiling

### CPU Profile

```bash
go test -bench=BenchmarkDispatchJob -cpuprofile=cpu.prof -benchtime=30s ./internal/leader
go tool pprof -http=:8080 cpu.prof
```

### Memory Profile

```bash
go test -bench=BenchmarkTaskCreation -memprofile=mem.prof ./internal/agent
go tool pprof -alloc_space mem.prof
```

### Trace

```bash
go test -bench=BenchmarkConcurrentHeartbeats -trace=trace.out ./internal/leader
go tool trace trace.out
```

## Known Bottlenecks

1. **Job lookup by name**: O(n) linear scan — 586 ns at 100 jobs, 625 us at 100k jobs
   - Fix: Add name index to JobStore
   - Impact: High with >10k jobs

2. **Status endpoint**: Fetches from all agents
   - Impact: High latency with >100 agents

3. **JSON serialization**: High allocation rate (102 allocs for 100 jobs)
   - Fix: Use sync.Pool for buffers
   - Impact: Moderate at >10k req/sec

4. **State channel contention**: Single channel for all state ops
   - Fix: Shard state by job ID hash
   - Impact: High with >1M ops/sec
