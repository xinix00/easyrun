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

## Leader Benchmarks

Located in `internal/leader/benchmark_test.go`:

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkDispatchJob` | Job dispatch throughput (jobs/sec) |
| `BenchmarkHeartbeat` | Heartbeat processing (heartbeats/sec) |
| `BenchmarkGetAgents` | Agent list retrieval with 1000 agents |
| `BenchmarkFindJobByName` | Job lookup performance with 10k jobs |
| `BenchmarkRoundRobinSelection` | Round-robin agent selection overhead |
| `BenchmarkConcurrentHeartbeats` | Concurrent heartbeat handling |
| `BenchmarkPlacementUpdate` | Placement tracking overhead |

**Expected Performance:**
- Heartbeat processing: >100k ops/sec
- Job lookup: >1M ops/sec
- Agent selection: >10M ops/sec
- Concurrent heartbeats: scales linearly with cores

## Agent Benchmarks

Located in `internal/agent/benchmark_test.go`:

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkTaskCreation` | Task creation and state tracking overhead |
| `BenchmarkGetJobs` | Job list retrieval with 1000 jobs |
| `BenchmarkSyncJobs` | Job synchronization overhead (100 jobs) |
| `BenchmarkCapacityCheck` | Capacity checking with 50 running tasks |
| `BenchmarkStateQuery` | State query performance |
| `BenchmarkConcurrentStateAccess` | Concurrent state access (read-only) |

**Expected Performance:**
- Task creation: >50k ops/sec
- Job list retrieval: >100k ops/sec
- Capacity check: >500k ops/sec
- State query: >1M ops/sec
- Concurrent reads: scales with cores

## API Benchmarks

Located in `internal/api/benchmark_test.go`:

| Benchmark | What it measures |
|-----------|------------------|
| `BenchmarkGetAgentsEndpoint` | GET /v1/agents throughput (100 agents) |
| `BenchmarkGetJobsEndpoint` | GET /v1/jobs throughput (1000 jobs) |
| `BenchmarkPostJobEndpoint` | POST /v1/jobs throughput |
| `BenchmarkHeartbeatEndpoint` | POST /v1/heartbeat throughput |
| `BenchmarkStatusEndpoint` | GET /v1/status throughput |
| `BenchmarkConcurrentRequests` | Concurrent API request handling |
| `BenchmarkJSONEncoding` | JSON serialization overhead |
| `BenchmarkJSONDecoding` | JSON deserialization overhead |

**Expected Performance:**
- GET endpoints: >10k req/sec per core
- POST endpoints: >5k req/sec per core
- Concurrent requests: scales linearly
- JSON encoding: >100k ops/sec
- JSON decoding: >50k ops/sec

## Performance Targets

### Latency (p99)

| Operation | Target | Acceptable |
|-----------|--------|------------|
| Job dispatch | <10ms | <50ms |
| Heartbeat | <1ms | <5ms |
| API requests | <10ms | <50ms |
| State queries | <100µs | <1ms |

### Throughput

| Component | Target | Scale |
|-----------|--------|-------|
| Leader | 10k jobs/sec | Horizontal (add leaders) |
| Agent | 1k tasks/node | Vertical (bigger nodes) |
| API | 50k req/sec | Horizontal (load balancer) |

## Optimization Tips

### Leader Performance

1. **High job churn**: Increase `stateChannelBufferSize` in leader.go
2. **Many agents**: Consider sharding (multiple clusters)
3. **Slow heartbeats**: Check network latency, reduce heartbeat frequency

### Agent Performance

1. **Task startup slow**: Use artifact caching, faster disk
2. **High CPU**: Reduce monitor interval (default 5s)
3. **Memory pressure**: Increase `stateChannelBufferSize`, reduce task count

### API Performance

1. **Slow /v1/status**: Reduce agent timeout, use caching proxy
2. **High latency**: Add connection pooling, use HTTP/2
3. **JSON overhead**: Consider protobuf for internal APIs

## Profiling

### CPU Profile

```bash
# Profile leader for 30 seconds
go test -bench=BenchmarkDispatchJob -cpuprofile=cpu.prof -benchtime=30s ./internal/leader

# Analyze
go tool pprof -http=:8080 cpu.prof
```

### Memory Profile

```bash
# Profile agent
go test -bench=BenchmarkTaskCreation -memprofile=mem.prof ./internal/agent

# Find allocations
go tool pprof -alloc_space mem.prof
go tool pprof -inuse_space mem.prof
```

### Trace

```bash
# Generate trace
go test -bench=BenchmarkConcurrentHeartbeats -trace=trace.out ./internal/leader

# Visualize
go tool trace trace.out
```

## Regression Testing

Run benchmarks in CI to catch performance regressions:

```bash
# In CI pipeline
go test -bench=. -benchmem ./internal/... | tee bench-$(git rev-parse --short HEAD).txt

# Compare with main branch
git checkout main
go test -bench=. -benchmem ./internal/... > bench-main.txt
git checkout -
benchstat bench-main.txt bench-$(git rev-parse --short HEAD).txt
```

## Real-World Load Testing

Benchmarks test individual components. For end-to-end testing:

```bash
# Start cluster
./bin/agent --config=test-config.yaml

# Load test with hey
hey -n 10000 -c 100 -m POST -d '{"name":"test","command":"sleep 1"}' http://localhost:9080/v1/jobs

# Monitor with pprof
curl http://localhost:9080/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof cpu.prof
```

## Known Bottlenecks

1. **Job lookup by name**: O(n) linear scan
   - Fix: Use map in JobStore interface
   - Impact: High with >10k jobs

2. **Cluster status endpoint**: Fetches from all agents serially
   - Fix: Parallel fetching (already implemented)
   - Impact: High latency with >100 agents

3. **JSON serialization**: High allocation rate
   - Fix: Use sync.Pool for buffers
   - Impact: Moderate at >10k req/sec

4. **State channel contention**: Single channel for all state ops
   - Fix: Shard state by job ID hash
   - Impact: High with >1M ops/sec
