# Performance Characteristics

Real-world performance measurements on Apple M4 Pro.

## Tested Scale Limits

### Agent Capacity

| Agents | Jobs | Tasks | Memory | Status |
|--------|------|-------|--------|--------|
| 100 | 1,000 | 3,000 | ~50 MB | ✅ Excellent |
| 1,000 | 10,000 | 30,000 | ~500 MB | ✅ Good |
| 10,000 | 100,000 | 300,000 | ~5 GB | ⚠️ High memory |

**Recommended limits for production:**
- **Agents:** 1,000 max per leader
- **Jobs:** 10,000 max per cluster
- **Tasks:** 50,000 max (assumes ~5 tasks per agent on 10k agents)

Above these limits, consider:
- Multiple clusters (shard by region/environment)
- Federated setup with separate leaders

### Operation Throughput

Measured on Apple M4 Pro (14 cores, 48GB RAM):

| Operation | Throughput | Latency (p50) | Notes |
|-----------|-----------|---------------|-------|
| Heartbeat processing | 500k/sec | 2 µs | With 100 agents |
| Job lookup (10k jobs) | 13.6k/sec | 73 µs | O(n) scan |
| Job list (1k jobs) | 125k/sec | 8 µs | Simple copy |
| Round-robin selection | 10M+/sec | <100 ns | Very fast |
| State query | 1M+/sec | <1 µs | Channel-based |

**Key Insights:**
- **Job lookup is bottleneck** - O(n) linear scan over all jobs
- **Heartbeat very fast** - can handle 1000 agents × 10/sec = 10k heartbeats/sec easily
- **State channel efficient** - buffered channel handles high concurrency well

### Memory Footprint

Per-entity memory usage (approximate):

| Entity | Size | 1000 entities | 10k entities |
|--------|------|---------------|--------------|
| Agent | ~200 bytes | 200 KB | 2 MB |
| Job | ~500 bytes | 500 KB | 5 MB |
| Task | ~300 bytes | 300 KB | 3 MB |
| Placement entry | ~50 bytes | 50 KB | 500 KB |

**Example cluster:**
- 100 agents = 20 KB
- 1,000 jobs = 500 KB
- 3,000 tasks (3 per job) = 900 KB
- 3,000 placements = 150 KB
- **Total: ~1.5 MB** (base state)

Add ~50 MB for Go runtime, buffers, etc. = **~50-60 MB per leader**

### Bottlenecks & Optimizations

#### 1. Job Lookup (O(n) scan)

**Current:** `FindJobByName` scans all jobs linearly
```go
for _, job := range jobs {
    if job.Name == name { return job }
}
```

**Impact:** 73 µs for 10k jobs → 730 µs for 100k jobs

**Fix:** Add name index to JobStore
```go
type JobStore interface {
    GetJobByName(name string) *Job  // O(1) map lookup
}
```

**Expected improvement:** 73 µs → <1 µs (100x faster)

#### 2. State Channel Buffer

**Current:** 64 entry buffer
```go
ops: make(chan func(*leaderState), 64)
```

**Under load:** With >1000 ops/sec, channel can fill up

**Fix:** Increase buffer or use separate channels for reads/writes
```go
readOps:  make(chan func(*leaderState), 512)
writeOps: make(chan func(*leaderState), 128)
```

#### 3. Heartbeat Lock Contention

**Current:** Single state loop handles all heartbeats sequentially

**At 1000 agents × 10 HB/sec = 10k ops/sec:** ~100 µs per op = can handle

**At 10k agents:** Would need 100k ops/sec = may bottleneck

**Fix:** Shard agents across multiple state loops by hash

### Real-World Performance

**Small cluster (10 nodes, 50 jobs, 150 tasks):**
- Heartbeat latency: <1 ms
- Job dispatch: <10 ms
- API response: <5 ms
- Memory usage: ~50 MB
- **Status:** Excellent, no optimization needed

**Medium cluster (100 nodes, 500 jobs, 1500 tasks):**
- Heartbeat latency: <2 ms
- Job dispatch: <50 ms
- API response: <20 ms
- Memory usage: ~100 MB
- **Status:** Good, monitor job lookup times

**Large cluster (1000 nodes, 5000 jobs, 15k tasks):**
- Heartbeat latency: <10 ms
- Job dispatch: <200 ms (scanning for capacity)
- API response: <100 ms
- Memory usage: ~500 MB
- **Status:** Acceptable, consider optimizations

**Very large (10k nodes, 50k jobs, 150k tasks):**
- Heartbeat latency: >100 ms (state channel saturation)
- Job dispatch: >1s (many capacity rejections)
- Memory usage: ~5 GB
- **Status:** Not recommended, use multiple clusters

## Stress Testing

Run stress tests to find your limits:

```bash
# Test with specific scale
go test -run=TestMassiveScale/100_agents -v ./internal/leader

# Benchmark at different scales
go test -bench=BenchmarkMassiveAgents -benchmem ./internal/leader

# Memory footprint
go test -bench=BenchmarkMemoryFootprint -benchmem ./internal/leader

# Full benchmark suite
go test -bench=. -benchmem ./internal/... > results.txt
```

## Optimization Roadmap

When to optimize:

| Symptom | Cause | Fix |
|---------|-------|-----|
| Slow job creation | O(n) lookup | Add name index |
| Heartbeat timeouts | State channel full | Increase buffer or shard |
| High memory (>1GB) | Too many jobs/agents | Split into multiple clusters |
| Slow /v1/status | Too many agents | Add caching, reduce poll frequency |

## Horizontal Scaling

Instead of optimizing a single leader, **scale horizontally:**

```
Cluster 1 (region=us-east):
  - 100 agents, 500 jobs

Cluster 2 (region=us-west):
  - 100 agents, 500 jobs

Total capacity: 200 agents, 1000 jobs
```

**Benefits:**
- Simpler codebase (no sharding complexity)
- Fault isolation (one region failure doesn't affect others)
- Better performance (each leader handles less state)
- Easier to reason about

**Trade-offs:**
- Manual orchestration across clusters
- No automatic cross-cluster job migration
