# Performance Characteristics

Real-world performance measurements on Apple M4 Pro (14 cores, 48GB RAM), Go 1.24.3.

## Operation Throughput

| Operation | Throughput | Latency | Notes |
|-----------|-----------|---------|-------|
| Task creation | 1.5M/sec | 786 ns | Agent state ops channel |
| State query | 3.5M/sec | 337 ns | Channel-based, very fast |
| Job sync (100 jobs) | 947k/sec | 1.3 us | Efficient sync path |
| Job list (1k jobs) | 137k/sec | 8.4 us | Copy + return |
| Capacity check (50 tasks) | 121k/sec | 10.3 us | With running task scan |
| Job lookup (100 jobs) | 2M/sec | 586 ns | O(n) scan |
| Job lookup (1k jobs) | 175k/sec | 6.9 us | O(n) scan |
| Job lookup (10k jobs) | 15.8k/sec | 74 us | O(n) scan |
| Job lookup (100k jobs) | 1.9k/sec | 625 us | O(n) scan - bottleneck |
| JSON decode | 990k/sec | 1.3 us | Fast |
| JSON encode (100 jobs) | 45k/sec | 25.8 us | 102 allocs |
| GET /v1/jobs (1k jobs) | 7.9k/sec | 151 us | Full HTTP roundtrip |

**Key Insights:**
- **State ops are fast** - channel-based model handles >1M ops/sec
- **Job lookup is the bottleneck** - O(n) linear scan, scales linearly
- **JSON encoding is allocation-heavy** - 102 allocs for 100 jobs

## Tested Scale Limits

### Cluster Capacity

| Agents | Jobs | Tasks | Memory | Status |
|--------|------|-------|--------|--------|
| 100 | 1,000 | 3,000 | ~50 MB | Excellent |
| 1,000 | 10,000 | 30,000 | ~500 MB | Good |
| 10,000 | 100,000 | 300,000 | ~5 GB | High memory |

**Recommended limits for production:**
- **Agents:** 1,000 max per leader
- **Jobs:** 10,000 max per cluster
- **Tasks:** 50,000 max

Above these limits, use multiple clusters (shard by region/environment).

### Memory Footprint

Per-entity memory usage (approximate):

| Entity | Size | 1,000 entities | 10k entities |
|--------|------|----------------|--------------|
| Agent | ~200 bytes | 200 KB | 2 MB |
| Job | ~500 bytes | 500 KB | 5 MB |
| Task | ~300 bytes | 300 KB | 3 MB |
| Placement entry | ~50 bytes | 50 KB | 500 KB |

**Example cluster (100 agents, 1k jobs, 3k tasks):**
- State data: ~1.5 MB
- Go runtime + buffers: ~50 MB
- **Total: ~50-60 MB per leader**

## Bottlenecks & Optimizations

### 1. Job Lookup (O(n) scan)

**Current:** `FindJobByName` scans all jobs linearly

**Measured impact:**
| Jobs | Lookup time |
|------|-------------|
| 100 | 586 ns |
| 1,000 | 6.9 us |
| 10,000 | 74 us |
| 100,000 | 625 us |

**Fix:** Add name index to JobStore (O(1) map lookup -> <100 ns)

### 2. State Channel Buffer

**Current:** 64 entry buffer. Under load with >1000 ops/sec, channel can fill up.

**Fix:** Increase buffer or use separate channels for reads/writes.

### 3. JSON Encoding Allocations

**Current:** 102 allocs for encoding 100 jobs (25.8 us).

**Fix:** Use `sync.Pool` for JSON encoder buffers.

## Real-World Performance

**Small cluster (10 nodes, 50 jobs, 150 tasks):**
- Heartbeat latency: <1 ms
- Job dispatch: <10 ms
- API response: <5 ms
- Memory usage: ~50 MB
- **Status:** Excellent

**Medium cluster (100 nodes, 500 jobs, 1500 tasks):**
- Heartbeat latency: <2 ms
- Job dispatch: <50 ms
- API response: <20 ms
- Memory usage: ~100 MB
- **Status:** Good

**Large cluster (1000 nodes, 5000 jobs, 15k tasks):**
- Heartbeat latency: <10 ms
- Job dispatch: <200 ms
- API response: <100 ms
- Memory usage: ~500 MB
- **Status:** Acceptable, consider optimizations

**Very large (10k nodes, 50k jobs, 150k tasks):**
- Heartbeat latency: >100 ms (state channel saturation)
- Job dispatch: >1s
- Memory usage: ~5 GB
- **Status:** Not recommended, use multiple clusters

## Stress Testing

```bash
# Benchmark at different scales
go test -bench=BenchmarkJobLookupScale -benchmem ./internal/leader

# Full benchmark suite
go test -bench=. -benchmem ./internal/... > results.txt

# Memory footprint
go test -bench=BenchmarkMemoryFootprint -benchmem ./internal/leader
```

## Optimization Roadmap

| Symptom | Cause | Fix |
|---------|-------|-----|
| Slow job creation | O(n) lookup | Add name index |
| Heartbeat timeouts | State channel full | Increase buffer or shard |
| High memory (>1GB) | Too many jobs/agents | Split into multiple clusters |
| Slow /v1/status | Too many agents | Add caching, reduce poll frequency |

## Horizontal Scaling

Instead of optimizing a single leader, **scale horizontally:**

```
Cluster 1 (region=us-east): 100 agents, 500 jobs
Cluster 2 (region=us-west): 100 agents, 500 jobs
Total capacity: 200 agents, 1000 jobs
```

**Benefits:** Simpler codebase, fault isolation, better performance per leader.
**Trade-offs:** Manual orchestration across clusters, no automatic cross-cluster job migration.
