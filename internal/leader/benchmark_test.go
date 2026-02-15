package leader

import (
	"context"
	"fmt"
	"testing"
	"time"

	"easyrun/internal/types"
)

// startLeader creates and starts a leader for benchmarking.
// Uses a long settle delay to prevent RegisterAgent from triggering
// reconcileJobs, which would attempt HTTP calls to fake agent endpoints.
func startLeader(store JobStore) (*Leader, context.CancelFunc) {
	l := New("local-agent", store, nil)
	l.SetSettleDelay(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go l.stateLoop(ctx)
	return l, cancel
}

// BenchmarkDispatchJob measures dispatch throughput
func BenchmarkDispatchJob(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Register 10 agents
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
		leader.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{}, "")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job := &types.Job{
			ID:      fmt.Sprintf("job-%d", i),
			Name:    fmt.Sprintf("bench-job-%d", i),
			Command: "echo hello",
			Count:   1,
		}
		store.StoreJob(job)
		// Dispatch simulation (without actual HTTP calls)
	}
}

// BenchmarkHeartbeat measures heartbeat processing throughput
func BenchmarkHeartbeat(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Pre-create a job
	job := &types.Job{
		ID:      "test-job",
		Name:    "bench-job",
		Command: "echo test",
		Count:   1,
	}
	store.StoreJob(job)

	// Pre-register agents
	for i := 0; i < 100; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		leader.RegisterAgent(agentID, endpoint, "", nil)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		agentID := fmt.Sprintf("agent-%d", i%100)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i%100)
		leader.Heartbeat(agentID, endpoint, nil, time.Time{}, "")
	}
}

// BenchmarkGetAgents measures agent list retrieval
func BenchmarkGetAgents(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Register 1000 agents
	for i := 0; i < 1000; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
		leader.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{}, "")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		agents := leader.GetAgents()
		if len(agents) != 1000 {
			b.Fatalf("Expected 1000 agents, got %d", len(agents))
		}
	}
}

// BenchmarkFindJobByName measures job lookup performance
func BenchmarkFindJobByName(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Create 10000 jobs
	for i := 0; i < 10000; i++ {
		job := &types.Job{
			ID:      fmt.Sprintf("job-%d", i),
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
		}
		store.StoreJob(job)
		leader.do(func(s *leaderState) { s.nameToID[job.Name] = job.ID })
	}
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("job-%d", i%10000)
		job := leader.FindJobByName(name)
		if job == nil {
			b.Fatal("Job not found")
		}
	}
}

// BenchmarkRoundRobinSelection measures round-robin agent selection
func BenchmarkRoundRobinSelection(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Register 100 agents
	for i := 0; i < 100; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
		leader.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{}, "")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		agent := leader.nextAgent()
		if agent == nil {
			b.Fatal("No agent selected")
		}
	}
}

// BenchmarkConcurrentHeartbeats measures concurrent heartbeat throughput
func BenchmarkConcurrentHeartbeats(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Pre-register agents
	for i := 0; i < 1000; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		leader.RegisterAgent(agentID, endpoint, "", nil)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			agentID := fmt.Sprintf("agent-%d", i%1000)
			endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i%1000)
			leader.Heartbeat(agentID, endpoint, nil, time.Time{}, "")
			i++
		}
	})
}

// BenchmarkPlacedUpdate measures placed tracking overhead
func BenchmarkPlacedUpdate(b *testing.B) {
	store := NewMockJobStore()
	leader, cancel := startLeader(store)
	defer cancel()

	// Register agents
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.RegisterAgent(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), "", nil)
		leader.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{}, "")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		jobID := fmt.Sprintf("job-%d", i%1000)
		agentID := fmt.Sprintf("agent-%d", i%10)

		// Simulate placed update
		leader.do(func(s *leaderState) {
			if s.placed[agentID] == nil {
				s.placed[agentID] = make(map[string]int)
			}
			s.placed[agentID][jobID]++
		})
	}
}
