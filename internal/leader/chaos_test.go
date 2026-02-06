package leader

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"easyrun/internal/types"
)


// TestChaos_CascadingFailure tests multiple agents failing simultaneously
func TestChaos_CascadingFailure(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond // Short timeout for testing

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Start 5 mock agents
	agents := make([]*mockAgent, 5)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	// Register all agents
	for i, agent := range agents {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.Heartbeat(agentID, agent.URL(), nil, time.Time{}, "")
	}
	time.Sleep(20 * time.Millisecond)

	// CHAOS: 3 out of 5 agents crash simultaneously
	agents[0].Close()
	agents[2].Close()
	agents[4].Close()

	// Keep alive agents sending heartbeats
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		// Only agents 1 and 3 are still alive
		leader.Heartbeat("agent-1", agents[1].URL(), nil, time.Time{}, "")
		leader.Heartbeat("agent-3", agents[3].URL(), nil, time.Time{}, "")
	}

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(50 * time.Millisecond)

	// Leader should detect 3 dead agents (0, 2, 4)
	aliveAgents := leader.GetAgents()
	if len(aliveAgents) != 2 {
		t.Errorf("Expected 2 alive agents, got %d", len(aliveAgents))
	}

	t.Logf("Cascading failure: 3/5 agents down, %d remaining", len(aliveAgents))
}

// TestChaos_LeaderCrashDuringRollingUpdate tests leader failure mid-update
func TestChaos_LeaderCrashDuringRollingUpdate(t *testing.T) {
	// Simulate: leader crashes after replacing 1/3 instances during rolling update
	// New leader should detect partial update and handle gracefully

	oldLeaderStore := NewMockJobStore()
	oldLeader := New("old-leader", oldLeaderStore, nil)

	ctx1, cancel1 := context.WithCancel(context.Background())
	go oldLeader.stateLoop(ctx1)

	// Old leader has job v1 with 3 instances
	oldJob := &types.Job{
		ID:      "app-v1-id",
		Name:    "app",
		Command: "./app-v1",
		Count:   3,
	}
	oldLeaderStore.StoreJob(oldJob)

	// Simulate placed (3 instances across 3 agents)
	oldLeader.do(func(s *leaderState) {
		s.placed["agent-1"] = map[string]int{"app-v1-id": 1}
		s.placed["agent-2"] = map[string]int{"app-v1-id": 1}
		s.placed["agent-3"] = map[string]int{"app-v1-id": 1}
	})

	// Start rolling update to v2
	newJob := &types.Job{
		ID:      "app-v2-id",
		Name:    "app",
		Command: "./app-v2",
		Count:   3,
	}

	// Simulate partial update: 1 instance replaced
	oldLeaderStore.StoreJob(newJob)
	oldLeader.do(func(s *leaderState) {
		s.placed["agent-1"] = map[string]int{"app-v2-id": 1} // agent-1 updated
		s.placed["agent-2"] = map[string]int{"app-v1-id": 1} // still old
		s.placed["agent-3"] = map[string]int{"app-v1-id": 1} // still old
	})

	// OLD LEADER CRASHES
	cancel1()
	time.Sleep(10 * time.Millisecond)

	// NEW leader takes over
	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go newLeader.stateLoop(ctx2)

	// Agents report their current state (mixed v1 and v2)
	agent1Jobs := []*types.Job{newJob} // Has v2
	agent2Jobs := []*types.Job{oldJob} // Still has v1
	agent3Jobs := []*types.Job{oldJob} // Still has v1

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agent1Jobs, time.Now(), "")
	newLeader.Heartbeat("agent-2", "http://10.0.0.2:8080", agent2Jobs, time.Now(), "")
	newLeader.Heartbeat("agent-3", "http://10.0.0.3:8080", agent3Jobs, time.Now(), "")
	time.Sleep(30 * time.Millisecond)

	// New leader should know about both versions
	jobs := newLeaderStore.GetJobs()
	t.Logf("Jobs after failover during update: %d jobs", len(jobs))

	// Both v1 and v2 exist (partial rollout preserved)
	foundV1 := false
	foundV2 := false
	for _, j := range jobs {
		if j.Command == "./app-v1" {
			foundV1 = true
		}
		if j.Command == "./app-v2" {
			foundV2 = true
		}
	}

	if !foundV1 || !foundV2 {
		t.Errorf("Both versions should exist after mid-update crash (v1=%v, v2=%v)", foundV1, foundV2)
	}
}

// TestChaos_NetworkPartition tests agent isolation from leader
func TestChaos_NetworkPartition(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 50 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Slow agent that times out
	slowAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond) // Longer than client timeout
		w.WriteHeader(http.StatusOK)
	}))
	defer slowAgent.Close()

	// Fast agent
	fastAgent := newMockAgent()
	defer fastAgent.Close()

	// Register both
	leader.Heartbeat("slow-agent", slowAgent.URL, nil, time.Time{}, "")
	leader.Heartbeat("fast-agent", fastAgent.URL(), nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	// Dispatch job
	job := &types.Job{ID: "job-id", Name: "timeout-job", Command: "./app", Count: 1}
	store.StoreJob(job)

	// Leader should timeout on slow agent, retry on fast agent
	err := leader.DispatchJob(job)
	if err != nil {
		t.Logf("Dispatch completed (some agents timed out): %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Slow agent should eventually be marked dead
	time.Sleep(150 * time.Millisecond)
	agents := leader.GetAgents()
	t.Logf("After network partition: %d agents alive", len(agents))

	// At least fast agent should be alive
	foundFast := false
	for _, a := range agents {
		if a.ID == "fast-agent" {
			foundFast = true
		}
	}
	if !foundFast {
		t.Error("Fast agent should still be alive")
	}
}

// TestChaos_AllAgentsDownExceptOne tests cluster surviving with 1 agent
func TestChaos_AllAgentsDownExceptOne(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Create 5 agents
	agents := make([]*mockAgent, 5)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
		agentID := fmt.Sprintf("agent-%d", i)
		leader.Heartbeat(agentID, agents[i].URL(), nil, time.Time{}, "")
	}
	time.Sleep(20 * time.Millisecond)

	// CHAOS: 4 agents crash (0, 1, 2, 3)
	for i := 0; i < 4; i++ {
		agents[i].Close()
	}

	// Keep the survivor (agent-4) sending heartbeats
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		leader.Heartbeat("agent-4", agents[4].URL(), nil, time.Time{}, "")
	}

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(50 * time.Millisecond)

	// Leader should detect 4 dead, 1 alive
	aliveAgents := leader.GetAgents()
	if len(aliveAgents) != 1 {
		t.Errorf("Expected 1 alive agent, got %d", len(aliveAgents))
	}

	// Verify we still have the survivor
	if len(aliveAgents) > 0 && aliveAgents[0].ID != "agent-4" {
		t.Errorf("Survivor should be agent-4, got %s", aliveAgents[0].ID)
	}
}

// TestChaos_RapidAgentChurn tests agents joining/leaving quickly
func TestChaos_RapidAgentChurn(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 50 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	go leader.Run(ctx) // Start dead agent checker

	// Rapidly add and remove agents
	for round := 0; round < 10; round++ {
		agent := newMockAgent()
		agentID := fmt.Sprintf("churn-%d", round)

		// Register
		leader.Heartbeat(agentID, agent.URL(), nil, time.Time{}, "")
		time.Sleep(5 * time.Millisecond)

		// Kill
		agent.Close()
		time.Sleep(15 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond) // Let dead agent checker run

	// System should be stable (all churned agents detected as dead)
	agents := leader.GetAgents()
	t.Logf("After rapid churn: %d agents remain", len(agents))

	// All should be dead/removed
	for _, a := range agents {
		age := time.Since(a.LastSeen)
		if age < 100*time.Millisecond {
			t.Errorf("Agent %s still considered alive after being killed", a.ID)
		}
	}
}

// TestChaos_JobDispatchToDeadAgent tests dispatch handling when agent dies during dispatch
func TestChaos_JobDispatchToDeadAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 50 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Create agent
	agent := newMockAgent()

	// Register
	leader.Heartbeat("dying-agent", agent.URL(), nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	// Kill agent BEFORE dispatch
	agent.Close()

	// Try to dispatch - should fail gracefully
	job := &types.Job{
		ID:      "test-job-id",
		Name:    "test-job",
		Command: "./test",
		Count:   1,
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("Dispatch should fail when agent is dead")
	}

	t.Logf("Graceful failure: %v", err)
}

// TestChaos_SplitBrainScenario tests behavior if two leaders exist temporarily
func TestChaos_SplitBrainScenario(t *testing.T) {
	// Simulate: network partition causes 2 leaders, they each accept jobs, then merge

	leader1Store := NewMockJobStore()
	leader1 := New("leader-1", leader1Store, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go leader1.stateLoop(ctx1)

	leader2Store := NewMockJobStore()
	leader2 := New("leader-2", leader2Store, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go leader2.stateLoop(ctx2)

	// Each leader gets different jobs during partition
	job1 := &types.Job{ID: "job-1-id", Name: "service-a", Command: "./a", Count: 1}
	job2 := &types.Job{ID: "job-2-id", Name: "service-b", Command: "./b", Count: 1}

	leader1Store.StoreJob(job1)
	leader2Store.StoreJob(job2)

	leader1.Heartbeat("agent-1", "http://10.0.0.1:8080", []*types.Job{job1}, time.Now(), "")
	leader2.Heartbeat("agent-2", "http://10.0.0.2:8080", []*types.Job{job2}, time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// PARTITION HEALS: Leader1 wins, leader2's agents now report to leader1
	// Leader1 learns about job2 through agent2's heartbeat
	leader1.Heartbeat("agent-2", "http://10.0.0.2:8080", []*types.Job{job2}, time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Leader1 should now have BOTH jobs
	mergedJobs := leader1Store.GetJobs()
	if len(mergedJobs) != 2 {
		t.Errorf("After split-brain merge, expected 2 jobs, got %d", len(mergedJobs))
	}

	foundA := false
	foundB := false
	for _, j := range mergedJobs {
		if j.Name == "service-a" {
			foundA = true
		}
		if j.Name == "service-b" {
			foundB = true
		}
	}

	if !foundA || !foundB {
		t.Errorf("Both jobs should exist after merge (a=%v, b=%v)", foundA, foundB)
	}

	t.Log("Split-brain resolved: both leaders' jobs merged successfully")
}

// TestChaos_AgentReturnsAfterLongDowntime tests agent rejoining with stale state
func TestChaos_AgentReturnsAfterLongDowntime(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Leader has the NEWER job already (simulating leader knows about v2)
	newJob := &types.Job{ID: "app-id", Name: "app", Command: "./app-v2", Count: 1}
	store.StoreJob(newJob)
	store.stateTime = time.Now()
	time.Sleep(10 * time.Millisecond)

	// Agent comes back with STALE state (running v1, same ID but outdated)
	oldJob := &types.Job{ID: "app-id", Name: "app", Command: "./app-v1", Count: 1}
	staleTime := time.Now().Add(-1 * time.Hour) // Agent has OLD state time
	jobs := leader.Heartbeat("zombie-agent", "http://10.0.0.99:8080", []*types.Job{oldJob}, staleTime, "")

	// Leader should return NEWER state in response (v2)
	// The leader's state is newer, so it returns its jobs to the agent
	foundV2 := false
	for _, j := range jobs {
		if j.Name == "app" && j.Command == "./app-v2" {
			foundV2 = true
			break
		}
	}
	if !foundV2 {
		t.Errorf("Leader should return v2 to agent with stale state, got %v", jobs)
	}

	t.Log("Zombie agent corrected with leader's newer state")
}

// TestChaos_MultipleJobUpdatesDuringFailover tests concurrent updates during failover
func TestChaos_MultipleJobUpdatesDuringFailover(t *testing.T) {
	oldStore := NewMockJobStore()
	oldLeader := New("old-leader", oldStore, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	go oldLeader.stateLoop(ctx1)

	// Old leader has 3 jobs
	for i := 0; i < 3; i++ {
		job := &types.Job{
			ID:      fmt.Sprintf("job-%d-v1", i),
			Name:    fmt.Sprintf("job-%d", i),
			Command: fmt.Sprintf("./app-%d-v1", i),
			Count:   2,
		}
		oldStore.StoreJob(job)
	}

	// Register agents
	oldLeader.Heartbeat("agent-a", "http://10.0.0.1:8080", oldStore.GetJobs(), time.Now(), "")
	oldLeader.Heartbeat("agent-b", "http://10.0.0.2:8080", oldStore.GetJobs(), time.Now(), "")

	// OLD LEADER CRASHES mid-update
	cancel1()

	// NEW LEADER
	newStore := NewMockJobStore()
	newLeader := New("new-leader", newStore, nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go newLeader.stateLoop(ctx2)

	// Agents report with different versions (chaos)
	agentAJobs := []*types.Job{
		{ID: "job-0-v2", Name: "job-0", Command: "./app-0-v2", Count: 2}, // Updated
		{ID: "job-1-v1", Name: "job-1", Command: "./app-1-v1", Count: 2}, // Old
		{ID: "job-2-v1", Name: "job-2", Command: "./app-2-v1", Count: 2}, // Old
	}

	agentBJobs := []*types.Job{
		{ID: "job-0-v1", Name: "job-0", Command: "./app-0-v1", Count: 2}, // Old
		{ID: "job-1-v2", Name: "job-1", Command: "./app-1-v2", Count: 2}, // Updated
		{ID: "job-2-v1", Name: "job-2", Command: "./app-2-v1", Count: 2}, // Old
	}

	newLeader.Heartbeat("agent-a", "http://10.0.0.1:8080", agentAJobs, time.Now(), "")
	newLeader.Heartbeat("agent-b", "http://10.0.0.2:8080", agentBJobs, time.Now(), "")
	time.Sleep(30 * time.Millisecond)

	// Leader should have learned about mixed state
	allJobs := newStore.GetJobs()
	t.Logf("After chaotic failover: %d job versions learned", len(allJobs))

	// Should have multiple versions of same jobs (agents diverged)
	if len(allJobs) < 3 {
		t.Errorf("Expected at least 3 jobs after merge, got %d", len(allJobs))
	}
}

// TestChaos_LeaderMemoryPressure tests behavior under memory pressure
func TestChaos_LeaderMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory pressure test in short mode")
	}

	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Extreme scenario: 10k agents, 100k jobs
	t.Log("Simulating memory pressure: 10k agents, 100k jobs...")
	start := time.Now()

	// Register 10k agents
	for i := 0; i < 10000; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		leader.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{}, "")
	}

	// Create 100k jobs
	for i := 0; i < 100000; i++ {
		job := &types.Job{
			ID:      fmt.Sprintf("job-%d", i),
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
			Count:   1,
		}
		store.StoreJob(job)
	}

	elapsed := time.Since(start)
	t.Logf("Created massive state in %v", elapsed)

	// System should still respond
	agents := leader.GetAgents()
	jobs := leader.GetJobs()

	if len(agents) != 10000 {
		t.Errorf("Expected 10k agents, got %d", len(agents))
	}
	if len(jobs) != 100000 {
		t.Errorf("Expected 100k jobs, got %d", len(jobs))
	}

	// Measure lookup performance under pressure
	start = time.Now()
	_ = leader.FindJobByName("job-50000")
	lookupTime := time.Since(start)

	t.Logf("Job lookup under pressure: %v", lookupTime)

	if lookupTime > 10*time.Millisecond {
		t.Logf("WARNING: Lookup slow (%v) with 100k jobs - consider optimization", lookupTime)
	}
}

// TestChaos_SimultaneousLeaderAndAgentCrash tests double failure
func TestChaos_SimultaneousLeaderAndAgentCrash(t *testing.T) {
	oldStore := NewMockJobStore()
	oldLeader := New("old-leader", oldStore, nil)
	ctx1, cancel1 := context.WithCancel(context.Background())
	go oldLeader.stateLoop(ctx1)

	// Register 3 agents
	agent1 := newMockAgent()
	defer agent1.Close()
	agent2 := newMockAgent()
	// agent2 will crash
	agent3 := newMockAgent()
	defer agent3.Close()

	oldLeader.Heartbeat("agent-1", agent1.URL(), nil, time.Time{}, "")
	oldLeader.Heartbeat("agent-2", agent2.URL(), nil, time.Time{}, "")
	oldLeader.Heartbeat("agent-3", agent3.URL(), nil, time.Time{}, "")

	// Create job
	job := &types.Job{ID: "job-id", Name: "important", Command: "./app", Count: 3}
	oldStore.StoreJob(job)

	// CHAOS: Leader AND agent2 crash simultaneously
	cancel1()
	agent2.Close()
	time.Sleep(10 * time.Millisecond)

	// New leader takes over
	newStore := NewMockJobStore()
	newLeader := New("new-leader", newStore, &http.Client{Timeout: 100 * time.Millisecond})
	newLeader.agentTimeout = 200 * time.Millisecond

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go newLeader.Run(ctx2) // Run() starts stateLoop internally

	// Surviving agents report
	newLeader.Heartbeat("agent-1", agent1.URL(), []*types.Job{job}, time.Now(), "")
	newLeader.Heartbeat("agent-3", agent3.URL(), []*types.Job{job}, time.Now(), "")
	time.Sleep(50 * time.Millisecond)

	// New leader should learn about job from survivors
	jobs := newStore.GetJobs()
	if len(jobs) != 1 || jobs[0].Name != "important" {
		t.Errorf("New leader should have learned job from survivors, got %v", jobs)
	}

	// Verify 2 agents are registered (agent2 is dead)
	agents := newLeader.GetAgents()
	if len(agents) != 2 {
		t.Errorf("Expected 2 agents registered, got %d", len(agents))
	}

	// Note: Placement is NOT tracked from heartbeats in KISS refactor.
	// Reconciliation uses GetClusterStatus() to find actual running tasks.

	t.Log("Double failure handled: leader failover + agent loss")
}

// TestChaos_HeartbeatStorm tests system under heartbeat flood
func TestChaos_HeartbeatStorm(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// 100 agents sending heartbeats as fast as possible
	done := make(chan bool)
	start := time.Now()
	heartbeats := 0

	for i := 0; i < 100; i++ {
		go func(id int) {
			agentID := fmt.Sprintf("agent-%d", id)
			endpoint := fmt.Sprintf("http://10.0.0.%d:8080", id)

			for j := 0; j < 100; j++ {
				leader.Heartbeat(agentID, endpoint, nil, time.Time{}, "")
				heartbeats++
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	elapsed := time.Since(start)
	rate := float64(heartbeats) / elapsed.Seconds()

	t.Logf("Heartbeat storm: %d heartbeats in %v = %.0f HB/sec", heartbeats, elapsed, rate)

	// System should survive
	agents := leader.GetAgents()
	if len(agents) != 100 {
		t.Errorf("Expected 100 agents after storm, got %d", len(agents))
	}
}

// TestChaos_ZeroAgentsAvailable tests dispatch when no agents are available
func TestChaos_ZeroAgentsAvailable(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// No agents registered
	job := &types.Job{
		ID:      "orphan-job",
		Name:    "orphan",
		Command: "./orphan",
		Count:   3,
	}

	err := leader.DispatchJob(job)
	if err == nil {
		t.Error("Dispatch should fail when no agents available")
	}

	// Error message now includes instance info
	if !strings.Contains(err.Error(), "no agents available") {
		t.Errorf("Expected error containing 'no agents available', got: %v", err)
	}

	t.Logf("Graceful handling of zero agents: %v", err)
}

// TestChaos_AgentFlapping tests agent rapidly going up/down
func TestChaos_AgentFlapping(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, &http.Client{Timeout: 50 * time.Millisecond})
	leader.agentTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond) // Let state loop start

	// Agent flaps: up → down → up → down
	for i := 0; i < 5; i++ {
		agent := newMockAgent()
		agentID := "flapping-agent"

		// UP - register agent
		leader.Heartbeat(agentID, agent.URL(), nil, time.Time{}, "")
		time.Sleep(30 * time.Millisecond) // Allow state to update

		// Verify registered
		agents := leader.GetAgents()
		found := false
		for _, a := range agents {
			if a.ID == agentID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Round %d: Agent should be registered", i)
		}

		// DOWN
		agent.Close()
		time.Sleep(150 * time.Millisecond) // Wait for timeout

		// Trigger dead agent check
		leader.checkDeadAgents()
		time.Sleep(20 * time.Millisecond)

		// Should be removed or marked as old
		agents = leader.GetAgents()
		for _, a := range agents {
			age := time.Since(a.LastSeen)
			if a.ID == agentID && age < 100*time.Millisecond {
				t.Errorf("Round %d: Flapping agent should be detected as dead", i)
			}
		}
	}

	t.Log("Flapping agent handled gracefully over 5 cycles")
}
