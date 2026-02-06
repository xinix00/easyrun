package leader

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"easyrun/internal/types"
)

// TestLeaderRestart_RejectUnknownHeartbeat verifies:
// Heartbeat REJECTS unknown agents (returns known=false).
// Unknown agents must re-register via RegisterAgent with placed counts.
// After settle, reconcileJobs has correct placed → no over-dispatch.
func TestLeaderRestart_RejectUnknownHeartbeat(t *testing.T) {
	agents := make([]*mockAgent, 4)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	// === PHASE 1: Dispatch 20 tasks ===
	store := NewMockJobStore()
	leader1 := New("agent-0", store, &http.Client{Timeout: 1 * time.Second})

	ctx1, cancel1 := context.WithCancel(context.Background())
	go leader1.stateLoop(ctx1)

	for i, agent := range agents {
		leader1.RegisterAgent(fmt.Sprintf("agent-%d", i), agent.URL(), "", nil)
	}
	time.Sleep(20 * time.Millisecond)

	job := &types.Job{
		ID:      "my-job-id",
		Name:    "my-job",
		Command: "./my-app",
		Count:   20,
	}
	if err := leader1.DispatchJob(job); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	agentTaskCounts := make([]int, 4)
	for i, agent := range agents {
		agentTaskCounts[i] = agent.TaskCount()
	}
	t.Logf("Phase 1: dispatched %v", agentTaskCounts)

	// === PHASE 2: Leader crashes ===
	cancel1()
	time.Sleep(10 * time.Millisecond)

	// === PHASE 3: New leader with settle ===
	leader2 := New("agent-0", store, &http.Client{Timeout: 1 * time.Second})
	leader2.SetSettleDelay(300 * time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go leader2.stateLoop(ctx2)

	// Leader registers itself
	leader2.RegisterAgent("agent-0", agents[0].URL(), "", map[string]int{job.ID: agentTaskCounts[0]})

	// Other agents heartbeat → should be REJECTED (unknown)
	for i := 1; i < 4; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		_, known := leader2.Heartbeat(agentID, agents[i].URL(), []*types.Job{job}, time.Now(), "")
		if known {
			t.Errorf("Agent %s heartbeat should be rejected (unknown), but was accepted", agentID)
		}
	}

	// Unknown agents should NOT be in agents map
	knownAgents := leader2.GetAgents()
	if len(knownAgents) != 1 {
		t.Errorf("Only agent-0 should be known, got %d agents", len(knownAgents))
	}

	// Agents get 404, re-register with placed counts
	for i := 1; i < 4; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		placed := map[string]int{job.ID: agentTaskCounts[i]}
		leader2.RegisterAgent(agentID, agents[i].URL(), "", placed)
	}

	// Wait for settle → reconcileJobs with correct placed data
	time.Sleep(500 * time.Millisecond)

	// === VERIFY: no over-scheduling ===
	finalTotal := 0
	for i, agent := range agents {
		count := agent.TaskCount()
		t.Logf("Agent %d: was=%d, now=%d", i, agentTaskCounts[i], count)
		finalTotal += count
	}

	if finalTotal != 20 {
		t.Errorf("Total should be 20, got %d", finalTotal)
	}
}
