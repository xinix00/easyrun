package leader

import (
	"context"
	"sync"
	"testing"
	"time"

	"easyrun/internal/types"
)

// ============== HEARTBEAT TESTS ==============

func TestLeaderHeartbeatRegistersAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Send heartbeat from remote agent
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})

	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 1 {
		t.Errorf("GetAgents() returned %d agents, want 1", len(agents))
	}

	if agents[0].ID != "remote-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "remote-agent")
	}
	if agents[0].Endpoint != "http://192.168.1.10:8080" {
		t.Errorf("Agent Endpoint = %q, want %q", agents[0].Endpoint, "http://192.168.1.10:8080")
	}
}

func TestLeaderHeartbeatUpdatesLastSeen(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// First heartbeat
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	firstSeen := agents[0].LastSeen

	// Wait and send another heartbeat
	time.Sleep(50 * time.Millisecond)
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	agents = leader.GetAgents()
	if !agents[0].LastSeen.After(firstSeen) {
		t.Error("LastSeen should be updated after second heartbeat")
	}
}

func TestLeaderHeartbeatLearnsJobsFromRemoteAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Remote agent has jobs
	remoteJobs := []*types.Job{
		{ID: "job-1", Name: "remote-job-1", Command: "echo 1"},
		{ID: "job-2", Name: "remote-job-2", Command: "echo 2"},
	}

	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", remoteJobs, time.Time{})
	time.Sleep(10 * time.Millisecond)

	// Store should have learned about the jobs
	jobs := store.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("Store has %d jobs, want 2", len(jobs))
	}
}

func TestLeaderHeartbeatSyncsNewerState(t *testing.T) {
	store := NewMockJobStore()
	store.stateTime = time.Now().Add(-1 * time.Hour) // Our state is old

	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Remote agent has newer state
	newerTime := time.Now()
	remoteJobs := []*types.Job{
		{ID: "newer-job", Name: "newer", Command: "echo newer"},
	}

	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", remoteJobs, newerTime)
	time.Sleep(10 * time.Millisecond)

	// Store should have synced
	if !store.stateTime.Equal(newerTime) {
		t.Errorf("Store stateTime = %v, want %v", store.stateTime, newerTime)
	}
}

func TestLeaderGetAgents(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register multiple agents
	for i := 0; i < 5; i++ {
		leader.Heartbeat("agent-"+string(rune('a'+i)), "http://host:8080", nil, time.Time{})
	}

	time.Sleep(10 * time.Millisecond)

	agents := leader.GetAgents()
	if len(agents) != 5 {
		t.Errorf("GetAgents() returned %d agents, want 5", len(agents))
	}
}

func TestLeaderUnregisterAgent(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agent
	leader.Heartbeat("remote-agent", "http://192.168.1.10:8080", nil, time.Time{})
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 1 {
		t.Fatal("Agent not registered")
	}

	// Unregister
	leader.UnregisterAgent("remote-agent")
	time.Sleep(10 * time.Millisecond)

	if len(leader.GetAgents()) != 0 {
		t.Error("Agent should be unregistered")
	}
}

func TestLeaderConcurrentHeartbeats(t *testing.T) {
	store := NewMockJobStore()
	leader := New("local-agent", store, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	var wg sync.WaitGroup

	// Concurrent heartbeats from multiple agents
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			agentID := "agent-" + string(rune('a'+n%10))
			for j := 0; j < 10; j++ {
				leader.Heartbeat(agentID, "http://host:8080", nil, time.Time{})
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(10 * time.Millisecond)

	// Should have 10 unique agents (a-j)
	agents := leader.GetAgents()
	if len(agents) != 10 {
		t.Errorf("GetAgents() returned %d agents, want 10", len(agents))
	}
}
