package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"easyrun/internal/types"
	"easyrun/pkg/config"
)

// ---- mocks ----

type mockDiscoverer struct {
	leader          string
	becomeLeaderOK  bool
	renewLeaseOK    bool
	tryBecomeLeaderCalls int
	releaseLeadershipCalls int
}

func (m *mockDiscoverer) GetLeader() string       { return m.leader }
func (m *mockDiscoverer) RenewLease() bool         { return m.renewLeaseOK }
func (m *mockDiscoverer) ReleaseLeadership()       { m.releaseLeadershipCalls++ }
func (m *mockDiscoverer) TryBecomeLeader() bool {
	m.tryBecomeLeaderCalls++
	return m.becomeLeaderOK
}

type mockAgent struct {
	id             string
	endpoint       string
	jobs           []*types.Job
	placed         map[string]int
	stateTime      time.Time
	syncJobsCalls  int
	syncJobsJobs   []*types.Job
	stopAllCalls   int
}

func (m *mockAgent) ID() string                                    { return m.id }
func (m *mockAgent) Endpoint() string                              { return m.endpoint }
func (m *mockAgent) GetJobs() []*types.Job                         { return m.jobs }
func (m *mockAgent) GetPlacedTaskCounts() map[string]int           { return m.placed }
func (m *mockAgent) GetStateTime() time.Time                       { return m.stateTime }
func (m *mockAgent) StopAllTasks()                                 { m.stopAllCalls++ }
func (m *mockAgent) SyncJobs(jobs []*types.Job, _ time.Time) {
	m.syncJobsCalls++
	m.syncJobsJobs = jobs
}

type mockLeader struct {
	agents []*types.Agent
}

func (m *mockLeader) GetAgents() []*types.Agent { return m.agents }

// ---- helpers ----

func errRegister(err error) func(_, _, _ string, _ map[string]int, _ string) error {
	return func(_, _, _ string, _ map[string]int, _ string) error { return err }
}

func okRegister() func(_, _, _ string, _ map[string]int, _ string) error {
	return errRegister(nil)
}

func errHeartbeat(err error) func(_, _, _ string, _ []*types.Job, _ time.Time, _ string) (*heartbeatResponse, error) {
	return func(_, _, _ string, _ []*types.Job, _ time.Time, _ string) (*heartbeatResponse, error) {
		return nil, err
	}
}

func okHeartbeat(jobs []*types.Job) func(_, _, _ string, _ []*types.Job, _ time.Time, _ string) (*heartbeatResponse, error) {
	return func(_, _, _ string, _ []*types.Job, _ time.Time, _ string) (*heartbeatResponse, error) {
		return &heartbeatResponse{Jobs: jobs}, nil
	}
}

func nopBecomeLeader() (func(), leaderAPI) {
	return func() {}, nil
}

func newTestLoop(disc *mockDiscoverer, ag *mockAgent) *agentLoop {
	cfg := config.DefaultConfig()
	cfg.Node.IP = "127.0.0.1"
	cfg.Node.Port = 8080
	return &agentLoop{
		ctx:            context.Background(),
		cfg:            cfg,
		ag:             ag,
		disc:           disc,
		doRegister:     okRegister(),
		doHeartbeat:    okHeartbeat(nil),
		doBecomeLeader: nopBecomeLeader,
	}
}

// ---- tests ----

// No leader from raft → 4 ticks → tryTakeOver triggered (~30s: T=0,10,20,30)
func TestTick_NoLeader_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: ""}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	for i := range 4 {
		loop.tick()
		if loop.failCount != i+1 {
			t.Fatalf("tick %d: failCount = %d, want %d", i+1, loop.failCount, i+1)
		}
	}

	if disc.tryBecomeLeaderCalls != 1 {
		t.Errorf("TryBecomeLeader calls = %d, want 1", disc.tryBecomeLeaderCalls)
	}
}

// 3 ticks must NOT yet trigger tryTakeOver
func TestTick_NoLeader_NotYetAt3(t *testing.T) {
	disc := &mockDiscoverer{leader: ""}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	for range 3 {
		loop.tick()
	}

	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader should not be called after only 3 failures, got %d", disc.tryBecomeLeaderCalls)
	}
}

// Register fails 4 times → tryTakeOver triggered (~30s with immediate first tick)
func TestTick_RegisterFails_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.doRegister = errRegister(errors.New("connection refused"))

	for range 4 {
		loop.tick()
	}

	if disc.tryBecomeLeaderCalls != 1 {
		t.Errorf("TryBecomeLeader calls = %d, want 1", disc.tryBecomeLeaderCalls)
	}
}

// Register fails 7 times → StopAllTasks called (network isolation, ~60s)
func TestTick_RegisterFails7_StopsAllTasks(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080", becomeLeaderOK: false}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.doRegister = errRegister(errors.New("connection refused"))

	for range 7 {
		loop.tick()
	}

	if ag.stopAllCalls != 1 {
		t.Errorf("StopAllTasks calls = %d, want 1", ag.stopAllCalls)
	}
	// failCount is clamped to 4 after StopAllTasks
	if loop.failCount != 4 {
		t.Errorf("failCount = %d, want 4 (clamped)", loop.failCount)
	}
}

// Successful register → registered=true, failCount reset
func TestTick_RegisterSuccess_ResetsState(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.failCount = 2 // simulated prior failures

	loop.tick()

	if !loop.registered {
		t.Error("expected registered = true")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
	if loop.lastLeaderAddr != "leader:9080" {
		t.Errorf("lastLeaderAddr = %q, want %q", loop.lastLeaderAddr, "leader:9080")
	}
}

// Heartbeat fails 4 times → tryTakeOver triggered (~30s)
func TestTick_HeartbeatFails_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.doHeartbeat = errHeartbeat(errors.New("timeout"))

	for range 4 {
		loop.tick()
	}

	if disc.tryBecomeLeaderCalls != 1 {
		t.Errorf("TryBecomeLeader calls = %d, want 1", disc.tryBecomeLeaderCalls)
	}
}

// Heartbeat returns 404 → re-register, no failCount increment
func TestTick_HeartbeatNotRegistered_Reregisters(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.doHeartbeat = errHeartbeat(errNotRegistered)

	loop.tick()

	if loop.registered {
		t.Error("expected registered = false after 404")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0 (no failure counted for 404)", loop.failCount)
	}
	if loop.lastLeaderAddr != "" {
		t.Errorf("lastLeaderAddr = %q, want empty (force re-discovery)", loop.lastLeaderAddr)
	}
	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader calls = %d, want 0 (just re-register)", disc.tryBecomeLeaderCalls)
	}
}

// Successful heartbeat → failCount=0, SyncJobs called when jobs present
func TestTick_HeartbeatSuccess_SyncsJobs(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.failCount = 2 // simulated prior failures

	jobs := []*types.Job{{Name: "my-api"}}
	loop.doHeartbeat = okHeartbeat(jobs)

	loop.tick()

	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
	if ag.syncJobsCalls != 1 {
		t.Errorf("SyncJobs calls = %d, want 1", ag.syncJobsCalls)
	}
}

// Successful heartbeat with empty jobs → SyncJobs NOT called
func TestTick_HeartbeatEmptyJobs_NoSync(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"

	loop.tick()

	if ag.syncJobsCalls != 0 {
		t.Errorf("SyncJobs calls = %d, want 0 (empty response)", ag.syncJobsCalls)
	}
}

// After successful takeover, stopLeader is set and failCount is reset
func TestTick_TakeoverSucceeds_SetsStopLeader(t *testing.T) {
	disc := &mockDiscoverer{leader: "", becomeLeaderOK: true}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	stopCalled := false
	loop.doBecomeLeader = func() (func(), leaderAPI) {
		return func() { stopCalled = true }, &mockLeader{}
	}

	// 4 ticks → takeover (~30s)
	for range 4 {
		loop.tick()
	}

	if loop.stopLeader == nil {
		t.Fatal("stopLeader should be set after successful takeover")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0 after takeover", loop.failCount)
	}

	// cleanup: call stopLeader to avoid leaks
	loop.stopLeader()
	if !stopCalled {
		t.Error("stopLeader function was not the one returned by doBecomeLeader")
	}
}

// Register failure count is independent per failure — 2 fails then success resets
func TestTick_PartialFailureThenSuccess_Resets(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	callCount := 0
	loop.doRegister = func(_, _, _ string, _ map[string]int, _ string) error {
		callCount++
		if callCount < 3 {
			return errors.New("not ready")
		}
		return nil
	}

	loop.tick() // fail 1
	loop.tick() // fail 2
	loop.tick() // success

	if !loop.registered {
		t.Error("expected registered = true after eventual success")
	}
	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader should not be called with <4 failures, got %d", disc.tryBecomeLeaderCalls)
	}
}

// Uses cached leader address, does not call GetLeader when lastLeaderAddr set
func TestTick_UsesCachedLeaderAddr(t *testing.T) {
	disc := &mockDiscoverer{leader: "wrong:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "cached:9080"

	var calledAddr string
	loop.doHeartbeat = func(addr, _, _ string, _ []*types.Job, _ time.Time, _ string) (*heartbeatResponse, error) {
		calledAddr = addr
		return &heartbeatResponse{}, nil
	}

	loop.tick()

	if calledAddr != "cached:9080" {
		t.Errorf("heartbeat sent to %q, want cached %q", calledAddr, "cached:9080")
	}
}
