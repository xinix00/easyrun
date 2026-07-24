package agentloop

import (
	"errors"
	"testing"

	"hop/internal/types"
	"hop/pkg/config"
)

// ---- mocks ----

type mockDiscoverer struct {
	leader                 string
	becomeLeaderOK         bool
	renewLeaseOK           bool
	renewLeaseDisplaced    bool
	tryBecomeLeaderCalls   int
	releaseLeadershipCalls int
}

func (m *mockDiscoverer) GetLeader() string { return m.leader }
func (m *mockDiscoverer) RenewLease() (bool, bool) {
	return m.renewLeaseOK, m.renewLeaseDisplaced
}
func (m *mockDiscoverer) ReleaseLeadership() { m.releaseLeadershipCalls++ }
func (m *mockDiscoverer) TryBecomeLeader() bool {
	m.tryBecomeLeaderCalls++
	return m.becomeLeaderOK
}

type mockAgent struct {
	id           string
	endpoint     string
	placed       map[string]int
	stopAllCalls int
}

func (m *mockAgent) ID() string                          { return m.id }
func (m *mockAgent) Endpoint() string                    { return m.endpoint }
func (m *mockAgent) GetPlacedTaskCounts() map[string]int { return m.placed }
func (m *mockAgent) StopAllTasks()                       { m.stopAllCalls++ }

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

func errHeartbeat(err error) func(_, _, _, _ string) error {
	return func(_, _, _, _ string) error {
		return err
	}
}

func okHeartbeat() func(_, _, _, _ string) error {
	return func(_, _, _, _ string) error {
		return nil
	}
}

func nopBecomeLeader() (func(), LeaderAPI) {
	return func() {}, nil
}

func newTestLoop(disc *mockDiscoverer, ag *mockAgent) *Loop {
	cfg := config.DefaultConfig()
	cfg.Node.IP = "127.0.0.1"
	cfg.Node.Port = 8080
	return &Loop{
		Cfg:            cfg,
		Ag:             ag,
		Disc:           disc,
		DoRegister:     okRegister(),
		DoHeartbeat:    okHeartbeat(),
		DoBecomeLeader: nopBecomeLeader,
	}
}

// ---- tests ----

// No leader from raft → 4 ticks → tryTakeOver triggered (~30s: T=0,10,20,30)
func TestTick_NoLeader_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: ""}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	for i := range 4 {
		loop.Tick()
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
		loop.Tick()
	}

	if disc.tryBecomeLeaderCalls != 0 {
		t.Errorf("TryBecomeLeader should not be called after only 3 failures, got %d", disc.tryBecomeLeaderCalls)
	}
}

// Register fails 4 times → tryTakeOver triggered (~30s with immediate first tick)
func TestTick_RegisterFails_TriggersAfter4(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.DoRegister = errRegister(errors.New("connection refused"))

	for range 4 {
		loop.Tick()
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
	loop.DoRegister = errRegister(errors.New("connection refused"))

	for range 7 {
		loop.Tick()
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

	loop.Tick()

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
	loop.DoHeartbeat = errHeartbeat(errors.New("timeout"))

	for range 4 {
		loop.Tick()
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
	loop.DoHeartbeat = errHeartbeat(ErrNotRegistered)

	loop.Tick()

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

// Successful heartbeat → failCount reset. Heartbeat is nu puur liveness
// (16-07): geen job-sync meer — gewenste staat heeft één auteur (leader→S3).
func TestTick_HeartbeatSuccess_ResetsFailCount(t *testing.T) {
	disc := &mockDiscoverer{leader: "leader:9080"}
	ag := &mockAgent{id: "a1"}
	loop := newTestLoop(disc, ag)
	loop.registered = true
	loop.lastLeaderAddr = "leader:9080"
	loop.failCount = 2 // simulated prior failures

	loop.DoHeartbeat = okHeartbeat()
	loop.Tick()

	if loop.failCount != 0 {
		t.Errorf("failCount = %d, want 0", loop.failCount)
	}
}

// After successful takeover, stopLeader is set and failCount is reset
func TestTick_TakeoverSucceeds_SetsStopLeader(t *testing.T) {
	disc := &mockDiscoverer{leader: "", becomeLeaderOK: true}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})

	stopCalled := false
	loop.DoBecomeLeader = func() (func(), LeaderAPI) {
		return func() { stopCalled = true }, &mockLeader{}
	}

	// 4 ticks → takeover (~30s)
	for range 4 {
		loop.Tick()
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
	loop.DoRegister = func(_, _, _ string, _ map[string]int, _ string) error {
		callCount++
		if callCount < 3 {
			return errors.New("not ready")
		}
		return nil
	}

	loop.Tick() // fail 1
	loop.Tick() // fail 2
	loop.Tick() // success

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

// Leader with raft unreachable but agents connected stays leader
func TestTick_LeaderRaftDown_StaysLeader(t *testing.T) {
	disc := &mockDiscoverer{renewLeaseOK: false}
	loop := newTestLoop(disc, &mockAgent{id: "leader1"})
	loop.stopLeader = func() {}
	loop.l = &mockLeader{agents: []*types.Agent{{ID: "follower1"}}}

	loop.Tick()

	if loop.stopLeader == nil {
		t.Error("should still be leader when agents are connected")
	}
}

// Leader with raft unreachable and NO agents loses leadership
func TestTick_LeaderRaftDown_NoAgents_LosesLeadership(t *testing.T) {
	disc := &mockDiscoverer{renewLeaseOK: false}
	loop := newTestLoop(disc, &mockAgent{id: "leader1"})

	stopCalled := false
	loop.stopLeader = func() { stopCalled = true }
	loop.l = &mockLeader{agents: []*types.Agent{}}

	loop.Tick()

	if !stopCalled {
		t.Error("should have called stopLeader")
	}
	if loop.stopLeader != nil {
		t.Error("stopLeader should be nil after losing leadership")
	}
}

// Raft recovers after being down → failCount resets
func TestTick_LeaderRaftRecovers_ResetsFailCount(t *testing.T) {
	disc := &mockDiscoverer{renewLeaseOK: false}
	loop := newTestLoop(disc, &mockAgent{id: "leader1"})
	loop.stopLeader = func() {}
	loop.l = &mockLeader{agents: []*types.Agent{{ID: "follower1"}}}
	loop.failCount = 3

	// Raft down: stays leader, failCount unchanged
	loop.Tick()
	if loop.failCount != 3 {
		t.Fatalf("failCount should stay %d during raft-down, got %d", 3, loop.failCount)
	}

	// Raft recovers
	disc.renewLeaseOK = true
	loop.Tick()
	if loop.failCount != 0 {
		t.Errorf("failCount should reset to 0 after raft recovery, got %d", loop.failCount)
	}
}

// Uses cached leader address, does not call GetLeader when lastLeaderAddr set
func TestTick_UsesCachedLeaderAddr(t *testing.T) {
	disc := &mockDiscoverer{leader: "wrong:9080"}
	loop := newTestLoop(disc, &mockAgent{id: "a1"})
	loop.registered = true
	loop.lastLeaderAddr = "cached:9080"

	var calledAddr string
	loop.DoHeartbeat = func(addr, _, _, _ string) error {
		calledAddr = addr
		return nil
	}

	loop.Tick()

	if calledAddr != "cached:9080" {
		t.Errorf("heartbeat sent to %q, want cached %q", calledAddr, "cached:9080")
	}
}
