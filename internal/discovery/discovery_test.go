package discovery

import (
	"testing"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/hoplock/mem"
)

func TestDiscoveryNew(t *testing.T) {
	d := New(mem.New(), "192.168.1.10", 8080, 30*time.Second)
	if got := d.NodeAddr(); got != "192.168.1.10:8080" {
		t.Errorf("NodeAddr() = %q, want %q", got, "192.168.1.10:8080")
	}
}

func TestNilBackendIsHarmless(t *testing.T) {
	d := New(nil, "10.0.0.1", 9080, time.Second)
	if d.GetLeader() != "" {
		t.Error("GetLeader() with nil backend should return empty")
	}
	if d.TryBecomeLeader() {
		t.Error("TryBecomeLeader() with nil backend should return false")
	}
	d.ReleaseLeadership() // must not panic
}

func TestTryBecomeLeaderCreates(t *testing.T) {
	d := New(mem.New(), "192.168.1.10", 8080, 30*time.Second)
	if !d.TryBecomeLeader() {
		t.Fatal("expected to claim empty lease")
	}
	if got := d.GetLeader(); got != "192.168.1.10:8080" {
		t.Errorf("GetLeader() = %q, want %q", got, "192.168.1.10:8080")
	}
	if !d.IsLeader() {
		t.Error("IsLeader() should be true")
	}
}

func TestTryBecomeLeaderDeniedWhenHeldByOther(t *testing.T) {
	backend := mem.New()
	other := New(backend, "192.168.1.20", 8080, 30*time.Second)
	if !other.TryBecomeLeader() {
		t.Fatal("other node failed to acquire")
	}

	mine := New(backend, "192.168.1.10", 8080, 30*time.Second)
	if mine.TryBecomeLeader() {
		t.Error("should not be able to take over a non-expired lease")
	}
	if mine.GetLeader() != "192.168.1.20:8080" {
		t.Errorf("expected leader to be other node, got %q", mine.GetLeader())
	}
}

func TestTryBecomeLeaderTakesOverExpired(t *testing.T) {
	backend := mem.New()
	now := time.Unix(1_000_000, 0)
	clock := &fakeClock{now: now}

	other := New(backend, "192.168.1.20", 8080, 10*time.Second)
	other.now = clock.Now
	if !other.TryBecomeLeader() {
		t.Fatal("other node failed to acquire")
	}

	// Past expiry from another candidate's perspective.
	clock.now = now.Add(time.Hour)

	mine := New(backend, "192.168.1.10", 8080, 10*time.Second)
	mine.now = clock.Now
	if !mine.TryBecomeLeader() {
		t.Fatal("expected to take over expired lease")
	}
	if got := mine.GetLeader(); got != "192.168.1.10:8080" {
		t.Errorf("after takeover GetLeader() = %q, want %q", got, "192.168.1.10:8080")
	}
}

func TestRenewKeepsHandle(t *testing.T) {
	d := New(mem.New(), "192.168.1.10", 8080, 30*time.Second)
	if !d.TryBecomeLeader() {
		t.Fatal("acquire")
	}
	for i := 0; i < 3; i++ {
		if !d.RenewLease() {
			t.Fatalf("renew %d failed", i)
		}
	}
}

func TestReleaseAllowsTakeover(t *testing.T) {
	backend := mem.New()
	a := New(backend, "192.168.1.10", 8080, 30*time.Second)
	if !a.TryBecomeLeader() {
		t.Fatal("a acquire")
	}
	a.ReleaseLeadership()

	b := New(backend, "192.168.1.20", 8080, 30*time.Second)
	if !b.TryBecomeLeader() {
		t.Fatal("b should be able to acquire immediately after release")
	}
	if b.GetLeader() != "192.168.1.20:8080" {
		t.Errorf("after takeover GetLeader() = %q", b.GetLeader())
	}
}

func TestGenerationMonotonic(t *testing.T) {
	backend := mem.New()
	now := time.Unix(1_000_000, 0)
	clock := &fakeClock{now: now}

	a := New(backend, "a:1", 0, 10*time.Second)
	a.now = clock.Now
	if !a.TryBecomeLeader() {
		t.Fatal("a acquire")
	}
	state, _, err := backend.Read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 1 {
		t.Errorf("first generation = %d, want 1", state.Generation)
	}

	// a renews — generation stays the same.
	if !a.RenewLease() {
		t.Fatal("renew")
	}
	state, _, _ = backend.Read(t.Context())
	if state.Generation != 1 {
		t.Errorf("after renew generation = %d, want 1", state.Generation)
	}

	// b takes over after expiry — generation bumps.
	clock.now = now.Add(time.Hour)
	b := New(backend, "b:1", 0, 10*time.Second)
	b.now = clock.Now
	if !b.TryBecomeLeader() {
		t.Fatal("b takeover")
	}
	state, _, _ = backend.Read(t.Context())
	if state.Generation != 2 {
		t.Errorf("after takeover generation = %d, want 2", state.Generation)
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// Ensure compile-time that hoplock errors are still in scope.
var _ = hoplock.ErrNoLease
