package api

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"easyrun/internal/leader"
)

// TestServerPortNotReleasedWithoutStop proves that cancelling the context
// does not synchronously release the port. This is the bug: becomeLeader
// fires srv.Run in a goroutine and leaderCancel() doesn't wait for port release.
func TestServerPortNotReleasedWithoutStop(t *testing.T) {
	t.Skip("Race condition demo — proves the bug exists, not a regression test")
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.Run(ctx)
	defer cancel()
	time.Sleep(10 * time.Millisecond)

	// Start server in goroutine (like becomeLeader does)
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	srv := NewServer(l, addr, "")
	_ = srv
	go srv.Run(leaderCtx)
	waitForPort(t, addr)

	// Cancel context (like "Lost leadership") — port release is async
	leaderCancel()

	// Immediately try to bind again — no waiting!
	// This simulates becomeLeader being called right after losing leadership.
	srv2 := NewServer(l, addr, "")
	leaderCtx2, leaderCancel2 := context.WithCancel(ctx)
	defer leaderCancel2()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv2.Run(leaderCtx2)
	}()

	// The new server must be reachable within 1 second
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("Port %s not reusable after context cancel (this is the bug): %v", addr, err)
	}
	conn.Close()
}

// TestServerPortReleasedWithExplicitStop proves that calling srv.Stop()
// synchronously releases the port so a new server can bind immediately.
func TestServerPortReleasedWithExplicitStop(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.Run(ctx)
	defer cancel()
	time.Sleep(10 * time.Millisecond)

	// Start server in goroutine (like becomeLeader does)
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	srv := NewServer(l, addr, "")
	go srv.Run(leaderCtx)
	waitForPort(t, addr)

	// Explicit Stop (synchronous) + cancel context
	srv.Stop()
	leaderCancel()

	// New server should bind immediately
	srv2 := NewServer(l, addr, "")
	leaderCtx2, leaderCancel2 := context.WithCancel(ctx)
	defer leaderCancel2()

	go srv2.Run(leaderCtx2)
	waitForPort(t, addr)

	// Verify new server is serving
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("New server failed to bind to %s after explicit Stop: %v", addr, err)
	}
	conn.Close()
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Server never started listening on %s", addr)
}
