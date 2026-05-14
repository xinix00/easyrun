// Package discovery wraps a hoplock.Backend in an imperative
// acquire/renew/release API tuned to hop's existing tick loop.
//
// Mutual exclusion lives entirely in the backend: every claim is a
// conditional write keyed on the last observed handle (ETag). The
// in-memory Discovery only caches the handle so the next renew can prove
// it has not been displaced.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/hoplock/mem"
	"github.com/xinix00/hoplock/s3"
	"github.com/xinix00/hoplockserver/client"
)

const backendTimeout = 5 * time.Second

// Discovery is the leader-election handle used by the agent main loop. It
// is safe to call from multiple goroutines.
type Discovery struct {
	backend hoplock.Backend
	owner   string
	ttl     time.Duration
	now     func() time.Time

	mu     sync.Mutex
	handle string
}

// New returns a Discovery bound to backend. The owner string is the
// "ip:port" address advertised as the leader address; ttl is how long
// each acquired or renewed lease is valid for.
func New(backend hoplock.Backend, nodeIP string, nodePort int, ttl time.Duration) *Discovery {
	return &Discovery{
		backend: backend,
		owner:   fmt.Sprintf("%s:%d", nodeIP, nodePort),
		ttl:     ttl,
		now:     time.Now,
	}
}

// HoplockServerBackend returns a hoplock.Backend that talks HTTP to a
// hoplockserver. The lease object is stored under "leases/<clusterName>".
func HoplockServerBackend(serverURL, apiKey, clusterName string) hoplock.Backend {
	return &client.Backend{
		URL:    serverURL,
		Key:    "leases/" + clusterName,
		APIKey: apiKey,
	}
}

// InMemoryBackend returns an in-process backend, used for standalone
// (single-node) mode and tests.
func InMemoryBackend() hoplock.Backend { return mem.New() }

// S3Backend returns a hoplock.Backend backed by an S3-compatible object
// store. The lease object lives at "leases/<clusterName>" within the
// supplied bucket.
type S3BackendConfig struct {
	Endpoint        string
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UsePathStyle    bool
}

// S3Backend wires a hoplock/s3 backend for the given cluster.
func S3Backend(cfg S3BackendConfig, clusterName string) hoplock.Backend {
	return &s3.Backend{
		Endpoint:        cfg.Endpoint,
		Bucket:          cfg.Bucket,
		Key:             "leases/" + clusterName,
		Region:          cfg.Region,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		UsePathStyle:    cfg.UsePathStyle,
	}
}

// NodeAddr returns this node's owner identity.
func (d *Discovery) NodeAddr() string { return d.owner }

// GetLeader returns the current leader address, or "" if there is no
// active lease or the backend cannot be reached.
func (d *Discovery) GetLeader() string {
	if d.backend == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), backendTimeout)
	defer cancel()
	state, _, err := d.backend.Read(ctx)
	if err != nil {
		return ""
	}
	if d.now().After(state.ExpiresAt) {
		return ""
	}
	return state.Owner
}

// TryBecomeLeader attempts to claim the lease. Returns true if we now
// hold it. Three success paths: create from no-lease, refresh while we
// already own it, or take over an expired lease.
func (d *Discovery) TryBecomeLeader() bool {
	if d.backend == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), backendTimeout)
	defer cancel()

	state, handle, err := d.backend.Read(ctx)
	switch {
	case errors.Is(err, hoplock.ErrNoLease):
		return d.refresh(ctx, "", 1)
	case err != nil:
		return false
	case state.Owner == d.owner:
		return d.refresh(ctx, handle, state.Generation)
	case d.now().After(state.ExpiresAt):
		return d.refresh(ctx, handle, state.Generation+1)
	default:
		return false
	}
}

// RenewLease refreshes our hold on the lease. Currently identical to
// TryBecomeLeader — same code path handles the "renew our own claim"
// case explicitly.
func (d *Discovery) RenewLease() bool {
	return d.TryBecomeLeader()
}

// ReleaseLeadership best-effort deletes the lease, allowing another
// candidate to take over immediately instead of waiting for TTL.
func (d *Discovery) ReleaseLeadership() {
	if d.backend == nil {
		return
	}
	d.mu.Lock()
	handle := d.handle
	d.handle = ""
	d.mu.Unlock()
	if handle == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), backendTimeout)
	defer cancel()
	_ = d.backend.Delete(ctx, handle)
}

// IsLeader reports whether the current leader is this node.
func (d *Discovery) IsLeader() bool {
	return d.GetLeader() == d.owner
}

func (d *Discovery) refresh(ctx context.Context, prevHandle string, gen int64) bool {
	state := &hoplock.State{
		Generation: gen,
		ExpiresAt:  d.now().Add(d.ttl),
		Owner:      d.owner,
	}
	newHandle, err := d.backend.Write(ctx, prevHandle, state)
	if err != nil {
		return false
	}
	d.mu.Lock()
	d.handle = newHandle
	d.mu.Unlock()
	return true
}
