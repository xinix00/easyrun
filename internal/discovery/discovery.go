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
	"log"
	"sync"
	"time"

	"github.com/xinix00/hoplock"
	"github.com/xinix00/hoplock/mem"
	"github.com/xinix00/hoplock/s3"
	"github.com/xinix00/hoplockserver/client"

	"hop/pkg/config"
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

// StatePersister is the committed-state contract used by the leader. It is
// defined here (structurally identical to leader.StatePersister) so
// discovery can build stores and hand them to the leader without importing
// the leader package.
type StatePersister interface {
	Save(ctx context.Context, snapshot []byte) error
	Load(ctx context.Context) (snapshot []byte, ok bool, err error)
}

// S3StateStore persists the leader's committed cluster state as a plain
// object at "state/<cluster>", next to the election lease — same bucket,
// same credentials, same signer (leader.StatePersister). Renaming or
// deleting that object in the bucket is the operator's "boot clean" switch.
type S3StateStore struct {
	b   *s3.Backend
	key string
}

// NewS3StateStore wires the state object for the given cluster.
func NewS3StateStore(cfg S3BackendConfig, clusterName string) *S3StateStore {
	return &S3StateStore{
		b: &s3.Backend{
			Endpoint:        cfg.Endpoint,
			Bucket:          cfg.Bucket,
			Key:             "state/" + clusterName, // alleen voor foutmeldingen; object-API krijgt de key expliciet
			Region:          cfg.Region,
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			SessionToken:    cfg.SessionToken,
			UsePathStyle:    cfg.UsePathStyle,
		},
		key: "state/" + clusterName,
	}
}

// StateStoreFromConfig geeft de committed-state-store voor deze cluster-
// config, of nil wanneer er geen durable backend is (standalone / mem). Dit
// is bewust de ENIGE gate zodat cmd/agent en agentboot niet uit elkaar lopen.
//
// De backend die de LEASE houdt, houdt ook de STAAT — één bron van waarheid:
//   - Een bruikbare S3-sectie ⇒ S3 (lease én state in dezelfde bucket).
//   - Anders een hoplockserver-URL ⇒ dezelfde server, object "state/<cluster>"
//     naast "leases/<cluster>". Zo krijgt de default (gratis, selfhosted)
//     modus óók durable desired state — een nieuwe leader verliest na failover
//     geen jobs meer die elders draaiden.
//   - mem / standalone ⇒ nil: in-process, geen netwerk-state nodig (de agent
//     bewaart z'n lokale state.json).
func StateStoreFromConfig(cfg *config.Config) StatePersister {
	if s3c := cfg.Cluster.Lock.S3; s3c.Bucket != "" && s3c.Endpoint != "" {
		return NewS3StateStore(S3BackendConfig{
			Endpoint:        s3c.Endpoint,
			Bucket:          s3c.Bucket,
			Region:          s3c.Region,
			AccessKeyID:     s3c.AccessKeyID,
			SecretAccessKey: s3c.SecretAccessKey,
			SessionToken:    s3c.SessionToken,
			UsePathStyle:    s3c.UsePathStyle,
		}, cfg.Cluster.Name)
	}
	if lc := cfg.Cluster.Lock; (lc.Type == "" || lc.Type == "hoplockserver") && lc.URL != "" {
		return NewHoplockServerStateStore(lc.URL, lc.APIKey, cfg.Cluster.Name)
	}
	return nil
}

// Save overschrijft de snapshot (enige schrijver: de leaseholder).
func (s *S3StateStore) Save(ctx context.Context, snapshot []byte) error {
	return s.b.PutObject(ctx, s.key, snapshot, "application/json")
}

// Load leest de snapshot; ok=false = geen object = schone boot.
func (s *S3StateStore) Load(ctx context.Context) ([]byte, bool, error) {
	return s.b.GetObject(ctx, s.key)
}

// HoplockServerStateStore persists the committed cluster state on the same
// hoplockserver that holds the election lease: a plain object at
// "state/<cluster>" next to "leases/<cluster>". No S3, no extra process —
// the default self-hosted backend now has durable desired state. Deleting
// that object on the server is the operator's "boot clean" switch, exactly
// like S3StateStore.
type HoplockServerStateStore struct {
	b   *client.Backend
	key string
}

// NewHoplockServerStateStore wires the state object for the given cluster on
// the hoplockserver at serverURL (same URL and API key as the lease backend).
func NewHoplockServerStateStore(serverURL, apiKey, clusterName string) *HoplockServerStateStore {
	key := "state/" + clusterName
	return &HoplockServerStateStore{
		b:   &client.Backend{URL: serverURL, Key: key, APIKey: apiKey},
		key: key,
	}
}

// Save overschrijft de snapshot (enige schrijver: de leaseholder).
func (s *HoplockServerStateStore) Save(ctx context.Context, snapshot []byte) error {
	return s.b.PutObject(ctx, s.key, snapshot, "application/json")
}

// Load leest de snapshot; ok=false = geen object = schone boot.
func (s *HoplockServerStateStore) Load(ctx context.Context) ([]byte, bool, error) {
	return s.b.GetObject(ctx, s.key)
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
		// Surface the backend error so operators can tell a transient
		// network blip from a real ErrLeaseHeld / 412 PreconditionFailed.
		log.Printf("lock refresh failed (prevHandle=%q, gen=%d): %v", prevHandle, gen, err)
		return false
	}
	d.mu.Lock()
	d.handle = newHandle
	d.mu.Unlock()
	return true
}
