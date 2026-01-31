package lease

import (
	"sync"
	"time"
)

// Lease represents a leader claim for a cluster
type Lease struct {
	Cluster  string    `json:"cluster"`
	LeaderIP string    `json:"leader"`
	Expires  time.Time `json:"expires"`
}

// IsExpired returns true if the lease has expired
func (l *Lease) IsExpired() bool {
	return time.Now().After(l.Expires)
}

// Manager manages leases for multiple clusters
type Manager struct {
	leases sync.Map // map[cluster]*Lease
}

// NewManager creates a new lease manager
func NewManager() *Manager {
	return &Manager{}
}

// Claim attempts to claim or renew a lease for a cluster
// Returns the lease and whether the claim was successful
func (m *Manager) Claim(cluster, ip string, ttl time.Duration) (*Lease, bool) {
	expires := time.Now().Add(ttl)

	// Try to load existing lease
	if existing, ok := m.leases.Load(cluster); ok {
		lease := existing.(*Lease)

		// Allow if: same IP (renew) or expired
		if lease.LeaderIP == ip || lease.IsExpired() {
			lease.LeaderIP = ip
			lease.Expires = expires
			return lease, true
		}

		// Different IP and not expired - deny
		return lease, false
	}

	// No existing lease - create new
	lease := &Lease{
		Cluster:  cluster,
		LeaderIP: ip,
		Expires:  expires,
	}
	m.leases.Store(cluster, lease)
	return lease, true
}

// Get returns the lease for a cluster if it exists and is not expired
func (m *Manager) Get(cluster string) *Lease {
	if existing, ok := m.leases.Load(cluster); ok {
		lease := existing.(*Lease)
		if !lease.IsExpired() {
			return lease
		}
	}
	return nil
}

// Release releases a lease if held by the given IP
func (m *Manager) Release(cluster, ip string) bool {
	if existing, ok := m.leases.Load(cluster); ok {
		lease := existing.(*Lease)
		if lease.LeaderIP == ip {
			m.leases.Delete(cluster)
			return true
		}
	}
	return false
}

// GetAll returns all active (non-expired) leases
func (m *Manager) GetAll() []*Lease {
	var leases []*Lease
	m.leases.Range(func(key, value any) bool {
		lease := value.(*Lease)
		if !lease.IsExpired() {
			leases = append(leases, lease)
		}
		return true
	})
	return leases
}
