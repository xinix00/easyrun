package discovery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDiscoveryNew(t *testing.T) {
	d := New("test-cluster", "192.168.1.10", 8080, []string{"http://raft:8080"}, 30*time.Second)

	if d.clusterName != "test-cluster" {
		t.Errorf("clusterName = %q, want %q", d.clusterName, "test-cluster")
	}
	if d.nodeAddr != "192.168.1.10:8080" {
		t.Errorf("nodeAddr = %q, want %q", d.nodeAddr, "192.168.1.10:8080")
	}
	if d.raftEndpoint != "http://raft:8080" {
		t.Errorf("raftEndpoint = %q, want %q", d.raftEndpoint, "http://raft:8080")
	}
}

func TestDiscoveryNewEmptyEndpoints(t *testing.T) {
	d := New("test-cluster", "192.168.1.10", 8080, []string{}, 30*time.Second)

	if d.raftEndpoint != "" {
		t.Errorf("raftEndpoint = %q, want empty", d.raftEndpoint)
	}
}

func TestDiscoveryNodeAddr(t *testing.T) {
	d := New("test-cluster", "192.168.1.10", 8080, nil, 30*time.Second)

	if d.NodeAddr() != "192.168.1.10:8080" {
		t.Errorf("NodeAddr() = %q, want %q", d.NodeAddr(), "192.168.1.10:8080")
	}
}

func TestDiscoveryGetLeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/leader/test-cluster" && r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(map[string]string{"leader": "192.168.1.20:8080"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	leader := d.GetLeader()
	if leader != "192.168.1.20:8080" {
		t.Errorf("GetLeader() = %q, want %q", leader, "192.168.1.20:8080")
	}
}

func TestDiscoveryGetLeaderNoEndpoint(t *testing.T) {
	d := New("test-cluster", "192.168.1.10", 8080, []string{}, 30*time.Second)

	leader := d.GetLeader()
	if leader != "" {
		t.Errorf("GetLeader() = %q, want empty", leader)
	}
}

func TestDiscoveryGetLeaderNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	leader := d.GetLeader()
	if leader != "" {
		t.Errorf("GetLeader() = %q, want empty", leader)
	}
}

func TestDiscoveryTryBecomeLeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/leader/test-cluster" && r.Method == http.MethodPost {
			var req map[string]any
			json.NewDecoder(r.Body).Decode(&req)

			if req["ip"] == "192.168.1.10:8080" {
				json.NewEncoder(w).Encode(map[string]bool{"success": true})
				return
			}
		}
		json.NewEncoder(w).Encode(map[string]bool{"success": false})
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	if !d.TryBecomeLeader() {
		t.Error("TryBecomeLeader() should return true")
	}
}

func TestDiscoveryTryBecomeLeaderNoEndpoint(t *testing.T) {
	d := New("test-cluster", "192.168.1.10", 8080, []string{}, 30*time.Second)

	if d.TryBecomeLeader() {
		t.Error("TryBecomeLeader() should return false without endpoint")
	}
}

func TestDiscoveryTryBecomeLeaderDenied(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]bool{"success": false})
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	if d.TryBecomeLeader() {
		t.Error("TryBecomeLeader() should return false when denied")
	}
}

func TestDiscoveryReleaseLeadership(t *testing.T) {
	released := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/leader/test-cluster" && r.Method == http.MethodDelete {
			released = true
			json.NewEncoder(w).Encode(map[string]bool{"released": true})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)
	d.ReleaseLeadership()

	if !released {
		t.Error("ReleaseLeadership() should call DELETE /leader/{cluster}")
	}
}

func TestDiscoveryReleaseLeadershipNoEndpoint(t *testing.T) {
	d := New("test-cluster", "192.168.1.10", 8080, []string{}, 30*time.Second)
	// Should not panic
	d.ReleaseLeadership()
}

func TestDiscoveryIsLeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"leader": "192.168.1.10:8080"})
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	if !d.IsLeader() {
		t.Error("IsLeader() should return true when we are leader")
	}
}

func TestDiscoveryIsLeaderFalse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"leader": "192.168.1.20:8080"})
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	if d.IsLeader() {
		t.Error("IsLeader() should return false when someone else is leader")
	}
}

func TestDiscoveryRenewLease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			json.NewEncoder(w).Encode(map[string]bool{"success": true})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	d := New("test-cluster", "192.168.1.10", 8080, []string{server.URL}, 30*time.Second)

	if !d.RenewLease() {
		t.Error("RenewLease() should return true")
	}
}
