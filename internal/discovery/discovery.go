package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const httpClientTimeout = 5 * time.Second

// Discovery handles leader election via easyraft
// easyraft manages all the complexity - we just ask it who's leader
type Discovery struct {
	clusterName  string
	nodeAddr     string
	raftEndpoint string
	leaderLease  time.Duration
	httpClient   *http.Client
}

// New creates a new Discovery instance
func New(clusterName, nodeIP string, nodePort int, raftEndpoints []string, leaderLease time.Duration) *Discovery {
	// Just use the first endpoint - easyraft handles its own HA
	endpoint := ""
	if len(raftEndpoints) > 0 {
		endpoint = raftEndpoints[0]
	}

	return &Discovery{
		clusterName:  clusterName,
		nodeAddr:     fmt.Sprintf("%s:%d", nodeIP, nodePort),
		raftEndpoint: endpoint,
		leaderLease:  leaderLease,
		httpClient:   &http.Client{Timeout: httpClientTimeout},
	}
}

// GetLeader returns the current leader for this cluster
func (d *Discovery) GetLeader() string {
	if d.raftEndpoint == "" {
		return ""
	}

	url := fmt.Sprintf("%s/leader/%s", d.raftEndpoint, d.clusterName)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return ""
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	var result struct {
		Leader string `json:"leader"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}

	return result.Leader
}

// TryBecomeLeader attempts to claim leadership
func (d *Discovery) TryBecomeLeader() bool {
	if d.raftEndpoint == "" {
		return false
	}

	body, _ := json.Marshal(map[string]any{
		"ip":          d.nodeAddr,
		"ttl_seconds": int(d.leaderLease.Seconds()),
	})

	url := fmt.Sprintf("%s/leader/%s", d.raftEndpoint, d.clusterName)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var result struct {
		Success bool `json:"success"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Success
}

// ReleaseLeadership releases the leader claim
func (d *Discovery) ReleaseLeadership() {
	if d.raftEndpoint == "" {
		return
	}

	body, _ := json.Marshal(map[string]string{"ip": d.nodeAddr})
	url := fmt.Sprintf("%s/leader/%s", d.raftEndpoint, d.clusterName)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// RenewLease renews the leader lease (same as TryBecomeLeader)
func (d *Discovery) RenewLease() bool {
	return d.TryBecomeLeader()
}

// IsLeader returns true if this node is the current leader
func (d *Discovery) IsLeader() bool {
	return d.GetLeader() == d.nodeAddr
}

// NodeAddr returns this node's address
func (d *Discovery) NodeAddr() string {
	return d.nodeAddr
}
