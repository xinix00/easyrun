package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"easyrun/internal/types"
	"easyrun/pkg/config"
)

func TestMatchesAffinity(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	// Agent has auto-detected: node.id=node-1, node.arch=<runtime>, node.os=<runtime>
	tests := []struct {
		name     string
		affinity map[string]string
		want     bool
	}{
		{"nil affinity", nil, true},
		{"empty affinity", map[string]string{}, true},
		{"match node.id", map[string]string{"node.id": "node-1"}, true},
		{"mismatch node.id", map[string]string{"node.id": "node-2"}, false},
		{"missing key", map[string]string{"gpu": "true"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agent.matchesAffinity(tt.affinity); got != tt.want {
				t.Errorf("matchesAffinity(%v) = %v, want %v (attrs=%v)", tt.affinity, got, tt.want, agent.attributes)
			}
		})
	}
}

func TestMatchesAffinityWithConfigAttributes(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{
		"region": "eu-west-1",
		"gpu":    "true",
	}
	agent := New(cfg, "node-1", NewMockRunner())

	tests := []struct {
		name     string
		affinity map[string]string
		want     bool
	}{
		{"match custom attr", map[string]string{"region": "eu-west-1"}, true},
		{"match multiple", map[string]string{"region": "eu-west-1", "gpu": "true"}, true},
		{"mismatch custom", map[string]string{"region": "us-east-1"}, false},
		{"mix auto+custom", map[string]string{"node.id": "node-1", "gpu": "true"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := agent.matchesAffinity(tt.affinity); got != tt.want {
				t.Errorf("matchesAffinity(%v) = %v, want %v", tt.affinity, got, tt.want)
			}
		})
	}
}

func TestConfigOverridesAutoDetected(t *testing.T) {
	cfg := testConfig()
	cfg.Node.Attributes = map[string]string{
		"node.id": "custom-id", // override auto-detected
	}
	agent := New(cfg, "auto-id", NewMockRunner())

	if agent.attributes["node.id"] != "custom-id" {
		t.Errorf("node.id = %q, want %q (config should override auto-detected)", agent.attributes["node.id"], "custom-id")
	}
}

func TestHandleRunAffinityMismatch(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Job requires node-2, but this agent is node-1
	job := types.Job{
		Name:     "test-job",
		Command:  "echo hello",
		Affinity: map[string]string{"node.id": "node-2"},
	}
	body, _ := json.Marshal(job)

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusNotAcceptable {
		t.Errorf("Status = %d, want %d (406 Not Acceptable)", w.Code, http.StatusNotAcceptable)
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["error"] != "affinity mismatch" {
		t.Errorf("error = %q, want %q", resp["error"], "affinity mismatch")
	}
}

func TestHandleRunAffinityMatch(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Job requires node-1, this agent IS node-1
	job := types.Job{
		Name:     "test-job",
		Command:  "echo hello",
		Affinity: map[string]string{"node.id": "node-1"},
	}
	body, _ := json.Marshal(job)

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Status = %d, want %d (202 Accepted)", w.Code, http.StatusAccepted)
	}
}

func TestHandleRunNoAffinity(t *testing.T) {
	cfg := testConfig()
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Job without affinity should be accepted
	job := types.Job{
		Name:    "test-job",
		Command: "echo hello",
	}
	body, _ := json.Marshal(job)

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Status = %d, want %d (202 Accepted)", w.Code, http.StatusAccepted)
	}
}

func TestCapacityIncludesAttributes(t *testing.T) {
	cfg := &config.Config{
		Node: config.NodeConfig{
			IP:   "127.0.0.1",
			Port: 8080,
			Attributes: map[string]string{
				"region": "eu-west-1",
			},
		},
		Paths: config.PathsConfig{
			RootfsBase: "/tmp/test-easyrun",
			StateFile:  "/tmp/test-easyrun/state.json",
		},
		Capacity: config.CapacityConfig{
			CPUShares: 1000,
			Memory:    1024 * 1024 * 1024,
		},
	}
	agent := New(cfg, "node-1", NewMockRunner())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/capacity", nil)
	w := httptest.NewRecorder()

	agent.handleCapacity(w, req)

	var resp CapacityResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if resp.Attributes["node.id"] != "node-1" {
		t.Errorf("attributes[node.id] = %q, want %q", resp.Attributes["node.id"], "node-1")
	}
	if resp.Attributes["region"] != "eu-west-1" {
		t.Errorf("attributes[region] = %q, want %q", resp.Attributes["region"], "eu-west-1")
	}
}
