// Package agentboot boots a complete single-node HOP agent for HopOS. It is
// the public entry point that hop-os links (internal/agent is not importable
// across modules): wire a hopos.SlotManager in, and out comes a running agent
// with leader API — the same bytes that cmd/agent runs on Linux/macOS.
//
// Fase-1 scaffolding, deliberately standalone-only: in-memory lock backend,
// this node is always the leader. Joining a real cluster (hoplockserver over
// the network) is the fase-2 step; at that point the tick loop in cmd/agent
// (leader election, takeover, fail counting) should be extracted into a
// shared package and replace the simplified loop below.
package agentboot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"hop/internal/agent"
	"hop/internal/api"
	"hop/internal/discovery"
	"hop/internal/leader"
	"hop/internal/runner"
	"hop/internal/types"
	"hop/pkg/config"
	"hop/pkg/hopos"
	"hop/pkg/httputil"
)

// Version is reported to the leader on register/heartbeat; hop-os may
// override it at build time.
var Version = "hopos-dev"

// Options configures the HopOS agent node.
type Options struct {
	Config *config.Config // cluster name, node IP/port, API key
	NodeID string         // stable node ID (HopOS has no disk to persist one)
	Slots  hopos.SlotManager

	// MemoryBytes is the slot memory this node offers; bare metal cannot
	// auto-detect it the way sysinfo_{linux,darwin} do.
	MemoryBytes uint64
}

// Run boots the agent + leader and blocks until ctx is cancelled or the
// agent's HTTP server stops. Init/LoadState are intentionally skipped:
// HopOS boots from a clean slate ("niets is persistent") and the exec/docker
// cleanup paths assume a POSIX filesystem.
func Run(ctx context.Context, o Options) error {
	cfg, sm := o.Config, o.Slots
	if cfg == nil || sm == nil || o.NodeID == "" {
		return errors.New("agentboot: Config, NodeID en Slots zijn verplicht")
	}

	// Node attributes: HopOS identity + core classes, so affinity can target
	// e.g. node.cores.big. User-configured attributes win.
	attrs := map[string]string{
		"node.id":     o.NodeID,
		"node.arch":   runtime.GOARCH,
		"node.os":     "hopos",
		"node.docker": "false",
	}
	classes := map[string]int{}
	for i := 1; i <= sm.NumSlots(); i++ {
		classes[sm.CoreClass(i)]++
	}
	for class, n := range classes {
		attrs["node.cores."+class] = strconv.Itoa(n)
	}
	for k, v := range cfg.Node.Attributes {
		attrs[k] = v
	}
	cfg.Node.Attributes = attrs

	// The HopRunner is both the hop driver and the default runner: on a
	// HopOS node every task is a slot task, and its errors ("exactly one
	// artifact required") beat ExecRunner's runtime failures on a missing
	// POSIX layer.
	hr := runner.NewHopRunner(sm, attrs)
	ag := agent.New(cfg, o.NodeID, hr).WithHopRunner(hr)
	ag.SetSysInfo(agent.SystemInfo{
		CPUCores:    sm.NumSlots(),
		MemoryBytes: o.MemoryBytes,
	})

	// Standalone leader election: in-memory backend, single node — becoming
	// leader cannot fail, and settle (waiting for other agents' placed
	// counts) is pointless on a boot-fresh single node.
	disc := discovery.New(discovery.InMemoryBackend(), cfg.Node.IP, cfg.Node.Port+1000, cfg.Timeouts.LeaderLease)
	ag.SetLeaderFunc(disc.GetLeader)
	if !disc.TryBecomeLeader() {
		return errors.New("agentboot: standalone node kon geen leader worden")
	}

	l := leader.New(o.NodeID, ag, nil)
	l.SetAPIKey(cfg.APIKey)
	if d := cfg.Timeouts.NodeDeadThreshold; d > 0 {
		l.SetAgentTimeout(d)
	}
	go l.Run(ctx)

	srv := api.NewServer(l, fmt.Sprintf(":%d", cfg.Node.Port+1000), cfg.APIKey, cfg.Cluster.Name)
	go func() {
		if err := srv.Run(ctx); err != nil {
			log.Printf("agentboot: leader-API: %v", err)
		}
	}()

	// De agent (state-loop + agent-API) moet draaien vóór registratie:
	// RegisterAgent reconciliet en dat bevraagt de agent-state.
	runErr := make(chan error, 1)
	go func() { runErr <- ag.Run(ctx) }()

	l.RegisterAgent(o.NodeID, ag.Endpoint(), Version, ag.GetPlacedTaskCounts())
	log.Printf("agentboot: node %s is leader (%s), %d slots", o.NodeID, cfg.Cluster.Name, sm.NumSlots())

	// Heartbeat-loop: lease vers houden + job-state syncen met de eigen
	// leader (dezelfde bytes als een remote agent zou sturen).
	go func() {
		leaderAddr := fmt.Sprintf("%s:%d", cfg.Node.IP, cfg.Node.Port+1000)
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				disc.ReleaseLeadership()
				return
			case <-t.C:
			}
			disc.RenewLease()
			resp, err := heartbeat(leaderAddr, o.NodeID, ag.Endpoint(), ag.GetJobs(), ag.GetStateTime(), cfg.APIKey)
			if err != nil {
				log.Printf("agentboot: heartbeat: %v", err)
				continue
			}
			if len(resp.Jobs) > 0 {
				ag.SyncJobs(resp.Jobs, resp.StateTime)
			}
		}
	}()

	return <-runErr
}

var httpClient = &http.Client{Timeout: 5 * time.Second}

type heartbeatResponse struct {
	Status    string       `json:"status"`
	Jobs      []*types.Job `json:"jobs"`
	StateTime time.Time    `json:"state_time"`
}

func heartbeat(leaderAddr, id, endpoint string, jobs []*types.Job, stateTime time.Time, apiKey string) (*heartbeatResponse, error) {
	body, _ := json.Marshal(map[string]any{
		"id": id, "endpoint": endpoint, "version": Version, "jobs": jobs, "state_time": stateTime,
	})
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/v1/heartbeat", leaderAddr), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	httputil.SignRequest(req, apiKey, body)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leader returned %d", resp.StatusCode)
	}
	var result heartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
