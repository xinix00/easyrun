package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"hop/internal/agent"
	"hop/internal/api"
	"hop/internal/discovery"
	"hop/internal/leader"
	"hop/internal/types"
	"hop/pkg/config"

	"github.com/google/uuid"
)

// version is set at build time via -ldflags "-X main.version=v1.0.0"
// Falls back to "dev" for local `go build` without version injection.
var version = "dev"

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeName := flag.String("node", "", "Node name/ID (overrides config file)")
	clusterName := flag.String("cluster", "", "Cluster name (e.g., haas-prod)")
	raftEndpoint := flag.String("raft", "", "HopRaft endpoint (overrides config file)")
	standalone := flag.Bool("standalone", false, "Run without hopraft (single-node mode)")
	apiKey := flag.String("api-key", "", "API key for authentication (overrides config file)")
	flag.Parse()

	// Load config (returns defaults if no file)
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override with flags
	if *clusterName != "" {
		cfg.Cluster.Name = *clusterName
	}
	if *raftEndpoint != "" && !*standalone {
		cfg.Cluster.RaftEndpoints = []string{*raftEndpoint}
	}
	if *apiKey != "" {
		cfg.APIKey = *apiKey
	}
	if *nodeName != "" {
		cfg.Node.ID = *nodeName
	}

	// Validate
	if !*standalone && len(cfg.Cluster.RaftEndpoints) == 0 {
		log.Fatal("No raft endpoint configured.\n  Use --raft <url> or set raft_endpoints in config file.\n  For single-node mode: --standalone")
	}
	if cfg.Cluster.Name == "" {
		log.Fatal("cluster name required (use --cluster or config file)")
	}

	// Use cluster-specific state file to avoid conflicts when running multiple clusters
	if cfg.Paths.StateFile == "./data/state.json" {
		cfg.Paths.StateFile = fmt.Sprintf("./data/state-%s.json", cfg.Cluster.Name)
	}

	// Get or create stable node ID
	nodeID := getOrCreateNodeID(cfg)

	// Auto-detect IP if not set
	if cfg.Node.IP == "" {
		cfg.Node.IP = getOutboundIP()
	}

	log.Printf("Starting hop agent %s", version)
	log.Printf("Node %s on %s:%d", nodeID, cfg.Node.IP, cfg.Node.Port)
	log.Printf("Cluster: %s", cfg.Cluster.Name)
	if *standalone {
		log.Println("Running in standalone mode (no raft)")
	} else {
		log.Printf("Using hopraft: %v", cfg.Cluster.RaftEndpoints)
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
	}()

	run(ctx, cfg, nodeID)
}

// httpClient is reused for all heartbeat/register calls (connection pooling)
var httpClient = &http.Client{Timeout: 5 * time.Second}

func run(ctx context.Context, cfg *config.Config, nodeID string) {
	// Create discovery client for leader election
	disc := discovery.New(
		cfg.Cluster.Name,
		cfg.Node.IP,
		cfg.Node.Port+1000, // Leader API port
		cfg.Cluster.RaftEndpoints,
		cfg.Timeouts.LeaderLease,
	)

	// Create agent (always runs)
	ag := agent.New(cfg, nodeID, nil)

	// Set leader discovery for proxy endpoints
	ag.SetLeaderFunc(disc.GetLeader)

	// Cleanup old task directories (fresh start)
	if err := ag.Init(); err != nil {
		log.Fatalf("Failed to initialize agent: %v", err)
	}

	// Load persisted state (jobs from last run)
	if err := ag.LoadState(); err != nil {
		log.Printf("Warning: failed to load state: %v", err)
	}

	loop := &agentLoop{
		ctx:         ctx,
		cfg:         cfg,
		ag:          ag,
		disc:        disc,
		doRegister:  registerAgent,
		doHeartbeat: sendHeartbeat,
		doBecomeLeader: func() (func(), leaderAPI) {
			var l *leader.Leader
			stop := becomeLeader(ctx, cfg, ag, &l, cfg.APIKey)
			return stop, l
		},
	}

	// Main loop: heartbeat to leader, handle leader election
	go func() {
		loop.tick()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if loop.stopLeader != nil {
					disc.ReleaseLeadership()
				}
				return
			case <-ticker.C:
				loop.tick()
			}
		}
	}()

	// Run agent HTTP server
	if err := ag.Run(ctx); err != nil {
		log.Printf("Agent error: %v", err)
	}
}

func becomeLeader(ctx context.Context, cfg *config.Config, ag *agent.Agent, l **leader.Leader, apiKey string) func() {
	log.Println("Became leader!")

	// Leader gets its own context — cancelled on leadership loss (not just shutdown)
	leaderCtx, cancel := context.WithCancel(ctx)

	// Start leader with settle delay - wait for agents to register with placed counts
	*l = leader.New(ag.ID(), ag, nil)
	(*l).SetAPIKey(apiKey)
	if d := cfg.Timeouts.NodeDeadThreshold; d > 0 {
		(*l).SetAgentTimeout(d)
	}
	(*l).EnableSettle()

	// Start leader state loop + health checker BEFORE any state operations
	// Without this, RegisterAgent deadlocks (query waits on ops channel that nobody reads)
	go (*l).Run(leaderCtx)

	// Start API server so other agents can register/heartbeat
	leaderAddr := fmt.Sprintf(":%d", cfg.Node.Port+1000)
	srv := api.NewServer(*l, leaderAddr, apiKey, cfg.Cluster.Name)
	go func() {
		if err := srv.Run(leaderCtx); err != nil {
			log.Printf("Leader API error: %v", err)
		}
	}()

	// Register self with placed counts (during settle, no reconcile yet)
	(*l).RegisterAgent(ag.ID(), ag.Endpoint(), version, ag.GetPlacedTaskCounts())

	log.Printf("Leader initialized with %d placed jobs from local agent", len(ag.GetPlacedTaskCounts()))

	return func() {
		srv.Stop() // release :9080 synchronously
		cancel()   // stop leader state loop
	}
}

type heartbeatResponse struct {
	Status    string       `json:"status"`
	Jobs      []*types.Job `json:"jobs"`
	StateTime time.Time    `json:"state_time"`
}

// postJSON sends a POST request with JSON body and API key to the leader.
func postJSON(path string, payload any, apiKey string) (*http.Response, error) {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	return httpClient.Do(req)
}

func registerAgent(leaderAddr, agentID, agentEndpoint string, placed map[string]int, apiKey string) error {
	resp, err := postJSON(fmt.Sprintf("http://%s/v1/agents", leaderAddr), map[string]any{
		"id": agentID, "endpoint": agentEndpoint, "version": version, "placed": placed,
	}, apiKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("leader returned %d", resp.StatusCode)
	}
	return nil
}

var errNotRegistered = errors.New("not registered with leader")

func sendHeartbeat(leaderAddr, agentID, agentEndpoint string, jobs []*types.Job, stateTime time.Time, apiKey string) (*heartbeatResponse, error) {
	resp, err := postJSON(fmt.Sprintf("http://%s/v1/heartbeat", leaderAddr), map[string]any{
		"id": agentID, "endpoint": agentEndpoint, "version": version, "jobs": jobs, "state_time": stateTime,
	}, apiKey)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errNotRegistered
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leader returned %d", resp.StatusCode)
	}

	var result heartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// getOrCreateNodeID returns a stable node ID (config > persisted > generated)
func getOrCreateNodeID(cfg *config.Config) string {
	// 1. Config takes priority (user override)
	if cfg.Node.ID != "" {
		return cfg.Node.ID
	}

	// 2. Check persisted ID
	stateDir := filepath.Dir(cfg.Paths.StateFile)
	idFile := filepath.Join(stateDir, "node-id")
	if data, err := os.ReadFile(idFile); err == nil {
		id := string(bytes.TrimSpace(data))
		if id != "" {
			log.Printf("Using persisted node ID: %s", id)
			return id
		}
	}

	// 3. Generate new ID and persist
	nodeID := uuid.New().String()[:8]
	_ = os.MkdirAll(stateDir, 0755)
	if err := os.WriteFile(idFile, []byte(nodeID), 0644); err != nil {
		log.Printf("Warning: failed to persist node ID: %v", err)
	} else {
		log.Printf("Generated and persisted new node ID: %s", nodeID)
	}

	return nodeID
}

func getOutboundIP() string {
	for {
		conn, err := net.Dial("udp", "8.8.8.8:80")
		if err != nil {
			log.Println("Waiting for network...")
			time.Sleep(2 * time.Second)
			continue
		}
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		conn.Close()
		return localAddr.IP.String()
	}
}
