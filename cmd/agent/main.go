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

	"easyrun/internal/agent"
	"easyrun/internal/api"
	"easyrun/internal/discovery"
	"easyrun/internal/leader"
	"easyrun/internal/types"
	"easyrun/pkg/config"

	"github.com/google/uuid"
)

const version = "v0.5.14" // Agent version - placed in RegisterAgent, not heartbeat

func main() {
	configPath := flag.String("config", "", "Path to config file")
	clusterName := flag.String("cluster", "", "Cluster name (e.g., easyflor-prod)")
	raftEndpoint := flag.String("raft", "", "EasyRaft endpoint (overrides config file)")
	standalone := flag.Bool("standalone", false, "Run without easyraft (single-node mode)")
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

	log.Printf("Starting easyrun agent %s", version)
	log.Printf("Node %s on %s:%d", nodeID, cfg.Node.IP, cfg.Node.Port)
	log.Printf("Cluster: %s", cfg.Cluster.Name)
	if *standalone {
		log.Println("Running in standalone mode (no raft)")
	} else {
		log.Printf("Using easyraft: %v", cfg.Cluster.RaftEndpoints)
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

	var l *leader.Leader
	var leaderCancel context.CancelFunc // non-nil = we are leader
	var failCount int
	var registered bool      // false on startup → first contact = register with placed counts
	var lastLeaderAddr string // cached leader address — fallback when raft is down

	// Main loop: heartbeat to leader, handle leader election
	go func() {
		tick := func() {
			// Use cached leader, only ask raft when unknown
			leaderAddr := lastLeaderAddr
			if leaderAddr == "" {
				leaderAddr = disc.GetLeader()
			}

			if leaderCancel != nil {
				// We are leader, renew lease AND send heartbeat to ourselves
				if !disc.RenewLease() {
					// Raft unreachable — but are agents still alive?
					agents := l.GetAgents()
					if len(agents) > 0 {
						log.Printf("Raft unreachable but %d agents still connected, staying leader", len(agents))
					} else {
						log.Println("Lost leadership (raft unreachable + no agents)")
						registered = false
						lastLeaderAddr = ""
						leaderCancel()
						leaderCancel = nil
						l = nil
					}
				} else {
					failCount = 0
					leaderAddr = fmt.Sprintf("%s:%d", cfg.Node.IP, cfg.Node.Port+1000)
					sendHeartbeat(leaderAddr, ag.ID(), ag.Endpoint(), ag.GetJobs(), ag.GetStateTime())
				}
			} else if leaderAddr != "" {
				// On startup (or after leader change): register first with placed counts
				if !registered {
					log.Printf("Registering with leader %s...", leaderAddr)
					if err := registerAgent(leaderAddr, ag.ID(), ag.Endpoint(), ag.GetPlacedTaskCounts()); err != nil {
						failCount++
						log.Printf("Register failed (%d): %v", failCount, err)
						if failCount >= 3 {
							lastLeaderAddr = ""
						}
					} else {
						log.Printf("Registered with leader %s", leaderAddr)
						registered = true
						failCount = 0
						lastLeaderAddr = leaderAddr
					}
					return
				}

				// Already registered → heartbeat
				resp, err := sendHeartbeat(leaderAddr, ag.ID(), ag.Endpoint(), ag.GetJobs(), ag.GetStateTime())
				if err != nil {
					if errors.Is(err, errNotRegistered) {
						// Leader doesn't know us (new leader?) → re-register next tick
						log.Printf("Not registered with leader, will re-register...")
						registered = false
						lastLeaderAddr = "" // force raft lookup for new leader
					} else {
						failCount++
						log.Printf("Heartbeat failed (%d): %v", failCount, err)

						// After 3 failures, try to become leader
						if failCount >= 3 {
							log.Println("Leader seems dead, trying to become leader...")
							lastLeaderAddr = ""
							if disc.TryBecomeLeader() {
								becomeLeader(ctx, cfg, ag, &l, &leaderCancel)
								failCount = 0
							}
							// If we couldn't become leader, failCount keeps incrementing
						}
					}
				} else {
					failCount = 0
					lastLeaderAddr = leaderAddr
					// Sync jobs from leader with their state time
					if len(resp.Jobs) > 0 {
						ag.SyncJobs(resp.Jobs, resp.StateTime)
					}
				}
			} else {
				// No leader, try to become one
				failCount++
				log.Printf("No leader found (%d), trying to become leader...", failCount)
				if disc.TryBecomeLeader() {
					becomeLeader(ctx, cfg, ag, &l, &leaderCancel)
					failCount = 0
				}
			}

			// If we've failed too many times, we're probably isolated
			// Kill our tasks to avoid running duplicates
			if failCount >= 6 {
				log.Println("Cannot reach leader or become leader - likely network isolated")
				log.Println("Stopping all tasks to avoid duplicates...")
				ag.StopAllTasks()
				failCount = 3 // Keep trying, but don't spam stop
			}
		}

		// Try immediately
		tick()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if leaderCancel != nil {
					disc.ReleaseLeadership()
				}
				return
			case <-ticker.C:
				tick()
			}
		}
	}()

	// Run agent HTTP server
	if err := ag.Run(ctx); err != nil {
		log.Printf("Agent error: %v", err)
	}
}

func becomeLeader(ctx context.Context, cfg *config.Config, ag *agent.Agent, l **leader.Leader, cancelOut *context.CancelFunc) {
	log.Println("Became leader!")

	// Leader gets its own context — cancelled on leadership loss (not just shutdown)
	leaderCtx, cancel := context.WithCancel(ctx)
	*cancelOut = cancel

	// Start leader with settle delay - wait for agents to register with placed counts
	*l = leader.New(ag.ID(), ag, nil)
	(*l).EnableSettle()

	// Start leader state loop + health checker BEFORE any state operations
	// Without this, RegisterAgent deadlocks (query waits on ops channel that nobody reads)
	go (*l).Run(leaderCtx)

	// Start API server so other agents can register/heartbeat
	leaderAddr := fmt.Sprintf("%s:%d", cfg.Node.IP, cfg.Node.Port+1000)
	srv := api.NewServer(*l, leaderAddr)
	go func() {
		if err := srv.Run(leaderCtx); err != nil {
			log.Printf("Leader API error: %v", err)
		}
	}()

	// Register self with placed counts (during settle, no reconcile yet)
	(*l).RegisterAgent(ag.ID(), ag.Endpoint(), version, ag.GetPlacedTaskCounts())

	log.Printf("Leader initialized with %d placed jobs from local agent", len(ag.GetPlacedTaskCounts()))
}

type heartbeatResponse struct {
	Status    string       `json:"status"`
	Jobs      []*types.Job `json:"jobs"`
	StateTime time.Time    `json:"state_time"`
}

func registerAgent(leaderAddr, agentID, agentEndpoint string, placed map[string]int) error {
	url := fmt.Sprintf("http://%s/v1/agents", leaderAddr)

	body, _ := json.Marshal(map[string]any{
		"id":       agentID,
		"endpoint": agentEndpoint,
		"version":  version,
		"placed":   placed, // jobID -> count (what's running on this agent)
	})

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
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

func sendHeartbeat(leaderAddr, agentID, agentEndpoint string, jobs []*types.Job, stateTime time.Time) (*heartbeatResponse, error) {
	url := fmt.Sprintf("http://%s/v1/heartbeat", leaderAddr)

	body, _ := json.Marshal(map[string]any{
		"id":         agentID,
		"endpoint":   agentEndpoint,
		"version":    version,
		"jobs":       jobs,      // All known jobs (for state sync)
		"state_time": stateTime,
	})

	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
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
	os.MkdirAll(stateDir, 0755)
	if err := os.WriteFile(idFile, []byte(nodeID), 0644); err != nil {
		log.Printf("Warning: failed to persist node ID: %v", err)
	} else {
		log.Printf("Generated and persisted new node ID: %s", nodeID)
	}

	return nodeID
}

func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
