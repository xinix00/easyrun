package main

import (
	"bytes"
	"context"
	"encoding/json"
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

func main() {
	configPath := flag.String("config", "", "Path to config file")
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Validate raft endpoints
	if len(cfg.Cluster.RaftEndpoints) == 0 {
		log.Fatal("raft_endpoints is required in config")
	}

	// Get or create stable node ID
	nodeID := getOrCreateNodeID(cfg)

	// Auto-detect IP if not set
	if cfg.Node.IP == "" {
		cfg.Node.IP = getOutboundIP()
	}

	log.Printf("Starting node %s on %s:%d", nodeID, cfg.Node.IP, cfg.Node.Port)
	log.Printf("Using easyraft endpoints: %v", cfg.Cluster.RaftEndpoints)

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

	var leaderSrv *api.Server
	var l *leader.Leader
	var isLeader bool
	var failCount int

	// Main loop: heartbeat to leader, handle leader election
	go func() {
		tick := func() {
			// Always try to find/contact leader
			leaderAddr := disc.GetLeader()

			if isLeader {
				// We are leader, renew lease
				if !disc.RenewLease() {
					log.Println("Lost leadership")
					isLeader = false
					if leaderSrv != nil {
						leaderSrv.Stop()
						leaderSrv = nil
					}
					l = nil
				} else {
					failCount = 0
				}
			} else if leaderAddr != "" {
				// Send heartbeat to leader with our jobs and state time
				resp, err := sendHeartbeat(leaderAddr, ag.ID(), ag.Endpoint(), ag.GetJobs(), ag.GetStateTime())
				if err != nil {
					failCount++
					log.Printf("Heartbeat failed (%d): %v", failCount, err)

					// After 3 failures, try to become leader
					if failCount >= 3 {
						log.Println("Leader seems dead, trying to become leader...")
						if disc.TryBecomeLeader() {
							becomeLeader(ctx, cfg, ag, &l, &leaderSrv, &isLeader)
							failCount = 0
						}
						// If we couldn't become leader, failCount keeps incrementing
					}
				} else {
					failCount = 0
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
					becomeLeader(ctx, cfg, ag, &l, &leaderSrv, &isLeader)
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
				if isLeader {
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

func becomeLeader(ctx context.Context, cfg *config.Config, ag *agent.Agent, l **leader.Leader, srv **api.Server, isLeader *bool) {
	log.Println("Became leader!")
	*isLeader = true

	// Start leader - it shares the agent's job store directly!
	// No bootstrapping needed - the agent already has all our jobs
	*l = leader.New(ag.ID(), ag, nil)

	log.Printf("Leader initialized with %d jobs from local agent", len(ag.GetJobs()))

	// Start leader health check loop
	go (*l).Run(ctx)

	// Start API server on leader port
	leaderAddr := fmt.Sprintf("%s:%d", cfg.Node.IP, cfg.Node.Port+1000)
	*srv = api.NewServer(*l, leaderAddr)
	go func() {
		if err := (*srv).Run(ctx); err != nil {
			log.Printf("Leader API error: %v", err)
		}
	}()
}

type heartbeatResponse struct {
	Status    string       `json:"status"`
	Jobs      []*types.Job `json:"jobs"`
	StateTime time.Time    `json:"state_time"`
}

func sendHeartbeat(leaderAddr, agentID, agentEndpoint string, jobs []*types.Job, stateTime time.Time) (*heartbeatResponse, error) {
	url := fmt.Sprintf("http://%s/v1/heartbeat", leaderAddr)

	body, _ := json.Marshal(map[string]any{
		"id":         agentID,
		"endpoint":   agentEndpoint,
		"jobs":       jobs,
		"state_time": stateTime,
	})

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
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
