package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xinix00/hop/internal/agent"
	"github.com/xinix00/hop/internal/agentloop"
	"github.com/xinix00/hop/internal/api"
	"github.com/xinix00/hop/internal/discovery"
	"github.com/xinix00/hop/internal/leader"
	"github.com/xinix00/hop/pkg/config"

	"github.com/google/uuid"
	"github.com/xinix00/hoplock"
)

// version is set at build time via -ldflags "-X main.version=v1.0.0"
// Falls back to "dev" for local `go build` without version injection.
var version = "dev"

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeName := flag.String("node", "", "Node name/ID (overrides config file)")
	clusterName := flag.String("cluster", "", "Cluster name (e.g., haas-prod)")
	lockURL := flag.String("lock", "", "hoplockserver URL (overrides config file)")
	standalone := flag.Bool("standalone", false, "Run without a lock backend (single-node mode)")
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
	if *lockURL != "" && !*standalone {
		cfg.Cluster.Lock.Type = "hoplockserver"
		cfg.Cluster.Lock.URL = *lockURL
	}
	if *apiKey != "" {
		cfg.APIKey = *apiKey
	}
	if *nodeName != "" {
		cfg.Node.ID = *nodeName
	}

	// No lock backend configured → run standalone (in-memory). Allows
	// hop to be useful with zero config beyond the cluster name.
	if !*standalone && !lockConfigured(cfg.Cluster.Lock) {
		*standalone = true
	}
	if cfg.Cluster.Name == "" {
		log.Fatal("cluster name required (use --cluster or config file)")
	}
	// Validate init_jobs up front: a config typo should stop the agent at
	// boot, not surface at the first clean-boot leader takeover.
	if _, err := leader.DecodeInitJobs(cfg.Cluster.InitJobs); err != nil {
		log.Fatalf("cluster.init_jobs: %v", err)
	}

	// Use cluster-specific state file to avoid conflicts when running multiple clusters
	if cfg.Paths.StateFile == "./data/state.json" {
		cfg.Paths.StateFile = fmt.Sprintf("./data/state-%s.json", cfg.Cluster.Name)
	}

	// Get or create stable node ID
	nodeID := getOrCreateNodeID(cfg)

	// Auto-detect IP if not set. If node.network is set, pick the first
	// interface IP inside that CIDR (useful for multi-homed hosts that need
	// to advertise on a specific LAN/VPN instead of the default-route
	// interface). Otherwise fall back to default-route detection.
	if cfg.Node.IP == "" {
		if cfg.Node.Network != "" {
			ip, err := pickIPInNetwork(cfg.Node.Network)
			if err != nil {
				log.Fatalf("node.network %q: %v", cfg.Node.Network, err)
			}
			cfg.Node.IP = ip
			log.Printf("Picked %s from node.network %s", ip, cfg.Node.Network)
		} else {
			cfg.Node.IP = getOutboundIP()
		}
	}

	log.Printf("Starting hop agent %s", version)
	log.Printf("Node %s on %s:%d", nodeID, cfg.Node.IP, cfg.Node.Port)
	log.Printf("Cluster: %s", cfg.Cluster.Name)
	if *standalone {
		log.Println("Running in standalone mode (in-memory lock backend, single-node)")
	} else {
		log.Printf("Lock backend: %s", lockLabel(cfg.Cluster.Lock))
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

	run(ctx, cfg, nodeID, *standalone)
}

func run(ctx context.Context, cfg *config.Config, nodeID string, standalone bool) {
	backend, err := buildBackend(cfg, standalone)
	if err != nil {
		log.Fatalf("Lock backend: %v", err)
	}

	// Create discovery client for leader election
	disc := discovery.New(
		backend,
		cfg.Node.IP,
		cfg.Node.Port+1000, // Leader API port
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

	loop := &agentloop.Loop{
		Cfg:  cfg,
		Ag:   ag,
		Disc: disc,
		DoRegister: func(leaderAddr, id, ep string, placed map[string]int, key string) error {
			return agentloop.Register(leaderAddr, id, ep, version, placed, key)
		},
		DoHeartbeat: func(leaderAddr, id, ep, key string) error {
			return agentloop.Heartbeat(leaderAddr, id, ep, version, key)
		},
		DoBecomeLeader: func() (func(), agentloop.LeaderAPI) {
			var l *leader.Leader
			stop := becomeLeader(ctx, cfg, ag, &l, cfg.APIKey, standalone)
			return stop, l
		},
	}

	// Main loop: heartbeat to leader, handle leader election (gedeeld met
	// agentboot — internal/agentloop, de fase-2-extractie).
	go loop.Run(ctx.Done(), 10*time.Second)

	// Run agent HTTP server
	if err := ag.Run(ctx); err != nil {
		log.Printf("Agent error: %v", err)
	}
}

// lockConfigured reports whether cluster.lock has enough information to
// construct a working backend.
func lockConfigured(c config.LockConfig) bool {
	switch c.Type {
	case "s3":
		return c.S3.Endpoint != "" && c.S3.Bucket != ""
	case "mem":
		return true
	default:
		return c.URL != ""
	}
}

// lockLabel returns a short human-readable description of the configured
// backend for startup logging.
func lockLabel(c config.LockConfig) string {
	switch c.Type {
	case "s3":
		return fmt.Sprintf("s3 (%s/%s)", c.S3.Endpoint, c.S3.Bucket)
	case "mem":
		return "mem (in-process)"
	default:
		return fmt.Sprintf("hoplockserver (%s)", c.URL)
	}
}

// buildBackend translates config into a hoplock.Backend. Standalone mode
// short-circuits to an in-memory backend regardless of what is configured.
func buildBackend(cfg *config.Config, standalone bool) (hoplock.Backend, error) {
	if standalone {
		return discovery.InMemoryBackend(), nil
	}
	c := cfg.Cluster.Lock
	switch c.Type {
	case "mem":
		return discovery.InMemoryBackend(), nil
	case "s3":
		return discovery.S3Backend(discovery.S3BackendConfig{
			Endpoint:        c.S3.Endpoint,
			Bucket:          c.S3.Bucket,
			Region:          c.S3.Region,
			AccessKeyID:     c.S3.AccessKeyID,
			SecretAccessKey: c.S3.SecretAccessKey,
			SessionToken:    c.S3.SessionToken,
			UsePathStyle:    c.S3.UsePathStyle,
		}, cfg.Cluster.Name), nil
	case "", "hoplockserver":
		return discovery.HoplockServerBackend(c.URL, c.APIKey, cfg.Cluster.Name), nil
	default:
		return nil, fmt.Errorf("unknown lock type %q (want one of: hoplockserver, s3, mem)", c.Type)
	}
}

func becomeLeader(ctx context.Context, cfg *config.Config, ag *agent.Agent, l **leader.Leader, apiKey string, standalone bool) func() {
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

	// Gecommitte clusterstaat: bij bruikbare S3-config commit deze leader
	// zijn gewenste staat naast de lease en laadt een verse leader hem
	// terug — failover zonder state-merging (zelfde gate als agentboot,
	// zie discovery.StateStoreFromConfig).
	cleanBoot := true
	if st := discovery.StateStoreFromConfig(cfg, standalone); st != nil {
		(*l).SetStatePersister(st)
		loaded, err := (*l).LoadCommittedState(leaderCtx)
		if err != nil {
			log.Printf("committed state not loaded: %v", err)
			// Store onbereikbaar ≠ store leeg: nooit seeden op een storing.
			cleanBoot = false
		} else {
			cleanBoot = !loaded
		}
	}

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

	// Clean boot (geen snapshot én lege job store) → seed de init jobs.
	// Tijdens settle worden ze alleen gestored; reconcile dispatcht daarna.
	if cleanBoot && len(cfg.Cluster.InitJobs) > 0 && len((*l).GetJobs()) == 0 {
		jobs, err := leader.DecodeInitJobs(cfg.Cluster.InitJobs)
		if err != nil {
			log.Printf("cluster.init_jobs: %v", err) // al gevalideerd bij boot; hooguit theoretisch
		} else {
			(*l).SeedInitJobs(jobs)
		}
	}

	return func() {
		srv.Stop() // release :9080 synchronously
		cancel()   // stop leader state loop
	}
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

// pickIPInNetwork enumerates local interfaces and returns the first IPv4
// address that falls inside the given CIDR. Retries on transient failures so
// boot ordering against a not-yet-configured NIC doesn't crash the agent.
func pickIPInNetwork(cidr string) (string, error) {
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", fmt.Errorf("invalid CIDR: %w", err)
	}
	for attempt := 0; ; attempt++ {
		addrs, err := net.InterfaceAddrs()
		if err == nil {
			for _, a := range addrs {
				ipNet, ok := a.(*net.IPNet)
				if !ok {
					continue
				}
				ip4 := ipNet.IP.To4()
				if ip4 == nil {
					continue
				}
				if ipnet.Contains(ip4) {
					return ip4.String(), nil
				}
			}
		}
		if attempt >= 15 {
			return "", fmt.Errorf("no interface IP found in %s", cidr)
		}
		log.Printf("Waiting for interface with IP in %s...", cidr)
		time.Sleep(2 * time.Second)
	}
}
