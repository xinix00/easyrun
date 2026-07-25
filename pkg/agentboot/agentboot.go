// Package agentboot boots a complete single-node HOP agent for HopOS. It is
// the public entry point that hop-os links (internal/agent is not importable
// across modules): wire a hopos.SlotManager in, and out comes a running agent
// with leader API — the same bytes that cmd/agent runs on Linux/macOS.
//
// Sinds 20-07 draait hier de gedeelde election-lus (internal/agentloop, de
// fase-2-extractie uit cmd/agent): zonder S3-config is het gedrag het oude
// standalone (in-memory lock, deze node wordt meteen leader); mét
// hopos.s3.*-config doet de node echte leader-election via de S3-lock —
// zelfde S3 + eigen naam + zelfde API key = meedoen aan het cluster, precies
// zoals de Linux-agent.
package agentboot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"time"

	"hop/internal/agent"
	"hop/internal/agentloop"
	"hop/internal/api"
	"hop/internal/discovery"
	"hop/internal/leader"
	"hop/internal/runner"
	"hop/pkg/config"
	"hop/pkg/hopos"
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

// Run boots the agent (+ leader zodra deze node de election wint) and blocks
// until ctx is cancelled or the agent's HTTP server stops. Init is
// intentionally skipped: HopOS boots from a clean slate ("niets is
// persistent") and the exec/docker cleanup paths assume a POSIX filesystem.
// Desired state, when durable, lives in the leader's StatePersister.
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
	for i := 1; i <= sm.NumCores(); i++ {
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
		CPUCores:    sm.NumCores(),
		MemoryBytes: o.MemoryBytes,
	})

	// Lock-backend: S3 als de boot-config hem draagt (echte election, zelfde
	// mechaniek als cmd/agent), anders het oude standalone in-memory gedrag.
	backend := discovery.InMemoryBackend()
	clustered := cfg.Cluster.Lock.Type == "s3" &&
		cfg.Cluster.Lock.S3.Endpoint != "" && cfg.Cluster.Lock.S3.Bucket != ""
	if clustered {
		s3 := cfg.Cluster.Lock.S3
		backend = discovery.S3Backend(discovery.S3BackendConfig{
			Endpoint:        s3.Endpoint,
			Bucket:          s3.Bucket,
			Region:          s3.Region,
			AccessKeyID:     s3.AccessKeyID,
			SecretAccessKey: s3.SecretAccessKey,
			UsePathStyle:    s3.UsePathStyle,
		}, cfg.Cluster.Name)
		log.Printf("agentboot: leader-election via s3 (%s/%s), cluster %q",
			s3.Endpoint, s3.Bucket, cfg.Cluster.Name)
	}
	disc := discovery.New(backend, cfg.Node.IP, cfg.Node.Port+1000, cfg.Timeouts.LeaderLease)
	ag.SetLeaderFunc(disc.GetLeader)

	// De agent (state-loop + agent-API) moet draaien vóór registratie:
	// RegisterAgent reconciliet en dat bevraagt de agent-state.
	runErr := make(chan error, 1)
	go func() { runErr <- ag.Run(ctx) }()

	// becomeLeader — de leader-helft, alleen actief op de election-winnaar:
	// leader + API-server + gecommitte S3-staat + init-seed. Zelfde volgorde
	// als cmd/agent's becomeLeader; settle alleen in cluster-modus (op een
	// standalone boot-verse node valt er niets te settlen — oude gedrag).
	becomeLeader := func() (func(), agentloop.LeaderAPI) {
		leaderCtx, cancel := context.WithCancel(ctx)
		l := leader.New(o.NodeID, ag, nil)
		l.SetAPIKey(cfg.APIKey)
		if d := cfg.Timeouts.NodeDeadThreshold; d > 0 {
			l.SetAgentTimeout(d)
		}
		if clustered {
			l.EnableSettle()
		}
		// Gecommitte clusterstaat (Derek, 15-07): met bruikbare S3-config
		// commit de leader zijn gewenste staat als object "state/<cluster>"
		// naast de lease en laadt hij hem bij boot terug — een reboot
		// herplaatst de jobs (declaratief). Object weghalen = schoon booten.
		cleanBoot := true
		// The persister follows the lock backend: S3 in cluster mode, else a
		// local crash-safe file (!clustered). agentboot only does in-memory or
		// S3 election, so a hoplockserver URL in the config must NOT become a
		// shared remote store here (independent in-memory leaders would clobber
		// it) — passing !clustered forces the local file for that case.
		if st := discovery.StateStoreFromConfig(cfg, !clustered); st != nil {
			l.SetStatePersister(st)
			loaded, err := l.LoadCommittedState(leaderCtx)
			if err != nil {
				// Luid maar niet fataal; store onbereikbaar ≠ store leeg:
				// nooit init jobs seeden op een storing.
				log.Printf("agentboot: committed state not loaded: %v", err)
				cleanBoot = false
			} else {
				cleanBoot = !loaded
			}
		}
		go l.Run(leaderCtx)

		srv := api.NewServer(l, fmt.Sprintf(":%d", cfg.Node.Port+1000), cfg.APIKey, cfg.Cluster.Name)
		go func() {
			if err := srv.Run(leaderCtx); err != nil {
				log.Printf("agentboot: leader-API: %v", err)
			}
		}()

		l.RegisterAgent(o.NodeID, ag.Endpoint(), Version, ag.GetPlacedTaskCounts())
		log.Printf("agentboot: node %s is leader (%s), %d app cores", o.NodeID, cfg.Cluster.Name, sm.NumCores())

		// Clean boot (geen snapshot én lege store — HopOS heeft niets
		// lokaals) → seed de init jobs: zo komt een kale node uit de doos
		// met zijn baseline. In cluster-modus stored settle ze eerst;
		// reconcile dispatcht daarna.
		if cleanBoot && len(cfg.Cluster.InitJobs) > 0 && len(l.GetJobs()) == 0 {
			jobs, err := leader.DecodeInitJobs(cfg.Cluster.InitJobs)
			if err != nil {
				log.Printf("agentboot: cluster.init_jobs: %v", err)
			} else {
				l.SeedInitJobs(jobs)
			}
		}
		return func() {
			srv.Stop() // release :9080 synchronously
			cancel()   // stop leader state loop
		}, l
	}

	// De gedeelde election/heartbeat-lus (internal/agentloop): word leader
	// via de lock, of registreer/heartbeat bij de gevonden leader; takeover
	// en isolatie-gedrag zitten erin. Standalone (in-memory) wint de eerste
	// tick de election — het oude "deze node is altijd leader".
	loop := &agentloop.Loop{
		Cfg:  cfg,
		Ag:   ag,
		Disc: disc,
		DoRegister: func(leaderAddr, id, ep string, placed map[string]int, key string) error {
			return agentloop.Register(leaderAddr, id, ep, Version, placed, key)
		},
		DoHeartbeat: func(leaderAddr, id, ep, key string) error {
			return agentloop.Heartbeat(leaderAddr, id, ep, Version, key)
		},
		DoBecomeLeader: becomeLeader,
	}
	// Eén directe poging bij boot: lock vrij (standalone, of de eerste van
	// een verse cluster) = meteen leader — geen 30s takeover-drempel voor de
	// init-desktop; lock bezet = tick 1 registreert bij de zittende leader.
	loop.BecomeLeaderNow()
	go loop.Run(ctx.Done(), 10*time.Second)

	return <-runErr
}
