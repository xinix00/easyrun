// Package agentloop is de gedeelde election/heartbeat-lus van een hop-node
// (geëxtraheerd uit cmd/agent — de fase-2-stap uit de agentboot-doc):
// probeer leader te worden via het lock-backend, of vind de leader en
// registreer/heartbeat daar. cmd/agent (Linux) en pkg/agentboot (HopOS)
// draaien hierdoor exact dezelfde lus — "zelfde S3, eigen naam, zelfde
// API key" is alles wat een node nodig heeft om mee te doen.
package agentloop

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/xinix00/hop/pkg/httputil"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/config"
)

// Discoverer handles leader election queries.
type Discoverer interface {
	GetLeader() string
	TryBecomeLeader() bool
	RenewLease() (renewed, displaced bool)
	ReleaseLeadership()
}

// AgentAPI is the subset of agent.Agent used in the tick loop.
type AgentAPI interface {
	ID() string
	Endpoint() string
	GetPlacedTaskCounts() map[string]int
	StopAllTasks()
}

// LeaderAPI is the subset of leader.Leader used in the tick loop.
type LeaderAPI interface {
	GetAgents() []*types.Agent
}

// Loop holds all mutable state and injected dependencies for the tick loop.
type Loop struct {
	Cfg  *config.Config
	Ag   AgentAPI
	Disc Discoverer

	// injectable for testing
	DoRegister     func(leaderAddr, agentID, agentEndpoint string, placed map[string]int, apiKey string) error
	DoHeartbeat    func(leaderAddr, agentID, agentEndpoint, apiKey string) error
	DoBecomeLeader func() (stop func(), l LeaderAPI)

	// mutable state
	l              LeaderAPI
	stopLeader     func()
	failCount      int
	registered     bool
	lastLeaderAddr string
}

// BecomeLeaderNow doet één directe election-poging — voor de boot: is de
// lock vrij (verse cluster of standalone in-memory), dan is deze node
// meteen leader in plaats van na de takeover-drempel (~4 ticks); is hij
// bezet, dan vindt de eerstvolgende Tick de leader en registreert daar.
func (s *Loop) BecomeLeaderNow() bool {
	if s.stopLeader != nil {
		return true
	}
	if !s.Disc.TryBecomeLeader() {
		return false
	}
	stop, l := s.DoBecomeLeader()
	s.stopLeader = stop
	s.l = l
	s.failCount = 0
	return true
}

// Run tikt elke interval tot done sluit (eerste tick meteen — registreren
// hoort niet 10s te wachten) en geeft bij het einde de leadership netjes
// terug. Bedoeld als goroutine; vervangt de identieke tickers die cmd/agent
// en agentboot elk zelf hadden.
func (s *Loop) Run(done <-chan struct{}, interval time.Duration) {
	s.Tick()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			if s.stopLeader != nil {
				s.Disc.ReleaseLeadership()
			}
			return
		case <-t.C:
			s.Tick()
		}
	}
}

func (s *Loop) tryTakeOver(reason string) {
	log.Printf("%s, trying to become leader...", reason)
	s.lastLeaderAddr = ""
	if s.Disc.TryBecomeLeader() {
		stop, l := s.DoBecomeLeader()
		s.stopLeader = stop
		s.l = l
		s.failCount = 0
	}
}

func (s *Loop) leaderFailed(format string, args ...any) {
	s.failCount++
	log.Printf(format, args...)
	if s.failCount >= 4 {
		s.tryTakeOver("Leader unreachable")
	}
	if s.failCount >= 7 {
		log.Println("Likely network isolated, stopping all tasks to avoid duplicates")
		s.Ag.StopAllTasks()
		s.failCount = 4
	}
}

func (s *Loop) Tick() {
	// Use cached leader, only ask the lock backend when unknown.
	leaderAddr := s.lastLeaderAddr
	if leaderAddr == "" {
		leaderAddr = s.Disc.GetLeader()
	}

	if s.stopLeader != nil {
		// We are leader — renew the lock lease.
		renewed, displaced := s.Disc.RenewLease()
		switch {
		case renewed:
			s.failCount = 0
		case displaced:
			// The lock store reports another leader — we have genuinely been
			// replaced, not just cut off. Step down NOW, regardless of connected
			// agents, or we would be a second leader writing to the same cluster.
			log.Println("Lost leadership: lock store reports another leader (stepping down)")
			s.registered = false
			s.lastLeaderAddr = ""
			s.stopLeader()
			s.stopLeader = nil
			s.l = nil
			return
		default:
			// Store unreachable (connectivity blip): keep leading while we still
			// see agents. A working LAN survives an internet/lock-store outage
			// without abandoning the cluster — and no one else can take the lease
			// while the store is unreachable to them too. No split-brain.
			agents := s.l.GetAgents()
			if len(agents) > 0 {
				log.Printf("Lock store unreachable but %d agents still connected, staying leader", len(agents))
			} else {
				log.Println("Lost leadership (lock store unreachable + no agents)")
				s.registered = false
				s.lastLeaderAddr = ""
				s.stopLeader()
				s.stopLeader = nil
				s.l = nil
				return
			}
		}
		// Self-heartbeat: puur liveness (LastSeen); job-sync is gesloopt —
		// gewenste staat heeft één auteur (leader → S3, leader/persist.go).
		leaderAddr = fmt.Sprintf("%s:%d", s.Cfg.Node.IP, s.Cfg.Node.Port+1000)
		_ = s.DoHeartbeat(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Cfg.APIKey)
	} else if leaderAddr != "" {
		// On startup (or after leader change): register first with placed counts
		if !s.registered {
			log.Printf("Registering with leader %s...", leaderAddr)
			if err := s.DoRegister(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Ag.GetPlacedTaskCounts(), s.Cfg.APIKey); err != nil {
				s.leaderFailed("Register failed (%d): %v", s.failCount+1, err)
			} else {
				log.Printf("Registered with leader %s", leaderAddr)
				s.registered = true
				s.failCount = 0
				s.lastLeaderAddr = leaderAddr
			}
			return
		}

		// Already registered → heartbeat (puur liveness, geen job-sync)
		err := s.DoHeartbeat(leaderAddr, s.Ag.ID(), s.Ag.Endpoint(), s.Cfg.APIKey)
		if err != nil {
			if errors.Is(err, ErrNotRegistered) {
				log.Printf("Not registered with leader, will re-register...")
				s.registered = false
				s.lastLeaderAddr = ""
			} else {
				s.leaderFailed("Heartbeat failed (%d): %v", s.failCount+1, err)
			}
		} else {
			s.failCount = 0
			s.lastLeaderAddr = leaderAddr
		}
	} else {
		// No leader known
		s.leaderFailed("No leader found (%d)", s.failCount+1)
	}
}

var httpClient = &http.Client{Timeout: 5 * time.Second}

// ErrNotRegistered: de leader kent deze agent niet (herstart) — herregistreren.
var ErrNotRegistered = errors.New("not registered with leader")

// postJSON sends a POST request with JSON body and API key to the leader.
func postJSON(path string, payload any, apiKey string) (*http.Response, error) {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	httputil.SignRequest(req, apiKey, body)
	return httpClient.Do(req)
}

// Register meldt een agent (met placed counts) aan bij de leader.
func Register(leaderAddr, agentID, agentEndpoint, version string, placed map[string]int, apiKey string) error {
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

// Heartbeat is puur een levensteken; de job-lijsten die hier vroeger
// meereisden zijn gesloopt (16-07) — gewenste staat heeft één auteur (de
// leader, gecommit naar S3; zie internal/leader/persist.go).
func Heartbeat(leaderAddr, agentID, agentEndpoint, version, apiKey string) error {
	resp, err := postJSON(fmt.Sprintf("http://%s/v1/heartbeat", leaderAddr), map[string]any{
		"id": agentID, "endpoint": agentEndpoint, "version": version,
	}, apiKey)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotRegistered
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("leader returned %d", resp.StatusCode)
	}
	return nil
}
