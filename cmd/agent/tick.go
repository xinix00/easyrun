package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"hop/internal/types"
	"hop/pkg/config"
)

// Discoverer handles leader election queries.
type Discoverer interface {
	GetLeader() string
	TryBecomeLeader() bool
	RenewLease() bool
	ReleaseLeadership()
}

// agentAPI is the subset of agent.Agent used in the tick loop.
type agentAPI interface {
	ID() string
	Endpoint() string
	GetJobs() []*types.Job
	GetPlacedTaskCounts() map[string]int
	GetStateTime() time.Time
	SyncJobs(jobs []*types.Job, stateTime time.Time)
	StopAllTasks()
}

// leaderAPI is the subset of leader.Leader used in the tick loop.
type leaderAPI interface {
	GetAgents() []*types.Agent
}

// agentLoop holds all mutable state and injected dependencies for the tick loop.
type agentLoop struct {
	ctx    context.Context
	cfg    *config.Config
	ag     agentAPI
	disc   Discoverer

	// injectable for testing
	doRegister     func(leaderAddr, agentID, agentEndpoint string, placed map[string]int, apiKey string) error
	doHeartbeat    func(leaderAddr, agentID, agentEndpoint string, jobs []*types.Job, stateTime time.Time, apiKey string) (*heartbeatResponse, error)
	doBecomeLeader func() (stop func(), l leaderAPI)

	// mutable state
	l              leaderAPI
	stopLeader     func()
	failCount      int
	registered     bool
	lastLeaderAddr string
}

func (s *agentLoop) tryTakeOver(reason string) {
	log.Printf("%s, trying to become leader...", reason)
	s.lastLeaderAddr = ""
	if s.disc.TryBecomeLeader() {
		stop, l := s.doBecomeLeader()
		s.stopLeader = stop
		s.l = l
		s.failCount = 0
	}
}

func (s *agentLoop) leaderFailed(format string, args ...any) {
	s.failCount++
	log.Printf(format, args...)
	if s.failCount >= 4 {
		s.tryTakeOver("Leader unreachable")
	}
	if s.failCount >= 7 {
		log.Println("Likely network isolated, stopping all tasks to avoid duplicates")
		s.ag.StopAllTasks()
		s.failCount = 4
	}
}

func (s *agentLoop) tick() {
	// Use cached leader, only ask raft when unknown
	leaderAddr := s.lastLeaderAddr
	if leaderAddr == "" {
		leaderAddr = s.disc.GetLeader()
	}

	if s.stopLeader != nil {
		// We are leader — renew raft lease
		if !s.disc.RenewLease() {
			agents := s.l.GetAgents()
			if len(agents) > 0 {
				log.Printf("Raft unreachable but %d agents still connected, staying leader", len(agents))
			} else {
				log.Println("Lost leadership (raft unreachable + no agents)")
				s.registered = false
				s.lastLeaderAddr = ""
				s.stopLeader()
				s.stopLeader = nil
				s.l = nil
				return
			}
		} else {
			s.failCount = 0
		}
		// Self-heartbeat for job state sync
		leaderAddr = fmt.Sprintf("%s:%d", s.cfg.Node.IP, s.cfg.Node.Port+1000)
		_, _ = s.doHeartbeat(leaderAddr, s.ag.ID(), s.ag.Endpoint(), s.ag.GetJobs(), s.ag.GetStateTime(), s.cfg.APIKey)
	} else if leaderAddr != "" {
		// On startup (or after leader change): register first with placed counts
		if !s.registered {
			log.Printf("Registering with leader %s...", leaderAddr)
			if err := s.doRegister(leaderAddr, s.ag.ID(), s.ag.Endpoint(), s.ag.GetPlacedTaskCounts(), s.cfg.APIKey); err != nil {
				s.leaderFailed("Register failed (%d): %v", s.failCount+1, err)
			} else {
				log.Printf("Registered with leader %s", leaderAddr)
				s.registered = true
				s.failCount = 0
				s.lastLeaderAddr = leaderAddr
			}
			return
		}

		// Already registered → heartbeat
		resp, err := s.doHeartbeat(leaderAddr, s.ag.ID(), s.ag.Endpoint(), s.ag.GetJobs(), s.ag.GetStateTime(), s.cfg.APIKey)
		if err != nil {
			if errors.Is(err, errNotRegistered) {
				log.Printf("Not registered with leader, will re-register...")
				s.registered = false
				s.lastLeaderAddr = ""
			} else {
				s.leaderFailed("Heartbeat failed (%d): %v", s.failCount+1, err)
			}
		} else {
			s.failCount = 0
			s.lastLeaderAddr = leaderAddr
			if len(resp.Jobs) > 0 {
				s.ag.SyncJobs(resp.Jobs, resp.StateTime)
			}
		}
	} else {
		// No leader known
		s.leaderFailed("No leader found (%d)", s.failCount+1)
	}
}
