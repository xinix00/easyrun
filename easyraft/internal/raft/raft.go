package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"easyraft/internal/config"
)

// Raft handles leader election via HTTP
type Raft struct {
	cfg *config.Config

	term     uint64
	leader   string
	isLeader bool
	votedFor string

	mu sync.RWMutex

	httpClient *http.Client
}

// New creates a new Raft instance
func New(cfg *config.Config) *Raft {
	return &Raft{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Status returns current raft status
type Status struct {
	Term     uint64 `json:"term"`
	Leader   string `json:"leader"`
	IsLeader bool   `json:"is_leader"`
	Self     string `json:"self"`
	Peers    int    `json:"peers"`
}

// GetStatus returns current status
func (r *Raft) GetStatus() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return Status{
		Term:     r.term,
		Leader:   r.leader,
		IsLeader: r.isLeader,
		Self:     r.cfg.Self,
		Peers:    len(r.cfg.Peers),
	}
}

// IsLeader returns true if this node is leader
func (r *Raft) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeader
}

// Leader returns the current leader URL
func (r *Raft) Leader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leader
}

// Run starts the raft election loop
func (r *Raft) Run(ctx context.Context) {
	heartbeatInterval := time.Duration(r.cfg.HeartbeatInterval) * time.Millisecond
	electionTimeout := time.Duration(r.cfg.ElectionTimeout) * time.Millisecond

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	lastHeartbeat := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.RLock()
			isLeader := r.isLeader
			r.mu.RUnlock()

			if isLeader {
				// Send heartbeats to all peers
				r.sendHeartbeats()
				lastHeartbeat = time.Now()
			} else {
				// Check if we should start election
				if time.Since(lastHeartbeat) > electionTimeout {
					if r.shouldBeLeader() {
						if r.tryBecomeLeader() {
							lastHeartbeat = time.Now()
						}
					}
				}
			}
		}
	}
}

// shouldBeLeader returns true if this node has the lowest URL (deterministic leader)
func (r *Raft) shouldBeLeader() bool {
	sorted := make([]string, len(r.cfg.Peers))
	copy(sorted, r.cfg.Peers)
	sort.Strings(sorted)

	return len(sorted) > 0 && sorted[0] == r.cfg.Self
}

// tryBecomeLeader attempts to claim leadership
func (r *Raft) tryBecomeLeader() bool {
	r.mu.Lock()
	r.term++
	newTerm := r.term
	r.votedFor = r.cfg.Self
	r.mu.Unlock()

	log.Printf("Starting election for term %d", newTerm)

	// Request votes from all peers
	votes := 1 // Vote for self
	needed := len(r.cfg.Peers)/2 + 1

	for _, peer := range r.cfg.Peers {
		if peer == r.cfg.Self {
			continue
		}

		if r.requestVote(peer, newTerm) {
			votes++
		}
	}

	if votes >= needed {
		r.mu.Lock()
		r.isLeader = true
		r.leader = r.cfg.Self
		r.mu.Unlock()

		log.Printf("Became leader for term %d with %d/%d votes", newTerm, votes, len(r.cfg.Peers))
		return true
	}

	log.Printf("Election failed: got %d/%d votes, needed %d", votes, len(r.cfg.Peers), needed)
	return false
}

// requestVote asks a peer for their vote
func (r *Raft) requestVote(peer string, term uint64) bool {
	url := fmt.Sprintf("%s/raft/vote", peer)

	body, _ := json.Marshal(map[string]any{
		"term":      term,
		"candidate": r.cfg.Self,
	})

	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.cfg.APIKey)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var result struct {
		Granted bool `json:"granted"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Granted
}

// HandleVoteRequest handles incoming vote requests
func (r *Raft) HandleVoteRequest(term uint64, candidate string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If term is old, reject
	if term < r.term {
		return false
	}

	// If term is newer, update and reset vote
	if term > r.term {
		r.term = term
		r.votedFor = ""
		r.isLeader = false
	}

	// Grant vote if we haven't voted yet, or voted for same candidate
	// AND candidate should be leader (lowest URL)
	if (r.votedFor == "" || r.votedFor == candidate) && r.shouldVoteFor(candidate) {
		r.votedFor = candidate
		return true
	}

	return false
}

// shouldVoteFor returns true if candidate should be leader (has lowest URL)
func (r *Raft) shouldVoteFor(candidate string) bool {
	sorted := make([]string, len(r.cfg.Peers))
	copy(sorted, r.cfg.Peers)
	sort.Strings(sorted)

	return len(sorted) > 0 && sorted[0] == candidate
}

// sendHeartbeats sends heartbeat to all peers
func (r *Raft) sendHeartbeats() {
	r.mu.RLock()
	term := r.term
	r.mu.RUnlock()

	for _, peer := range r.cfg.Peers {
		if peer == r.cfg.Self {
			continue
		}

		go r.sendHeartbeat(peer, term)
	}
}

// sendHeartbeat sends a heartbeat to a single peer
func (r *Raft) sendHeartbeat(peer string, term uint64) {
	url := fmt.Sprintf("%s/raft/heartbeat", peer)

	body, _ := json.Marshal(map[string]any{
		"term":   term,
		"leader": r.cfg.Self,
	})

	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", r.cfg.APIKey)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// HandleHeartbeat handles incoming heartbeat from leader
func (r *Raft) HandleHeartbeat(term uint64, leader string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term >= r.term {
		r.term = term
		r.leader = leader
		r.isLeader = false
		r.votedFor = ""
	}
}
