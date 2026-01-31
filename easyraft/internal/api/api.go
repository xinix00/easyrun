package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"easyraft/internal/config"
	"easyraft/internal/lease"
	"easyraft/internal/raft"
)

// Server provides the HTTP API
type Server struct {
	cfg    *config.Config
	raft   *raft.Raft
	leases *lease.Manager
	server *http.Server
}

// New creates a new API server
func New(cfg *config.Config, r *raft.Raft, l *lease.Manager) *Server {
	return &Server{
		cfg:    cfg,
		raft:   r,
		leases: l,
	}
}

// Run starts the HTTP server
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	// Public endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/leader/", s.handleLeader)

	// Internal raft endpoints (require API key)
	mux.HandleFunc("/raft/vote", s.requireAPIKey(s.handleVote))
	mux.HandleFunc("/raft/heartbeat", s.requireAPIKey(s.handleHeartbeat))

	addr := fmt.Sprintf(":%d", s.cfg.Port)
	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(shutdownCtx)
	}()

	log.Printf("API server listening on %s", addr)
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// requireAPIKey middleware checks for valid API key
func (s *Server) requireAPIKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key != s.cfg.APIKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// handleHealth returns health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStatus returns raft cluster status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.raft.GetStatus())
}

// handleVote handles vote requests from other raft nodes
func (s *Server) handleVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Term      uint64 `json:"term"`
		Candidate string `json:"candidate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	granted := s.raft.HandleVoteRequest(req.Term, req.Candidate)
	writeJSON(w, http.StatusOK, map[string]bool{"granted": granted})
}

// handleHeartbeat handles heartbeat from leader
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Term   uint64 `json:"term"`
		Leader string `json:"leader"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	s.raft.HandleHeartbeat(req.Term, req.Leader)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleLeader handles GET/POST/DELETE /leader/{cluster}
func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	// Only leader handles lease requests
	if !s.raft.IsLeader() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":  "not leader",
			"leader": s.raft.Leader(),
		})
		return
	}

	cluster := strings.TrimPrefix(r.URL.Path, "/leader/")
	if cluster == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "cluster name required"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getLeader(w, cluster)
	case http.MethodPost:
		s.claimLeader(w, r, cluster)
	case http.MethodDelete:
		s.releaseLeader(w, r, cluster)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// getLeader returns the current leader for a cluster
func (s *Server) getLeader(w http.ResponseWriter, cluster string) {
	l := s.leases.Get(cluster)
	if l == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no leader"})
		return
	}
	writeJSON(w, http.StatusOK, l)
}

// claimLeader claims or renews leadership for a cluster
func (s *Server) claimLeader(w http.ResponseWriter, r *http.Request, cluster string) {
	var req struct {
		IP         string `json:"ip"`
		TTLSeconds int    `json:"ttl_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if req.IP == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ip required"})
		return
	}

	ttl := 30 * time.Second
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}

	l, ok := s.leases.Claim(cluster, req.IP, ttl)
	writeJSON(w, http.StatusOK, map[string]any{
		"success": ok,
		"leader":  l.LeaderIP,
		"expires": l.Expires,
	})
}

// releaseLeader releases leadership for a cluster
func (s *Server) releaseLeader(w http.ResponseWriter, r *http.Request, cluster string) {
	var req struct {
		IP string `json:"ip"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	ok := s.leases.Release(cluster, req.IP)
	writeJSON(w, http.StatusOK, map[string]bool{"released": ok})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
