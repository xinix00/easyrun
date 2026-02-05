package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"easyrun/internal/leader"
	"easyrun/internal/types"
	"easyrun/pkg/httputil"

	"github.com/google/uuid"
)

// generateID creates a random UUID (reuses existing dependency)
func generateID() string {
	return uuid.New().String()
}

// Server provides the HTTP API for the leader
type Server struct {
	leader *leader.Leader
	server *http.Server
}

// NewServer creates a new API server
func NewServer(l *leader.Leader, addr string) *Server {
	s := &Server{
		leader: l,
	}

	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/health", s.handleHealth)

	// Agents
	mux.HandleFunc("GET /v1/agents", s.handleGetAgents)
	mux.HandleFunc("POST /v1/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("DELETE /v1/agents/", s.handleUnregisterAgent)

	// Jobs
	mux.HandleFunc("GET /v1/jobs", s.handleGetJobs)
	mux.HandleFunc("POST /v1/jobs", s.handleRunJob)
	mux.HandleFunc("DELETE /v1/jobs/", s.handleDeleteJob)

	// Status
	mux.HandleFunc("GET /v1/status", s.handleStatus)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

// Run starts the HTTP server
func (s *Server) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	log.Printf("API server listening on %s", s.server.Addr)
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.server.Shutdown(ctx)
}

// handleHealth returns health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleGetAgents returns all registered agents
func (s *Server) handleGetAgents(w http.ResponseWriter, r *http.Request) {
	agents := s.leader.GetAgents()
	httputil.WriteJSON(w, http.StatusOK, agents)
}

// handleHeartbeat handles agent heartbeat (also registers new agents)
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID        string       `json:"id"`
		Endpoint  string       `json:"endpoint"`
		Version   string       `json:"version,omitempty"`
		Jobs      []*types.Job `json:"jobs,omitempty"`
		StateTime time.Time    `json:"state_time,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.ID == "" || req.Endpoint == "" {
		httputil.WriteError(w, http.StatusBadRequest, "id and endpoint required")
		return
	}

	jobs := s.leader.Heartbeat(req.ID, req.Endpoint, req.Jobs, req.StateTime, req.Version)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"jobs":       jobs,
		"state_time": s.leader.GetStateTime(),
	})
}

// handleUnregisterAgent removes an agent
func (s *Server) handleUnregisterAgent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/agents/")
	if id == "" {
		httputil.WriteError(w, http.StatusBadRequest, "agent id required")
		return
	}

	s.leader.UnregisterAgent(id)
	w.WriteHeader(http.StatusNoContent)
}

// handleGetJobs returns all jobs
func (s *Server) handleGetJobs(w http.ResponseWriter, r *http.Request) {
	jobs := s.leader.GetJobs()
	httputil.WriteJSON(w, http.StatusOK, jobs)
}

// handleRunJob dispatches or updates a job (upsert based on job.Name)
// If job with this name exists, it's updated according to update_policy
// If job doesn't exist, it's created and dispatched
func (s *Server) handleRunJob(w http.ResponseWriter, r *http.Request) {
	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if job.Name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name required")
		return
	}

	if job.Command == "" {
		httputil.WriteError(w, http.StatusBadRequest, "command required")
		return
	}

	// Always generate new ID (during updates, old and new job coexist temporarily)
	job.ID = generateID()

	// Check if job with this name already exists (UPDATE)
	existingJob := s.leader.FindJobByName(job.Name)
	if existingJob != nil {
		log.Printf("Job %s exists (old ID %s), updating to new ID %s (policy=%s)",
			job.Name, existingJob.ID, job.ID, job.UpdatePolicy)

		if err := s.leader.UpdateJob(&job); err != nil {
			httputil.WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}

		httputil.WriteJSON(w, http.StatusOK, map[string]string{
			"id":     job.ID,
			"name":   job.Name,
			"status": "updated",
			"policy": string(job.UpdatePolicy),
		})
		return
	}

	if err := s.leader.DispatchJob(&job); err != nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, map[string]string{
		"id":     job.ID,
		"name":   job.Name,
		"status": "dispatched",
	})
}

// handleDeleteJob deletes a job and cleans up all its tasks
func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "job name required")
		return
	}

	s.leader.DeleteJob(name)
	w.WriteHeader(http.StatusNoContent)
}

// handleStatus returns cluster status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	agents := s.leader.GetAgents()
	tasks := s.leader.GetClusterStatus()

	totalTasks := 0
	running := 0
	for _, agentTasks := range tasks {
		for _, t := range agentTasks {
			totalTasks++
			if t.State == types.TaskRunning {
				running++
			}
		}
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"agents":        len(agents),
		"total_tasks":   totalTasks,
		"running_tasks": running,
		"tasks_by_agent": tasks,
	})
}

