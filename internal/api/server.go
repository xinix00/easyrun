package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"easyrun/internal/leader"
	"easyrun/internal/types"
	"easyrun/pkg/httputil"

	"github.com/google/uuid"
)

// Server provides the HTTP API for the leader
type Server struct {
	leader *leader.Leader
	server *http.Server
}

// NewServer creates a new API server
func NewServer(l *leader.Leader, addr string, apiKey string) *Server {
	s := &Server{
		leader: l,
	}

	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return httputil.RequireAPIKey(apiKey, h)
	}

	mux := http.NewServeMux()

	// Health (public - needed for discovery)
	mux.HandleFunc("/health", s.handleHealth)

	// Agents (authenticated)
	mux.HandleFunc("GET /v1/agents", auth(s.handleGetAgents))
	mux.HandleFunc("POST /v1/agents", auth(s.handleRegisterAgent))
	mux.HandleFunc("POST /v1/heartbeat", auth(s.handleHeartbeat))
	mux.HandleFunc("DELETE /v1/agents/", auth(s.handleUnregisterAgent))

	// Jobs (authenticated)
	mux.HandleFunc("GET /v1/jobs", auth(s.handleGetJobs))
	mux.HandleFunc("POST /v1/jobs", auth(s.handleRunJob))
	mux.HandleFunc("DELETE /v1/jobs/", auth(s.handleDeleteJob))

	// Status (authenticated)
	mux.HandleFunc("GET /v1/status", auth(s.handleStatus))

	// Events (authenticated)
	mux.HandleFunc("GET /v1/events", auth(s.handleEvents))
	mux.HandleFunc("POST /v1/notify", auth(s.handleNotify))

	// Per-job status (authenticated)
	mux.HandleFunc("GET /v1/jobs/{name}/status", auth(s.handleJobStatus))

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
	_ = s.server.Shutdown(ctx)
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
		ID        string         `json:"id"`
		Endpoint  string         `json:"endpoint"`
		Version   string         `json:"version,omitempty"`
		Jobs      []*types.Job   `json:"jobs,omitempty"`       // All known jobs (for state sync)
		Placed    map[string]int `json:"placed,omitempty"`     // jobID -> count (ground truth from agent)
		StateTime time.Time      `json:"state_time,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.ID == "" || req.Endpoint == "" {
		httputil.WriteError(w, http.StatusBadRequest, "id and endpoint required")
		return
	}

	jobs, known := s.leader.Heartbeat(req.ID, req.Endpoint, req.Jobs, req.Placed, req.StateTime, req.Version)
	if !known {
		httputil.WriteError(w, http.StatusNotFound, "not registered")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"jobs":       jobs,
		"state_time": s.leader.GetStateTime(),
	})
}

// handleRegisterAgent registers a (re)starting agent
func (s *Server) handleRegisterAgent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string         `json:"id"`
		Endpoint string         `json:"endpoint"`
		Version  string         `json:"version,omitempty"`
		Placed   map[string]int `json:"placed,omitempty"` // jobID -> count (what's running on this agent)
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if req.ID == "" || req.Endpoint == "" {
		httputil.WriteError(w, http.StatusBadRequest, "id and endpoint required")
		return
	}

	s.leader.RegisterAgent(req.ID, req.Endpoint, req.Version, req.Placed)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "registered",
		"jobs":       s.leader.GetJobs(),
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
	job.ID = uuid.New().String()

	// Check if job with this name already exists (UPDATE)
	existingJob := s.leader.FindJobByName(job.Name)
	if existingJob != nil {
		log.Printf("Job %s exists (old ID %s), updating to new ID %s (policy=%s)",
			job.Name, existingJob.ID, job.ID, job.UpdatePolicy)

		// Fire-and-forget: rolling updates can take seconds per instance
		jobCopy := job
		go func() {
			if err := s.leader.UpdateJob(&jobCopy); err != nil {
				log.Printf("Update job %s failed: %v", jobCopy.Name, err)
			}
		}()

		httputil.WriteJSON(w, http.StatusAccepted, map[string]string{
			"id":     job.ID,
			"name":   job.Name,
			"status": "updating",
			"policy": string(job.UpdatePolicy),
		})
		return
	}

	if err := s.leader.DispatchJob(&job); err != nil {
		// Job is stored but dispatch failed — report as accepted (will retry on reconciliation)
		httputil.WriteJSON(w, http.StatusCreated, map[string]string{
			"id":     job.ID,
			"name":   job.Name,
			"status": "pending",
			"error":  err.Error(),
		})
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

// handleStatus returns cluster overview from placed data (no HTTP calls to agents).
// For task details (state, pid, restarts), use GET /v1/jobs/{name}/status.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	agents := s.leader.GetAgents()
	jobs := s.leader.GetJobs()
	placed := s.leader.GetPlacedByJobName()

	totalPlaced := 0
	for _, count := range placed {
		totalPlaced += count
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"agents":       len(agents),
		"jobs":         len(jobs),
		"total_placed": totalPlaced,
		"settling":     !s.leader.IsSettled(),
		"placed":       placed,
	})
}

// handleEvents streams SSE notifications when cluster state changes.
// SSE event types: "ping" (initial), "agent", "job", "task".
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	sse := httputil.SSEWriter(w)
	if sse == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	ch := s.leader.EventBus().Subscribe()
	defer s.leader.EventBus().Unsubscribe(ch)

	// Initial ping so client does an immediate refetch
	sse.WriteEvent("ping", "{}")

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			switch {
			case strings.HasPrefix(msg, "agent:"):
				sse.WriteEvent("agent", fmt.Sprintf(`{"id":%q}`, strings.TrimPrefix(msg, "agent:")))
			case strings.HasPrefix(msg, "job:"):
				// "job:name" = job-level change (dispatched/deleted)
				// "job:name:event" = task lifecycle (start/started/crash/stop)
				rest := strings.TrimPrefix(msg, "job:")
				if name, event, ok := strings.Cut(rest, ":"); ok {
					sse.WriteEvent("task", fmt.Sprintf(`{"job":%q,"event":%q}`, name, event))
				} else {
					sse.WriteEvent("job", fmt.Sprintf(`{"name":%q}`, rest))
				}
			}
		}
	}
}

// handleNotify receives agent task-change notifications and fires the event bus.
// Events: "start" (process started), "started" (healthy), "crash", "stop".
func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Job   string `json:"job"`
		Event string `json:"event"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req) // best-effort, empty body = generic notify
	if req.Job != "" {
		topic := "job:" + req.Job
		if req.Event != "" {
			topic += ":" + req.Event
		}
		s.leader.EventBus().Notify(topic)
	} else {
		s.leader.EventBus().Notify("")
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleJobStatus returns tasks and agents for a specific job.
// Only queries agents that have this job placed (via placed map).
func (s *Server) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "job name required")
		return
	}

	tasks, agents := s.leader.GetJobStatus(name)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"agents":         agents,
		"tasks_by_agent": tasks,
	})
}

