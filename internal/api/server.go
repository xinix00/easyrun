package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"hop/internal/leader"
	"hop/internal/types"
	"hop/pkg/httputil"
)

// Server provides the HTTP API for the leader
type Server struct {
	leader      *leader.Leader
	server      *http.Server
	clusterName string
	apiKey      string
}

// NewServer creates a new API server
func NewServer(l *leader.Leader, addr string, apiKey string, clusterName string) *Server {
	s := &Server{
		leader:      l,
		clusterName: clusterName,
		apiKey:      apiKey,
	}

	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return httputil.RequireHMAC(apiKey, h)
	}

	mux := http.NewServeMux()

	// Health (public - needed for discovery)
	mux.HandleFunc("/health", s.handleHealth)

	// Agents (authenticated)
	mux.HandleFunc("GET /v1/agents", auth(s.handleGetAgents))
	mux.HandleFunc("POST /v1/agents", auth(s.handleRegisterAgent))
	mux.HandleFunc("POST /v1/heartbeat", auth(s.handleHeartbeat))
	mux.HandleFunc("DELETE /v1/agents/", auth(s.handleUnregisterAgent))
	mux.HandleFunc("GET /v1/agents/{agent_id}/capacity", auth(s.handleAgentCapacity))
	mux.HandleFunc("GET /v1/agents/{agent_id}/logs/{task_id}/{stream}", auth(s.handleAgentLogs))

	// Jobs (authenticated)
	mux.HandleFunc("GET /v1/jobs", auth(s.handleGetJobs))
	mux.HandleFunc("POST /v1/jobs", auth(s.handleApplyJob))
	mux.HandleFunc("DELETE /v1/jobs/", auth(s.handleDeleteJob))

	// Status (authenticated)
	mux.HandleFunc("GET /v1/status", auth(s.handleStatus))

	// Events (authenticated)
	mux.HandleFunc("GET /v1/events", auth(s.handleEvents))
	mux.HandleFunc("POST /v1/notify", auth(s.handleNotify))

	// Per-job endpoints (authenticated)
	mux.HandleFunc("GET /v1/jobs/{name}/status", auth(s.handleJobStatus))
	mux.HandleFunc("PATCH /v1/jobs/{name}/priority", auth(s.handlePatchJobPriority))

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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetAgents(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, s.leader.GetAgents())
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID        string         `json:"id"`
		Endpoint  string         `json:"endpoint"`
		Version   string         `json:"version,omitempty"`
		Jobs      []*types.Job   `json:"jobs,omitempty"`
		Placed    map[string]int `json:"placed,omitempty"`
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

	jobs, known := s.leader.Heartbeat(req.ID, req.Endpoint, req.Jobs, req.StateTime, req.Version)
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

func (s *Server) handleRegisterAgent(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string         `json:"id"`
		Endpoint string         `json:"endpoint"`
		Version  string         `json:"version,omitempty"`
		Placed   map[string]int `json:"placed,omitempty"` // jobName -> count
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.ID == "" || req.Endpoint == "" {
		httputil.WriteError(w, http.StatusBadRequest, "id and endpoint required")
		return
	}

	if !s.leader.RegisterAgent(req.ID, req.Endpoint, req.Version, req.Placed) {
		httputil.WriteError(w, http.StatusConflict, fmt.Sprintf("agent %s already registered with different endpoint", req.ID))
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "registered",
		"jobs":       s.leader.GetJobs(),
		"state_time": s.leader.GetStateTime(),
	})
}

func (s *Server) handleUnregisterAgent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/agents/")
	if id == "" {
		httputil.WriteError(w, http.StatusBadRequest, "agent id required")
		return
	}
	s.leader.UnregisterAgent(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetJobs(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, s.leader.GetJobs())
}

// handleApplyJob creates or updates a job by name (upsert).
// Name exists → UpdateJob with update_policy. Name unknown → DispatchJob.
func (s *Server) handleApplyJob(w http.ResponseWriter, r *http.Request) {
	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if job.Name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "name required")
		return
	}
	// hop driver: the artifact IS the program (a native app image); there is
	// no command (exec) or container image (docker).
	hopImage := job.Driver == types.DriverHop && len(job.Artifacts) == 1
	if job.Command == "" && job.Image == "" && !hopImage {
		httputil.WriteError(w, http.StatusBadRequest, "command or image required (or driver \"hop\" with one artifact)")
		return
	}

	// UPDATE — job already exists. Run synchronously so callers get real status codes:
	// 200 OK on success, 409 Conflict if another update is in flight, 500 on dispatch failure.
	if s.leader.GetJob(job.Name) != nil {
		policy := job.UpdatePolicy
		if policy == "" {
			policy = "rolling"
		}
		if err := s.leader.UpdateJob(&job); err != nil {
			if errors.Is(err, leader.ErrJobLocked) {
				httputil.WriteError(w, http.StatusConflict, err.Error())
				return
			}
			httputil.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, map[string]string{
			"name":   job.Name,
			"status": "updated",
			"policy": string(policy),
		})
		return
	}

	// CREATE — new job
	explicitPriority := job.Priority
	if job.Priority == nil {
		n := s.leader.NextPriority()
		job.Priority = &n
	}
	if err := s.leader.DispatchJob(&job); err != nil {
		httputil.WriteJSON(w, http.StatusCreated, map[string]string{
			"name":   job.Name,
			"status": "pending",
			"error":  err.Error(),
		})
		return
	}
	if explicitPriority != nil {
		_ = s.leader.PatchJobPriority(job.Name, *explicitPriority)
	}
	httputil.WriteJSON(w, http.StatusCreated, map[string]string{
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
	s.leader.DeleteJobByName(name)
	w.WriteHeader(http.StatusNoContent)
}

// handleStatus returns cluster overview from placed data (no HTTP calls to agents).
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	agents := s.leader.GetAgents()
	jobs := s.leader.GetJobs()
	placed := s.leader.GetPlacedCounts()

	totalPlaced := 0
	for _, count := range placed {
		totalPlaced += count
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"cluster_name": s.clusterName,
		"agents":       len(agents),
		"jobs":         len(jobs),
		"total_placed": totalPlaced,
		"settling":     !s.leader.IsSettled(),
		"placed":       placed,
	})
}

// handleEvents streams SSE notifications when cluster state changes.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	sse := httputil.SSEWriter(w)
	if sse == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	ch := s.leader.EventBus().Subscribe()
	defer s.leader.EventBus().Unsubscribe(ch)

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
				rest := strings.TrimPrefix(msg, "job:")
				if name, event, ok := strings.Cut(rest, ":"); ok {
					sse.WriteEvent("task", fmt.Sprintf(`{"job":%q,"event":%q}`, name, event))
				} else {
					sse.WriteEvent("job", fmt.Sprintf(`{"name":%q}`, rest))
				}
			default:
				sse.WriteEvent("status", "{}")
			}
		}
	}
}

// handleNotify receives agent task-change notifications and fires the event bus.
func (s *Server) handleNotify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Job   string `json:"job"`
		Event string `json:"event"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
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

// handlePatchJobPriority updates only the priority of a job.
func (s *Server) handlePatchJobPriority(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		httputil.WriteError(w, http.StatusBadRequest, "job name required")
		return
	}
	var body struct {
		Priority int `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if err := s.leader.PatchJobPriority(name, body.Priority); err != nil {
		httputil.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleJobStatus returns tasks and agents for a specific job.
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

// handleAgentCapacity proxies capacity query to specific agent
func (s *Server) handleAgentCapacity(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("agent_id")
	if agentID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "agent id required")
		return
	}

	resp := s.proxyToAgent(w, r, agentID, "/capacity")
	if resp == nil {
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, resp.Body)
}

// handleAgentLogs proxies SSE log streaming from specific agent
func (s *Server) handleAgentLogs(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("agent_id")
	taskID := r.PathValue("task_id")
	stream := r.PathValue("stream")

	if agentID == "" || taskID == "" || (stream != "stdout" && stream != "stderr") {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request parameters")
		return
	}

	path := fmt.Sprintf("/logs/%s/%s", taskID, stream)
	resp := s.proxyToAgent(w, r, agentID, path)
	if resp == nil {
		return
	}
	defer resp.Body.Close()

	sse := httputil.SSEWriter(w)
	if sse == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		fmt.Fprintf(w, "%s\n", scanner.Text())
		if scanner.Text() == "" {
			sse.Flush()
		}
	}
}

// proxyToAgent forwards an HTTP request to an agent, checking existence and setting API headers
func (s *Server) proxyToAgent(w http.ResponseWriter, r *http.Request, agentID string, path string) *http.Response {
	agent := s.leader.GetAgent(agentID)
	if agent == nil {
		httputil.WriteError(w, http.StatusNotFound, "agent not found")
		return nil
	}

	url := agent.Endpoint + path
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, nil)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to create proxy request")
		return nil
	}

	if accept := r.Header.Get("Accept"); accept != "" {
		req.Header.Set("Accept", accept)
	}

	httputil.SignRequest(req, s.apiKey, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, "failed to contact agent")
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		resp.Body.Close()
		return nil
	}

	return resp
}
