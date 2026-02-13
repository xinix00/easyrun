package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"easyrun/internal/leader"
	"easyrun/internal/types"
)

func init() {
	// Fast timeouts for tests
	leader.HTTPClientTimeout = 10 * time.Millisecond
	leader.VerifyInterval = 10 * time.Millisecond
}

func setupTestServer(t *testing.T) (*Server, *leader.Leader, context.CancelFunc) {
	t.Helper()
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond) // allow stateLoop to start
	server := NewServer(l, ":9080")
	return server, l, cancel
}

func doRequest(server *Server, method, path string, body any) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		data, _ := json.Marshal(body)
		req = httptest.NewRequest(method, path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(v); err != nil {
		t.Fatalf("Failed to decode response: %v (body: %s)", err, w.Body.String())
	}
}

// --- Health ---

func TestHealthEndpoint(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/health", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]string
	decodeJSON(t, w, &resp)
	if resp["status"] != "ok" {
		t.Errorf("status = %q, want %q", resp["status"], "ok")
	}
}

// --- Agents ---

func TestGetAgentsEmpty(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/agents", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var agents []any
	decodeJSON(t, w, &agents)
	if len(agents) != 0 {
		t.Errorf("Got %d agents, want 0", len(agents))
	}
}

func TestGetAgentsWithRegistered(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	for i := 0; i < 3; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		l.RegisterAgent(agentID, endpoint, "", nil)
		l.Heartbeat(agentID, endpoint, nil, nil, time.Time{}, "")
	}
	time.Sleep(10 * time.Millisecond)

	w := doRequest(server, "GET", "/v1/agents", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var agents []*types.Agent
	decodeJSON(t, w, &agents)
	if len(agents) != 3 {
		t.Errorf("Got %d agents, want 3", len(agents))
	}
}

// --- Heartbeat ---

func TestHeartbeatSuccess(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	body := map[string]string{
		"id":       "agent-1",
		"endpoint": "http://10.0.0.1:8080",
	}

	// Register agent first
	doRequest(server, "POST", "/v1/agents", body)

	w := doRequest(server, "POST", "/v1/heartbeat", body)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	decodeJSON(t, w, &resp)
	if resp["status"] != "ok" {
		t.Errorf("status = %v, want %q", resp["status"], "ok")
	}
	if _, ok := resp["state_time"]; !ok {
		t.Error("Response missing state_time")
	}
}

func TestHeartbeatMissingFields(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	tests := []struct {
		name string
		body map[string]string
	}{
		{"missing id", map[string]string{"endpoint": "http://10.0.0.1:8080"}},
		{"missing endpoint", map[string]string{"id": "agent-1"}},
		{"both empty", map[string]string{"id": "", "endpoint": ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := doRequest(server, "POST", "/v1/heartbeat", tt.body)
			if w.Code != 400 {
				t.Errorf("Status = %d, want 400", w.Code)
			}
		})
	}
}

func TestHeartbeatInvalidJSON(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	req := httptest.NewRequest("POST", "/v1/heartbeat", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestHeartbeatRegistersNewAgent(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	body := map[string]string{
		"id":       "new-agent",
		"endpoint": "http://10.0.0.1:8080",
	}

	// Register agent first
	doRequest(server, "POST", "/v1/agents", body)

	w := doRequest(server, "POST", "/v1/heartbeat", body)
	if w.Code != 200 {
		t.Fatalf("Heartbeat status = %d, want 200", w.Code)
	}

	// Verify agent is now visible
	time.Sleep(10 * time.Millisecond)
	w = doRequest(server, "GET", "/v1/agents", nil)

	var agents []*types.Agent
	decodeJSON(t, w, &agents)
	if len(agents) != 1 {
		t.Errorf("Got %d agents, want 1", len(agents))
	}
	if len(agents) > 0 && agents[0].ID != "new-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "new-agent")
	}
}

// --- Jobs ---

func TestGetJobsEmpty(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/jobs", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var jobs []any
	decodeJSON(t, w, &jobs)
	if len(jobs) != 0 {
		t.Errorf("Got %d jobs, want 0", len(jobs))
	}
}

func TestGetJobsWithStored(t *testing.T) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 5; i++ {
		store.StoreJob(&types.Job{
			ID:   fmt.Sprintf("job-%d", i),
			Name: fmt.Sprintf("job-%d", i),
		})
	}

	server := NewServer(l, ":9080")
	w := doRequest(server, "GET", "/v1/jobs", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var jobs []any
	decodeJSON(t, w, &jobs)
	if len(jobs) != 5 {
		t.Errorf("Got %d jobs, want 5", len(jobs))
	}
}

func TestRunJobMissingName(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{Command: "echo test"})

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestRunJobMissingCommand(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{Name: "test"})

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestRunJobInvalidJSON(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	req := httptest.NewRequest("POST", "/v1/jobs", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

func TestRunJobCreatesNew(t *testing.T) {
	// Need a mock agent to accept the dispatch
	mockAgent := newTestMockAgent()
	defer mockAgent.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("mock-agent", mockAgent.URL(), "", nil)
	l.Heartbeat("mock-agent", mockAgent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	server := NewServer(l, ":9080")

	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "test-job",
		Command: "echo hello",
		Count:   1,
	})

	if w.Code != 201 {
		t.Errorf("Status = %d, want 201 (body: %s)", w.Code, w.Body.String())
	}

	var resp map[string]string
	decodeJSON(t, w, &resp)
	if resp["status"] != "dispatched" {
		t.Errorf("status = %q, want %q", resp["status"], "dispatched")
	}
	if resp["name"] != "test-job" {
		t.Errorf("name = %q, want %q", resp["name"], "test-job")
	}
	if resp["id"] == "" {
		t.Error("Response missing id")
	}
}

func TestRunJobNoAgentsAvailable(t *testing.T) {
	server, store, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "test-job",
		Command: "echo hello",
	})

	// Job should be stored (201) even when no agents are available
	if w.Code != 201 {
		t.Errorf("Status = %d, want 201", w.Code)
	}

	var resp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "pending" {
		t.Errorf("status = %q, want %q", resp["status"], "pending")
	}

	// Job should exist in store for later reconciliation
	jobs := store.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job in store, got %d", len(jobs))
	}
	if jobs[0].Name != "test-job" {
		t.Errorf("stored job name = %q, want %q", jobs[0].Name, "test-job")
	}
}

func TestRunJobUpdateExisting(t *testing.T) {
	mockAgent := newTestMockAgent()
	defer mockAgent.Close()

	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	time.Sleep(10 * time.Millisecond)

	l.RegisterAgent("mock-agent", mockAgent.URL(), "", nil)
	l.Heartbeat("mock-agent", mockAgent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	server := NewServer(l, ":9080")

	// First: create job
	w := doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "update-test",
		Command: "echo v1",
		Count:   1,
	})
	if w.Code != 201 {
		t.Fatalf("Create status = %d, want 201 (body: %s)", w.Code, w.Body.String())
	}

	// Second: update same name
	w = doRequest(server, "POST", "/v1/jobs", types.Job{
		Name:    "update-test",
		Command: "echo v2",
		Count:   1,
	})

	if w.Code != 202 {
		t.Errorf("Update status = %d, want 202 (body: %s)", w.Code, w.Body.String())
	}

	var resp map[string]string
	decodeJSON(t, w, &resp)
	if resp["status"] != "updating" {
		t.Errorf("status = %q, want %q", resp["status"], "updating")
	}
}

// --- Delete ---

func TestDeleteJobByName(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "DELETE", "/v1/jobs/myapp", nil)

	if w.Code != 204 {
		t.Errorf("Status = %d, want 204", w.Code)
	}
}

func TestDeleteJobEmptyName(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "DELETE", "/v1/jobs/", nil)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

// --- Unregister Agent ---

func TestUnregisterAgent(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	l.RegisterAgent("agent-1", "http://10.0.0.1:8080", "", nil)
	l.Heartbeat("agent-1", "http://10.0.0.1:8080", nil, nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	w := doRequest(server, "DELETE", "/v1/agents/agent-1", nil)

	if w.Code != 204 {
		t.Errorf("Status = %d, want 204", w.Code)
	}

	// Verify agent is removed
	time.Sleep(10 * time.Millisecond)
	w = doRequest(server, "GET", "/v1/agents", nil)
	var agents []any
	decodeJSON(t, w, &agents)
	if len(agents) != 0 {
		t.Errorf("Got %d agents after unregister, want 0", len(agents))
	}
}

func TestUnregisterAgentEmptyID(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "DELETE", "/v1/agents/", nil)

	if w.Code != 400 {
		t.Errorf("Status = %d, want 400", w.Code)
	}
}

// --- Status ---

func TestStatusEndpointEmpty(t *testing.T) {
	server, _, cancel := setupTestServer(t)
	defer cancel()

	w := doRequest(server, "GET", "/v1/status", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	decodeJSON(t, w, &resp)
	if resp["agents"].(float64) != 0 {
		t.Errorf("agents = %v, want 0", resp["agents"])
	}
	if resp["jobs"].(float64) != 0 {
		t.Errorf("jobs = %v, want 0", resp["jobs"])
	}
}

func TestStatusEndpointWithAgents(t *testing.T) {
	server, l, cancel := setupTestServer(t)
	defer cancel()

	for i := 0; i < 3; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		endpoint := fmt.Sprintf("http://10.0.0.%d:8080", i)
		l.RegisterAgent(agentID, endpoint, "", nil)
		l.Heartbeat(agentID, endpoint, nil, nil, time.Time{}, "")
	}
	time.Sleep(10 * time.Millisecond)

	w := doRequest(server, "GET", "/v1/status", nil)

	if w.Code != 200 {
		t.Errorf("Status = %d, want 200", w.Code)
	}

	var resp map[string]any
	decodeJSON(t, w, &resp)
	if resp["agents"].(float64) != 3 {
		t.Errorf("agents = %v, want 3", resp["agents"])
	}
}

// --- Mock Agent for dispatch tests ---

type testMockAgent struct {
	server *httptest.Server
	mu     sync.Mutex
	tasks  []*types.Task
	seq    int
}

func newTestMockAgent() *testMockAgent {
	ma := &testMockAgent{}

	mux := http.NewServeMux()
	mux.HandleFunc("/run", ma.handleRun)
	mux.HandleFunc("/tasks", ma.handleTasks)
	mux.HandleFunc("/delete/", ma.handleDelete)

	ma.server = httptest.NewServer(mux)
	return ma
}

func (ma *testMockAgent) handleRun(w http.ResponseWriter, r *http.Request) {
	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	ma.mu.Lock()
	ma.seq++
	task := &types.Task{
		ID:      fmt.Sprintf("task-%s-%d", job.Name, ma.seq),
		JobID:   job.ID,
		JobName: job.Name,
		State:   types.TaskRunning,
	}
	ma.tasks = append(ma.tasks, task)
	ma.mu.Unlock()

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(task)
}

func (ma *testMockAgent) handleTasks(w http.ResponseWriter, r *http.Request) {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	_ = json.NewEncoder(w).Encode(ma.tasks)
}

func (ma *testMockAgent) handleDelete(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]int{"deleted": 1})
}

func (ma *testMockAgent) URL() string {
	return ma.server.URL
}

func (ma *testMockAgent) Close() {
	ma.server.Close()
}
