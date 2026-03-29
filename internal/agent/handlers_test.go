package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hop/internal/types"
)

// ============== HTTP HANDLER TESTS ==============

func TestHandleHealth(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	agent.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("status = %q, want %q", resp["status"], "ok")
	}
}

func TestHandleTasks(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add some tasks
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "job-1",
			State:   types.TaskRunning,
			Pid:     1234,
		}
		s.tasks["task-2"] = &types.Task{
			ID:      "task-2",
			JobName: "job-2",
			State:   types.TaskStopped,
			Pid:     5678,
		}
	})

	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w := httptest.NewRecorder()

	agent.handleTasks(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var tasks []*types.Task
	if err := json.NewDecoder(w.Body).Decode(&tasks); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(tasks) != 2 {
		t.Errorf("Got %d tasks, want 2", len(tasks))
	}
}

func TestHandleRunSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo hello",
	}

	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted asynchronously
	if w.Code != http.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusAccepted)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["status"] != "accepted" {
		t.Errorf("status = %q, want %q", resp["status"], "accepted")
	}
	if resp["job"] != "test" {
		t.Errorf("job = %q, want %q", resp["job"], "test")
	}

	// Wait for async job start
	time.Sleep(50 * time.Millisecond)

	// Verify task was created
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Errorf("Got %d tasks, want 1", len(tasks))
	}
	if len(tasks) > 0 && tasks[0].JobName != "test" {
		t.Errorf("Task JobName = %q, want %q", tasks[0].JobName, "test")
	}
}

func TestHandleRunMethodNotAllowed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodGet, "/run", nil)
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleRunInvalidJSON(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleRunInsufficientCapacity(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 1, MemoryBytes: 1024}) // 1024 CPU shares

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:      "test",
		Command:   "echo",
		CPUShares: 2000, // Exceeds 1024 shares
	}

	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleRunRunnerError(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
	}

	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted (fire-and-forget), error happens async
	if w.Code != http.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusAccepted)
	}

	// Wait for async job attempt
	time.Sleep(50 * time.Millisecond)

	// Verify no task was created (runner failed)
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 0 {
		t.Errorf("Got %d tasks, want 0 (runner should have failed)", len(tasks))
	}
}

func TestHandleDeleteSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add a job and running task
	agent.StoreJob(&types.Job{Name: "test", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:      "task-1",
			JobName: "test",
			State:   types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodDelete, "/delete/test", nil)
	w := httptest.NewRecorder()

	agent.handleDelete(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]int
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["deleted"] != 1 {
		t.Errorf("deleted = %d, want 1", resp["deleted"])
	}
}

func TestHandleDeleteMethodNotAllowed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodPost, "/delete/job-1", nil)
	w := httptest.NewRecorder()

	agent.handleDelete(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleDeleteMissingJobID(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodDelete, "/delete/", nil)
	w := httptest.NewRecorder()

	agent.handleDelete(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleDeleteNonExistentJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodDelete, "/delete/nonexistent", nil)
	w := httptest.NewRecorder()

	agent.handleDelete(w, req)

	// Should succeed with 0 deleted
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]int
	_ = json.NewDecoder(w.Body).Decode(&resp)

	if resp["deleted"] != 0 {
		t.Errorf("deleted = %d, want 0", resp["deleted"])
	}
}

func TestHandleLogsInvalidPath(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodGet, "/logs/invalid", nil)
	w := httptest.NewRecorder()

	agent.handleLogs(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleLogsInvalidStream(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodGet, "/logs/task-1/invalid", nil)
	w := httptest.NewRecorder()

	agent.handleLogs(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleLogsTaskNotFound(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodGet, "/logs/nonexistent/stdout", nil)
	w := httptest.NewRecorder()

	agent.handleLogs(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleRunEmptyJSON(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()

	// Configure mock to fail on empty command (simulating real runner validation)
	mockRunner.onRun = func(job *types.Job) error {
		if job.Command == "" {
			return errors.New("command is required")
		}
		return nil
	}

	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	// Empty JSON object - job accepted, but will fail async
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte("{}")))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted (fire-and-forget), validation happens async
	if w.Code != http.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusAccepted)
	}

	// Wait for async job attempt
	time.Sleep(50 * time.Millisecond)

	// Verify no task was created (command validation failed)
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 0 {
		t.Errorf("Got %d tasks, want 0 (empty command should fail)", len(tasks))
	}
}

func TestHandleRunWithPorts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
		Ports:   map[string]int{"http": 0, "grpc": 0},
	}

	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusAccepted)
	}

	// Wait for async job start
	time.Sleep(50 * time.Millisecond)

	// Check that task has allocated ports
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Fatalf("Got %d tasks, want 1", len(tasks))
	}

	task := tasks[0]
	if len(task.Ports) != 2 {
		t.Errorf("Ports = %d, want 2", len(task.Ports))
	}

	if _, ok := task.Ports["http"]; !ok {
		t.Error("Should have http port")
	}
	if _, ok := task.Ports["grpc"]; !ok {
		t.Error("Should have grpc port")
	}
}

func TestHandleRunWithFixedPorts(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
		Ports:   map[string]int{"http": 8080, "grpc": 0}, // http fixed, grpc dynamic
	}

	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusAccepted)
	}

	// Wait for async job start
	time.Sleep(50 * time.Millisecond)

	// Check that task has correct ports
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Fatalf("Got %d tasks, want 1", len(tasks))
	}

	task := tasks[0]
	// http should be fixed at 8080
	if task.Ports["http"] != 8080 {
		t.Errorf("http port = %d, want 8080", task.Ports["http"])
	}

	// grpc should be dynamic (non-zero)
	if task.Ports["grpc"] == 0 {
		t.Error("grpc port should be dynamically allocated")
	}
}

func TestHandleRunPortInUse(t *testing.T) {
	// Occupy a port
	listener, err := net.Listen("tcp", "127.0.0.1:19876")
	if err != nil {
		t.Fatalf("Failed to occupy port: %v", err)
	}
	defer listener.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		Name:    "test",
		Command: "echo",
		Ports:   map[string]int{"http": 19876}, // Port is in use
	}

	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	// Job is accepted (fire-and-forget), port check happens async
	if w.Code != http.StatusAccepted {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusAccepted)
	}

	// Wait for async job attempt
	time.Sleep(50 * time.Millisecond)

	// Verify no task was created (port in use)
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 0 {
		t.Errorf("Got %d tasks, want 0 (port in use should fail)", len(tasks))
	}
}

// ============== CAPACITY HANDLER TESTS ==============

func TestHandleCapacity(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 8, MemoryBytes: 16 * 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/capacity", nil)
	w := httptest.NewRecorder()

	agent.handleCapacity(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp CapacityResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.CPUCores != 8 {
		t.Errorf("CPUCores = %d, want 8", resp.CPUCores)
	}
	if resp.MemoryBytes != 16*1024*1024*1024 {
		t.Errorf("MemoryBytes = %d, want %d", resp.MemoryBytes, 16*1024*1024*1024)
	}
	if resp.TasksRunning != 0 {
		t.Errorf("TasksRunning = %d, want 0", resp.TasksRunning)
	}
	if resp.CPUUsedShares != 0 {
		t.Errorf("CPUUsedShares = %d, want 0", resp.CPUUsedShares)
	}
	if resp.MemoryUsedBytes != 0 {
		t.Errorf("MemoryUsedBytes = %d, want 0", resp.MemoryUsedBytes)
	}
}

// ============== LEADER HANDLER TESTS ==============

func TestHandleLeader(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// No getLeader set

	req := httptest.NewRequest(http.MethodGet, "/leader", nil)
	w := httptest.NewRecorder()

	agent.handleLeader(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["leader"] != "" {
		t.Errorf("leader = %q, want empty string", resp["leader"])
	}
}

func TestHandleLeaderWithFunc(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return "10.0.0.1:9080"
	})

	req := httptest.NewRequest(http.MethodGet, "/leader", nil)
	w := httptest.NewRecorder()

	agent.handleLeader(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["leader"] != "10.0.0.1:9080" {
		t.Errorf("leader = %q, want %q", resp["leader"], "10.0.0.1:9080")
	}
}

// ============== PROXY TO LEADER TESTS ==============

func TestProxyToLeaderNoFunc(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	// No getLeader set

	req := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	w := httptest.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestProxyToLeaderNoLeader(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return "" // No leader available
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	w := httptest.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestProxyToLeaderSuccess(t *testing.T) {
	// Mock leader server
	leaderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"id":"agent-1"}]`))
	}))
	defer leaderServer.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return leaderServer.Listener.Addr().String()
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
	w := httptest.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	// Verify response was proxied
	body := w.Body.String()
	if body != `[{"id":"agent-1"}]` {
		t.Errorf("Body = %q, want agent list", body)
	}
}

func TestProxyToLeaderPostForward(t *testing.T) {
	var receivedBody string
	var receivedMethod string

	leaderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		data, _ := io.ReadAll(r.Body)
		receivedBody = string(data)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer leaderServer.Close()

	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)
	agent.SetLeaderFunc(func() string {
		return leaderServer.Listener.Addr().String()
	})

	reqBody := `{"name":"test","command":"echo"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewReader([]byte(reqBody)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	agent.proxyToLeader(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusCreated)
	}
	if receivedMethod != http.MethodPost {
		t.Errorf("Proxied method = %q, want %q", receivedMethod, http.MethodPost)
	}
	if receivedBody != reqBody {
		t.Errorf("Proxied body = %q, want %q", receivedBody, reqBody)
	}
}

// ============== LOG STREAMING TESTS ==============

func TestHandleLogsStdoutStream(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Start a job to create a task with log broadcasters
	job := &types.Job{Name: "log-test", Command: "echo hello"}
	task := newTask(job)
	if err := agent.startJob(job, task); err != nil {
		t.Fatalf("startJob failed: %v", err)
	}

	// Write to the stdout broadcaster
	broadcaster := mockRunner.GetStdout(task.ID)
	if broadcaster == nil {
		t.Fatal("No stdout broadcaster for task")
	}

	// Create a request with a context we can cancel
	reqCtx, reqCancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/logs/"+task.ID+"/stdout", nil)
	req = req.WithContext(reqCtx)
	w := httptest.NewRecorder()

	// Run handleLogs in a goroutine (it blocks on the SSE stream)
	done := make(chan struct{})
	go func() {
		agent.handleLogs(w, req)
		close(done)
	}()

	// Write a log line
	time.Sleep(20 * time.Millisecond)
	_, _ = broadcaster.Write([]byte("test log line"))

	// Give time for the message to be processed
	time.Sleep(50 * time.Millisecond)

	// Cancel the request to stop the stream
	reqCancel()
	<-done

	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("data: test log line")) {
		t.Errorf("Body = %q, want SSE format with 'data: test log line'", body)
	}
}

// TestStopDuringStartDoesNotResurrectTask verifies that when /stop is called
// while startJob is still running (goroutine), the task does not come back
// as a zombie in "stopping" state.
func TestStopDuringStartDoesNotResurrectTask(t *testing.T) {
	cfg := testConfig()
	cfg.Capacity.CPUShares = 4096 // 4 cores

	// Mock runner with a delay on Run() to simulate slow process start
	startCh := make(chan struct{})
	mockRunner := NewMockRunner()
	mockRunner.onRun = func(job *types.Job) error {
		<-startCh // block until test releases
		return nil
	}

	agent := New(cfg, "test-agent", mockRunner)
	agent.SetSysInfo(SystemInfo{CPUCores: 4, MemoryBytes: 4 * 1024 * 1024 * 1024})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Dispatch a job via handleRun — task is added to state, startJob blocks
	job := types.Job{Name: "myapp", Command: "./app", Count: 1, CPUShares: 1024}
	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()
	agent.handleRun(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("handleRun: status %d, want %d", w.Code, http.StatusAccepted)
	}

	// Task should be in state (capacity reserved)
	count := query(agent, func(s *agentState) int { return len(s.tasks) })
	if count != 1 {
		t.Fatalf("Expected 1 task in state after dispatch, got %d", count)
	}

	// Stop the job while startJob is still blocked
	stopReq := httptest.NewRequest(http.MethodPost, "/stop/myapp", nil)
	stopW := httptest.NewRecorder()
	agent.handleStop(stopW, stopReq)

	// Task should be removed from state
	count = query(agent, func(s *agentState) int { return len(s.tasks) })
	if count != 0 {
		t.Fatalf("Expected 0 tasks after stop, got %d", count)
	}

	// Now let startJob complete — it should NOT resurrect the task
	close(startCh)
	time.Sleep(100 * time.Millisecond) // let goroutine finish

	// Verify task did NOT come back
	count = query(agent, func(s *agentState) int { return len(s.tasks) })
	if count != 0 {
		t.Errorf("Ghost task! Expected 0 tasks after startJob completed, got %d", count)
	}
}

// ============== EARLY FAILURE TESTS ==============

// TestHandleRun_EarlyFailure_TaskStaysFailed verifies that when startJob
// fails (e.g., volume doesn't exist, runner error), the task stays in state
// with state=failed instead of being deleted.
func TestHandleRun_EarlyFailure_TaskStaysFailed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	job := types.Job{Name: "failing-app", Command: "echo hello"}
	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("Expected 202 Accepted, got %d", w.Code)
	}

	// Wait for background startJob to fail
	time.Sleep(50 * time.Millisecond)

	// Task should still exist with state=failed (NOT deleted)
	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task (failed), got %d (task was deleted instead of marked failed)", len(tasks))
	}
	if tasks[0].State != types.TaskFailed {
		t.Errorf("Task state should be 'failed', got %q", tasks[0].State)
	}
	if tasks[0].JobName != "failing-app" {
		t.Errorf("Task job name should be 'failing-app', got %q", tasks[0].JobName)
	}
}

// TestHandleRun_EarlyFailure_VisibleInTaskList verifies that a failed task
// shows up in the /tasks endpoint response.
func TestHandleRun_EarlyFailure_VisibleInTaskList(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	mockRunner.SetRunError(ErrSimulated)
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// Dispatch job that will fail
	job := types.Job{Name: "broken", Command: "echo"}
	body, _ := json.Marshal(job)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()
	agent.handleRun(w, req)
	time.Sleep(50 * time.Millisecond)

	// GET /tasks should show the failed task
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w = httptest.NewRecorder()
	agent.handleTasks(w, req)

	var tasks []*types.Task
	if err := json.NewDecoder(w.Body).Decode(&tasks); err != nil {
		t.Fatalf("Failed to decode tasks: %v", err)
	}

	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task in /tasks response, got %d", len(tasks))
	}
	if tasks[0].State != types.TaskFailed {
		t.Errorf("Task in /tasks should be 'failed', got %q", tasks[0].State)
	}
}

// TestHandleRun_EarlyFailure_ThenSuccess verifies that after an early failure,
// a subsequent successful dispatch works correctly alongside the failed task.
func TestHandleRun_EarlyFailure_ThenSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)
	time.Sleep(10 * time.Millisecond)

	// First dispatch: will fail
	mockRunner.SetRunError(ErrSimulated)
	job1 := types.Job{Name: "app", Command: "echo v1"}
	body, _ := json.Marshal(job1)
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()
	agent.handleRun(w, req)
	time.Sleep(50 * time.Millisecond)

	// Second dispatch: will succeed
	mockRunner.SetRunError(nil)
	job2 := types.Job{Name: "app", Command: "echo v2"}
	body, _ = json.Marshal(job2)
	req = httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
	w = httptest.NewRecorder()
	agent.handleRun(w, req)
	time.Sleep(50 * time.Millisecond)

	tasks := query(agent, func(s *agentState) []*types.Task {
		var result []*types.Task
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})

	// Should have 2 tasks: 1 failed + 1 running
	failed := 0
	running := 0
	for _, task := range tasks {
		switch task.State {
		case types.TaskFailed:
			failed++
		case types.TaskRunning:
			running++
		}
	}

	if failed != 1 {
		t.Errorf("Expected 1 failed task, got %d", failed)
	}
	if running != 1 {
		t.Errorf("Expected 1 running task, got %d", running)
	}
}
