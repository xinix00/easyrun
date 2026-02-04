package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"easyrun/internal/types"
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

	if w.Code != http.StatusCreated {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusCreated)
	}

	var task types.Task
	if err := json.NewDecoder(w.Body).Decode(&task); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if task.JobName != "test" {
		t.Errorf("Task JobName = %q, want %q", task.JobName, "test")
	}
	if task.State != types.TaskRunning {
		t.Errorf("Task State = %q, want %q", task.State, types.TaskRunning)
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

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusInternalServerError)
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

func TestHandleDeleteMissingJobName(t *testing.T) {
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
	json.NewDecoder(w.Body).Decode(&resp)

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

	// Empty JSON object - should fail because command is empty
	req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader([]byte("{}")))
	w := httptest.NewRecorder()

	agent.handleRun(w, req)

	// Should fail because command is required in runner
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Status code = %d, want %d (empty command should fail)", w.Code, http.StatusInternalServerError)
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

	if w.Code != http.StatusCreated {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusCreated)
	}

	var task types.Task
	json.NewDecoder(w.Body).Decode(&task)

	// Should have allocated ports
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

	if w.Code != http.StatusCreated {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusCreated)
	}

	var task types.Task
	json.NewDecoder(w.Body).Decode(&task)

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

	// Should fail because port is in use
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusInternalServerError)
	}

	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)

	if !strings.Contains(resp["error"], "already in use") {
		t.Errorf("Error should mention port in use, got: %s", resp["error"])
	}
}
