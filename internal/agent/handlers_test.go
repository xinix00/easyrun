package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
			JobID:   "job-1",
			State:   types.TaskRunning,
			Pid:     1234,
		}
		s.tasks["task-2"] = &types.Task{
			ID:      "task-2",
			JobID:   "job-2",
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
		ID:      "test-job",
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

	if task.JobID != "test-job" {
		t.Errorf("Task JobID = %q, want %q", task.JobID, "test-job")
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
	cfg.Capacity.CPUShares = 10
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	job := types.Job{
		ID:        "big-job",
		Name:      "test",
		Command:   "echo",
		CPUShares: 100, // Exceeds capacity
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
		ID:      "test-job",
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

func TestHandleStopSuccess(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	// Add a job and running task
	agent.StoreJob(&types.Job{ID: "job-1", Name: "test", Command: "echo"})
	agent.do(func(s *agentState) {
		s.tasks["task-1"] = &types.Task{
			ID:    "task-1",
			JobID: "job-1",
			State: types.TaskRunning,
		}
	})

	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodDelete, "/stop/job-1", nil)
	w := httptest.NewRecorder()

	agent.handleStop(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]int
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["stopped"] != 1 {
		t.Errorf("stopped = %d, want 1", resp["stopped"])
	}
}

func TestHandleStopMethodNotAllowed(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodPost, "/stop/job-1", nil)
	w := httptest.NewRecorder()

	agent.handleStop(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleStopMissingJobID(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	req := httptest.NewRequest(http.MethodDelete, "/stop/", nil)
	w := httptest.NewRecorder()

	agent.handleStop(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleStopNonExistentJob(t *testing.T) {
	cfg := testConfig()
	mockRunner := NewMockRunner()
	agent := New(cfg, "test-agent", mockRunner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go agent.stateLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodDelete, "/stop/nonexistent", nil)
	w := httptest.NewRecorder()

	agent.handleStop(w, req)

	// Should succeed with 0 stopped
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp map[string]int
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["stopped"] != 0 {
		t.Errorf("stopped = %d, want 0", resp["stopped"])
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
		ID:      "port-job",
		Name:    "test",
		Command: "echo",
		Ports:   []string{"http", "grpc"},
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
