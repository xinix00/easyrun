package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"easyrun/internal/runner"
	"easyrun/internal/types"
	"easyrun/pkg/httputil"
)

// proxyToLeader forwards requests to the current leader
func (a *Agent) proxyToLeader(w http.ResponseWriter, r *http.Request) {
	if a.getLeader == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "leader discovery not configured")
		return
	}

	leaderAddr := a.getLeader()
	if leaderAddr == "" {
		httputil.WriteError(w, http.StatusServiceUnavailable, "no leader available")
		return
	}

	// Forward request to leader (preserve method and body)
	url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.Path)
	req, err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to create request")
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))

	resp, err := a.httpClient.Do(req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, "failed to contact leader")
		return
	}
	defer resp.Body.Close()

	// Copy response headers and status
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handleLeader returns the current leader address
func (a *Agent) handleLeader(w http.ResponseWriter, r *http.Request) {
	leader := ""
	if a.getLeader != nil {
		leader = a.getLeader()
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"leader": leader})
}

// handleHealth returns basic health status
func (a *Agent) handleHealth(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// CapacityResponse shows system resources and usage
type CapacityResponse struct {
	CPUCores    int    `json:"cpu_cores"`
	MemoryBytes uint64 `json:"memory_bytes"`
}

// handleCapacity returns detected system capacity
func (a *Agent) handleCapacity(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, CapacityResponse{
		CPUCores:    a.sysInfo.CPUCores,
		MemoryBytes: a.sysInfo.MemoryBytes,
	})
}

// handleTasks returns all running tasks
func (a *Agent) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks := query(a, func(s *agentState) []*types.Task {
		result := make([]*types.Task, 0, len(s.tasks))
		for _, t := range s.tasks {
			result = append(result, t)
		}
		return result
	})
	httputil.WriteJSON(w, http.StatusOK, tasks)
}

// handleRun starts a new job
func (a *Agent) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if !a.hasCapacity(&job) {
		httputil.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "insufficient capacity",
		})
		return
	}

	task, err := a.startJob(&job)
	if err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	httputil.WriteJSON(w, http.StatusCreated, task)
}

// handleStop stops a job
func (a *Agent) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/stop/")
	if jobID == "" {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id required"})
		return
	}

	stopped := a.stopJob(jobID)
	httputil.WriteJSON(w, http.StatusOK, map[string]int{"stopped": stopped})
}

// startJob starts a job and returns the task
func (a *Agent) startJob(job *types.Job) (*types.Task, error) {
	ports, err := allocatePorts(job.Ports)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate ports: %w", err)
	}

	task, err := a.runner.Run(job, ports)
	if err != nil {
		return nil, fmt.Errorf("failed to start: %w", err)
	}

	// Store in state and persist
	a.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.tasks[task.ID] = task
	})
	a.SaveState()

	log.Printf("Started task %s (job %s) with ports %v, pid %d", task.ID, job.Name, ports, task.Pid)
	return task, nil
}

// allocatePorts allocates ports based on job port config
// If value is 0, allocates a dynamic free port
// If value > 0, uses that fixed port (after checking availability)
func allocatePorts(portConfig map[string]int) (map[string]int, error) {
	ports := make(map[string]int)
	for name, fixed := range portConfig {
		if fixed > 0 {
			// Check if fixed port is available
			if !isPortAvailable(fixed) {
				return nil, fmt.Errorf("port %d for %s is already in use", fixed, name)
			}
			ports[name] = fixed
		} else {
			// Allocate dynamic port
			port, err := getFreePort()
			if err != nil {
				return nil, fmt.Errorf("failed to get port for %s: %w", name, err)
			}
			ports[name] = port
		}
	}
	return ports, nil
}

// isPortAvailable checks if a port is available for binding
func isPortAvailable(port int) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	listener.Close()
	return true
}

// restartTask restarts a failed task
func (a *Agent) restartTask(task *types.Task) {
	job := a.getJob(task.JobName)
	if job == nil {
		log.Printf("Cannot restart task %s: job %s not found", task.ID, task.JobName)
		return
	}

	maxRestarts := job.MaxRestarts
	if maxRestarts == 0 {
		maxRestarts = defaultMaxRestarts
	}

	// Check restart count (read current value)
	restartCount := query(a, func(s *agentState) int {
		if t := s.tasks[task.ID]; t != nil {
			return t.RestartCount
		}
		return 0
	})

	// -1 means unlimited restarts
	if maxRestarts > 0 && restartCount >= maxRestarts {
		log.Printf("Task %s exceeded max restarts (%d), giving up", task.ID, maxRestarts)
		a.do(func(s *agentState) {
			if t := s.tasks[task.ID]; t != nil {
				t.State = types.TaskFailed
			}
		})
		return
	}

	ports, err := allocatePorts(job.Ports)
	if err != nil {
		log.Printf("Failed to allocate ports for restart: %v", err)
		return
	}

	newTask, err := a.runner.Run(job, ports)
	if err != nil {
		log.Printf("Failed to restart task %s: %v", task.ID, err)
		a.do(func(s *agentState) {
			if t := s.tasks[task.ID]; t != nil {
				t.RestartCount++
			}
		})
		return
	}

	// Update task with new info
	a.do(func(s *agentState) {
		if t := s.tasks[task.ID]; t != nil {
			t.Pid = newTask.Pid
			t.Ports = newTask.Ports
			t.State = types.TaskRunning
			t.StartedAt = newTask.StartedAt
			t.RestartCount++
		}
	})

	log.Printf("Restarted task %s (job %s), restart #%d", task.ID, job.Name, restartCount+1)
}

// stopJob stops all tasks for a job
func (a *Agent) stopJob(jobName string) int {
	// Get tasks to stop and remove job from state
	tasksToStop := query(a, func(s *agentState) []*types.Task {
		delete(s.jobs, jobName) // Remove job so it won't restart

		var tasks []*types.Task
		for _, task := range s.tasks {
			if task.JobName == jobName && task.State == types.TaskRunning {
				tasks = append(tasks, task)
			}
		}
		return tasks
	})

	// Stop tasks outside of state loop (runner.Stop can block)
	stopped := 0
	for _, task := range tasksToStop {
		if err := a.runner.Stop(task); err != nil {
			log.Printf("Failed to stop task %s: %v", task.ID, err)
		} else {
			a.do(func(s *agentState) {
				if t := s.tasks[task.ID]; t != nil {
					t.State = types.TaskStopped
				}
			})
			stopped++
		}
	}

	// Persist state after job removal
	if stopped > 0 {
		a.SaveState()
	}
	return stopped
}

// handleLogs streams task logs (stdout or stderr)
func (a *Agent) handleLogs(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/logs/"), "/")
	if len(parts) != 2 {
		http.Error(w, "usage: /logs/{taskID}/stdout or /logs/{taskID}/stderr", http.StatusBadRequest)
		return
	}

	taskID := parts[0]
	stream := parts[1]

	var broadcaster *runner.LogBroadcaster
	switch stream {
	case "stdout":
		broadcaster = a.runner.GetStdout(taskID)
	case "stderr":
		broadcaster = a.runner.GetStderr(taskID)
	default:
		http.Error(w, "stream must be stdout or stderr", http.StatusBadRequest)
		return
	}

	if broadcaster == nil {
		http.Error(w, "task not found or not running", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	logCh := broadcaster.Subscribe()
	defer broadcaster.Unsubscribe(logCh)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case line, ok := <-logCh:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", line)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
