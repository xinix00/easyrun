package agent

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"easyrun/internal/runner"
	"easyrun/internal/types"
	"easyrun/pkg/httputil"

	"github.com/google/uuid"
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
	if key := r.Header.Get("X-API-Key"); key != "" {
		req.Header.Set("X-API-Key", key)
	} else if a.apiKey != "" {
		req.Header.Set("X-API-Key", a.apiKey)
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, "failed to contact leader")
		return
	}
	defer resp.Body.Close()

	// Copy response headers and status
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// proxySSEToLeader forwards the SSE event stream from the leader.
// Uses a dedicated client without timeout and flushes after each SSE event.
func (a *Agent) proxySSEToLeader(w http.ResponseWriter, r *http.Request) {
	if a.getLeader == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "leader discovery not configured")
		return
	}
	leaderAddr := a.getLeader()
	if leaderAddr == "" {
		httputil.WriteError(w, http.StatusServiceUnavailable, "no leader available")
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), "GET", fmt.Sprintf("http://%s/v1/events", leaderAddr), nil)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to create request")
		return
	}
	if key := r.Header.Get("X-API-Key"); key != "" {
		req.Header.Set("X-API-Key", key)
	} else if a.apiKey != "" {
		req.Header.Set("X-API-Key", a.apiKey)
	}

	// No timeout — SSE is long-lived
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		httputil.WriteError(w, http.StatusBadGateway, "failed to contact leader")
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
		if scanner.Text() == "" { // empty line = SSE event boundary
			sse.Flush()
		}
	}
}

// notifyLeader sends a lightweight event to the leader's /v1/notify endpoint.
// Events: "start" (process started), "started" (healthy), "crash", "stop".
func (a *Agent) notifyLeader(jobName, event string) {
	if a.getLeader == nil {
		return
	}
	addr := a.getLeader()
	if addr == "" {
		return
	}
	body := strings.NewReader(fmt.Sprintf(`{"job":%q,"event":%q}`, jobName, event))
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/v1/notify", addr), body)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		req.Header.Set("X-API-Key", a.apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
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

// CapacityResponse shows system resources and actual usage
type CapacityResponse struct {
	CPUCores        int               `json:"cpu_cores"`
	MemoryBytes     uint64            `json:"memory_bytes"`
	CPUUsedShares   int               `json:"cpu_used_shares"`
	MemoryUsedBytes uint64            `json:"memory_used_bytes"`
	TasksRunning    int               `json:"tasks_running"`
	Attributes      map[string]string `json:"attributes,omitempty"`
}

// handleCapacity returns detected system capacity with actual usage from running tasks
func (a *Agent) handleCapacity(w http.ResponseWriter, r *http.Request) {
	usage := query(a, func(s *agentState) CapacityResponse {
		cpuUsed, memUsed := s.resourceUsage()
		var running int
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				running++
			}
		}
		return CapacityResponse{
			CPUCores:        a.sysInfo.CPUCores,
			MemoryBytes:     a.sysInfo.MemoryBytes,
			CPUUsedShares:   cpuUsed,
			MemoryUsedBytes: memUsed,
			TasksRunning:    running,
			Attributes:      a.attributes,
		}
	})
	httputil.WriteJSON(w, http.StatusOK, usage)
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

	// Check affinity before capacity (agent-side: leader stays dumb)
	if !a.matchesAffinity(job.Affinity) {
		httputil.WriteJSON(w, http.StatusNotAcceptable, map[string]string{
			"error": "affinity mismatch",
		})
		return
	}

	// Ensure driver is set, then create the task.
	if job.Driver == "" {
		job.Driver = types.DriverFor(job.Image)
	}
	task := newTask(&job)

	// Check capacity AND add task to state atomically.
	// The task in state IS the capacity reservation — no separate reservation needed.
	added := query(a, func(s *agentState) bool {
		usedCPU, usedMem := s.resourceUsage()
		if job.CPUShares > 0 && usedCPU+job.CPUShares > a.sysInfo.CPUCores*1024 {
			return false
		}
		if job.MemoryLimit > 0 && usedMem+job.MemoryLimit > a.sysInfo.MemoryBytes {
			return false
		}
		s.jobs[job.Name] = &job
		s.tasks[task.ID] = task
		return true
	})

	if !added {
		httputil.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "insufficient capacity",
		})
		return
	}

	// Accept job immediately (fire-and-forget)
	httputil.WriteJSON(w, http.StatusAccepted, map[string]string{
		"status":  "accepted",
		"job":     job.Name,
		"message": "job accepted, starting in background",
	})

	// Start process in background (task already in state for capacity reservation)
	go func() {
		if err := a.startJob(&job, task); err != nil {
			log.Printf("Failed to start job %s: %v", job.Name, err)
			a.do(func(s *agentState) { delete(s.tasks, task.ID) })
		}
	}()
}

// handleDelete deletes a job and cleans up all its tasks (by job name)
func (a *Agent) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobName := strings.TrimPrefix(r.URL.Path, "/delete/")
	if jobName == "" {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "job name required"})
		return
	}

	deleted := a.deleteJob(jobName)
	httputil.WriteJSON(w, http.StatusOK, map[string]int{"deleted": deleted})
}

// handleStop stops all tasks for a job WITHOUT removing the job definition (by job name).
// Used by the leader for preemption — the job definition must remain for rescheduling.
func (a *Agent) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobName := strings.TrimPrefix(r.URL.Path, "/stop/")
	if jobName == "" {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "job name required"})
		return
	}

	stopped := a.stopJobTasks(jobName)
	httputil.WriteJSON(w, http.StatusOK, map[string]int{"stopped": stopped})
}

// handleStopTask stops a single specific task by task ID.
// Used by rolling and blue-green updates to stop precise old instances.
func (a *Agent) handleStopTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := strings.TrimPrefix(r.URL.Path, "/stop-task/")
	if taskID == "" {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "task id required"})
		return
	}

	task := query(a, func(s *agentState) *types.Task {
		if t := s.tasks[taskID]; t != nil {
			t.State = types.TaskStopping
			return t
		}
		return nil
	})

	if task == nil {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	go func() {
		if err := a.runnerFor(task.Driver).Stop(task); err != nil {
			log.Printf("Failed to stop task %s: %v", taskID, err)
		}
		a.do(func(s *agentState) {
			delete(s.tasks, taskID)
		})
		a.scheduleSave()
	}()

	httputil.WriteJSON(w, http.StatusOK, map[string]string{"stopped": taskID})
}

// stopJobTasks stops all tasks for a job WITHOUT removing the job definition.
// Used for preemption so the job remains in the store for future rescheduling.
func (a *Agent) stopJobTasks(jobName string) int {
	tasks := query(a, func(s *agentState) []*types.Task {
		var tasks []*types.Task
		for _, task := range s.tasks {
			if task.JobName == jobName {
				task.State = types.TaskStopping
				tasks = append(tasks, task)
			}
		}
		return tasks
	})

	a.stopTasks(tasks)
	log.Printf("Stopped tasks for job %s: %d tasks (job definition preserved)", jobName, len(tasks))
	return len(tasks)
}

// newTask creates a Task from a Job.
func newTask(job *types.Job) *types.Task {
	return &types.Task{
		ID:          uuid.New().String(),
		JobName:     job.Name,
		Driver:      job.Driver,
		Image:       job.Image,
		State:       types.TaskRunning,
		CPUShares:   job.CPUShares,
		MemoryLimit: job.MemoryLimit,
		StartedAt:   time.Now(),
	}
}

// startJob prepares and starts a job process. The task must be pre-created
// (via newTask). startJob allocates ports, runs the process, and stores in state.
func (a *Agent) startJob(job *types.Job, task *types.Task) error {
	if job.Driver == "" {
		job.Driver = types.DriverFor(job.Image)
	}

	// Resolve platform-specific artifact (runtime only — don't modify stored job)
	runJob := job
	if len(job.Artifacts) > 0 {
		resolved := a.resolveArtifact(job.Artifacts)
		if resolved == nil {
			return fmt.Errorf("no matching artifact for this node's attributes")
		}
		copy := *job
		copy.Artifacts = []types.Artifact{*resolved}
		runJob = &copy
	}

	ports, err := a.allocatePortsForJob(runJob)
	if err != nil {
		return fmt.Errorf("failed to allocate ports: %w", err)
	}
	task.Ports = ports

	// Runner fills in Pid and registers internal state
	// ER_ATTR_* env vars are injected by the runner itself (via Config.NodeAttrs)
	if err := a.runnerFor(runJob.Driver).Run(runJob, task); err != nil {
		return fmt.Errorf("failed to start: %w", err)
	}

	// Store in state. If /stop marked the task as Stopping while we were starting,
	// don't re-add it (prevents ghost tasks after preemption race).
	alive := query(a, func(s *agentState) bool {
		s.jobs[job.Name] = job
		if task.State == types.TaskRunning {
			s.tasks[task.ID] = task
			return true
		}
		return false
	})
	if !alive {
		_ = a.runnerFor(job.Driver).Stop(task)
		return nil
	}
	a.scheduleSave()

	log.Printf("Started task %s (job %s) with ports %v, pid %d", task.ID, job.Name, ports, task.Pid)
	if job.HealthCheck != nil {
		go a.notifyLeader(job.Name, "start")
	} else {
		go a.notifyLeader(job.Name, "started")
	}
	return nil
}

// allocatePorts allocates ports based on job port config
func allocatePorts(portConfig map[string]int) (map[string]int, error) {
	ports := make(map[string]int)
	for name, fixed := range portConfig {
		if fixed > 0 {
			if !isPortAvailable(fixed) {
				return nil, fmt.Errorf("port %d for %s is already in use", fixed, name)
			}
			ports[name] = fixed
		} else {
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
	job := a.GetJob(task.JobName)
	if job == nil {
		log.Printf("Cannot restart task %s: job %s not found", task.ID, task.JobName)
		return
	}

	maxRestarts := job.MaxRestarts
	if maxRestarts == 0 {
		maxRestarts = defaultMaxRestarts
	}
	restartWindow := job.RestartWindow
	if restartWindow == 0 {
		restartWindow = defaultRestartWindow
	}

	restartCount := query(a, func(s *agentState) int {
		if t := s.tasks[task.ID]; t != nil {
			// Grace period: reset count if last crash was longer ago than restart window
			if !t.LastFailedAt.IsZero() && time.Since(t.LastFailedAt) > restartWindow {
				t.RestartCount = 0
			}
			t.LastFailedAt = time.Now()
			return t.RestartCount
		}
		return 0
	})

	// -1 means unlimited restarts
	if maxRestarts > 0 && restartCount >= maxRestarts {
		log.Printf("Task %s exceeded max restarts (%d within %s), giving up", task.ID, maxRestarts, restartWindow)
		a.do(func(s *agentState) {
			if t := s.tasks[task.ID]; t != nil {
				t.State = types.TaskFailed
			}
		})
		return
	}

	// Clean up old runner entries (process already dead)
	_ = a.runnerFor(task.Driver).Stop(task)

	ports, err := a.allocatePortsForJob(job)
	if err != nil {
		log.Printf("Failed to allocate ports for restart: %v", err)
		return
	}

	// Atomic swap: new task via newTask(), preserve RestartCount, no capacity gap
	replacement := newTask(job)
	replacement.Ports = ports
	swapped := query(a, func(s *agentState) bool {
		old := s.tasks[task.ID]
		if old == nil {
			return false
		}
		replacement.RestartCount = old.RestartCount + 1
		delete(s.tasks, task.ID)
		s.tasks[replacement.ID] = replacement
		return true
	})
	if !swapped {
		log.Printf("Task %s disappeared from state during restart", task.ID)
		return
	}

	if err := a.runnerFor(job.Driver).Run(job, replacement); err != nil {
		log.Printf("Failed to restart task %s: %v", task.ID, err)
		a.do(func(s *agentState) {
			if t := s.tasks[replacement.ID]; t != nil {
				t.State = types.TaskFailed
			}
		})
		return
	}

	log.Printf("Restarted task %s -> %s (job %s), restart #%d", task.ID, replacement.ID, job.Name, replacement.RestartCount)
}

// deleteJob removes job definition AND cleans up all tasks by job name
func (a *Agent) deleteJob(jobName string) int {
	tasks := query(a, func(s *agentState) []*types.Task {
		delete(s.jobs, jobName)
		var tasks []*types.Task
		for _, task := range s.tasks {
			if task.JobName == jobName {
				task.State = types.TaskStopping
				tasks = append(tasks, task)
			}
		}
		return tasks
	})
	a.scheduleSave()

	a.stopTasks(tasks)
	log.Printf("Deleted job %s: %d tasks stopped", jobName, len(tasks))

	go a.notifyLeader(jobName, "stop")

	return len(tasks)
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

	var get func(runner.Runner) *runner.LogBroadcaster
	switch stream {
	case "stdout":
		get = func(r runner.Runner) *runner.LogBroadcaster { return r.GetStdout(taskID) }
	case "stderr":
		get = func(r runner.Runner) *runner.LogBroadcaster { return r.GetStderr(taskID) }
	default:
		http.Error(w, "stream must be stdout or stderr", http.StatusBadRequest)
		return
	}
	broadcaster := get(a.execRunner)
	if broadcaster == nil {
		broadcaster = get(a.dockerRunner)
	}

	if broadcaster == nil {
		http.Error(w, "task not found or not running", http.StatusNotFound)
		return
	}

	sse := httputil.SSEWriter(w)
	if sse == nil {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	logCh := broadcaster.Subscribe()
	defer broadcaster.Unsubscribe(logCh)

	for {
		select {
		case line, ok := <-logCh:
			if !ok {
				return
			}
			sse.WriteData(line)
		case <-r.Context().Done():
			return
		}
	}
}
