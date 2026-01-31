package agent

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"easyrun/internal/types"
)

// handleHealth returns health status
func (a *Agent) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleTasks returns all running tasks
func (a *Agent) handleTasks(w http.ResponseWriter, r *http.Request) {
	a.tasksMu.RLock()
	tasks := make([]*types.Task, 0, len(a.tasks))
	for _, t := range a.tasks {
		tasks = append(tasks, t)
	}
	a.tasksMu.RUnlock()

	writeJSON(w, http.StatusOK, tasks)
}

// handleRun starts a new job
func (a *Agent) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	// Check if we have capacity for this job
	if !a.hasCapacity(&job) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "insufficient capacity",
		})
		return
	}

	task, err := a.startJob(&job)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, task)
}

// handleStop stops a job
func (a *Agent) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/stop/")
	if jobID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id required"})
		return
	}

	stopped := a.stopJob(jobID)
	writeJSON(w, http.StatusOK, map[string]int{"stopped": stopped})
}

// startJob starts a job and returns the task
func (a *Agent) startJob(job *types.Job) (*types.Task, error) {
	// Allocate ports
	ports, err := allocatePorts(job.Ports)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate ports: %w", err)
	}

	task, err := a.runner.Run(job, ports)
	if err != nil {
		return nil, fmt.Errorf("failed to start: %w", err)
	}

	a.tasksMu.Lock()
	a.jobs[job.ID] = job
	a.tasks[task.ID] = task
	a.tasksMu.Unlock()

	log.Printf("Started task %s (job %s) with ports %v, pid %d", task.ID, job.Name, ports, task.Pid)
	return task, nil
}

// allocatePorts allocates free ports for the given port names
func allocatePorts(portNames []string) (map[string]int, error) {
	ports := make(map[string]int)

	for _, name := range portNames {
		port, err := getFreePort()
		if err != nil {
			return nil, fmt.Errorf("failed to get port for %s: %w", name, err)
		}
		ports[name] = port
	}

	return ports, nil
}

// restartTask restarts a failed task
func (a *Agent) restartTask(task *types.Task) {
	job := a.getJob(task.JobID)
	if job == nil {
		log.Printf("Cannot restart task %s: job %s not found", task.ID, task.JobID)
		return
	}

	maxRestarts := job.MaxRestarts
	if maxRestarts == 0 {
		maxRestarts = defaultMaxRestarts
	}

	// -1 means unlimited restarts
	if maxRestarts > 0 && task.RestartCount >= maxRestarts {
		log.Printf("Task %s exceeded max restarts (%d), giving up", task.ID, maxRestarts)
		task.State = types.TaskFailed
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
		task.RestartCount++
		return
	}

	// Update task with new info
	a.tasksMu.Lock()
	task.Pid = newTask.Pid
	task.Ports = newTask.Ports
	task.State = types.TaskRunning
	task.StartedAt = newTask.StartedAt
	task.RestartCount++
	a.tasksMu.Unlock()

	log.Printf("Restarted task %s (job %s), restart #%d", task.ID, job.Name, task.RestartCount)
}

// stopJob stops all tasks for a job
func (a *Agent) stopJob(jobID string) int {
	a.tasksMu.Lock()
	defer a.tasksMu.Unlock()

	// Remove job so it won't restart
	delete(a.jobs, jobID)

	stopped := 0
	for id, task := range a.tasks {
		if task.JobID == jobID && task.State == types.TaskRunning {
			if err := a.runner.Stop(task); err != nil {
				log.Printf("Failed to stop task %s: %v", id, err)
			} else {
				task.State = types.TaskStopped
				stopped++
			}
		}
	}
	return stopped
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
