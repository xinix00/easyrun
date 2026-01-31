package agent

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"easyrun/internal/types"
)

// monitorTasks periodically checks task states and health
func (a *Agent) monitorTasks(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.checkTasks()
		}
	}
}

// checkTasks checks process states and health, restarts if needed
func (a *Agent) checkTasks() {
	a.tasksMu.Lock()
	defer a.tasksMu.Unlock()

	for _, task := range a.tasks {
		if task.State != types.TaskRunning {
			continue
		}

		// Check if process is still alive
		state, err := a.runner.Status(task)
		if err != nil {
			log.Printf("Failed to get status for task %s: %v", task.ID, err)
			continue
		}

		if state != types.TaskRunning {
			log.Printf("Task %s crashed (was %s, now %s)", task.ID, task.State, state)
			task.State = state

			// Try to restart
			go a.restartTask(task)
			continue
		}

		// Check health if configured
		job := a.jobs[task.JobID]
		if job != nil && job.HealthCheck != nil {
			if !a.checkHealth(task, job.HealthCheck) {
				log.Printf("Task %s failed health check", task.ID)
				// Kill and restart
				a.runner.Stop(task)
				task.State = types.TaskFailed
				go a.restartTask(task)
			}
		}
	}
}

// checkHealth performs a health check on a task
func (a *Agent) checkHealth(task *types.Task, hc *types.HealthCheck) bool {
	timeout := hc.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	// Determine which port to use
	portName := hc.Port
	if portName == "" {
		portName = "http" // default to http port
	}

	port, ok := task.Ports[portName]
	if !ok {
		log.Printf("Task %s has no port named %s for health check", task.ID, portName)
		return false
	}

	client := &http.Client{Timeout: timeout}

	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, hc.Path)
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
