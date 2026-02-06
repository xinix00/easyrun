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
	ticker := time.NewTicker(taskMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final save on shutdown
			if a.needsSave.Load() {
				a.SaveState()
			}
			return
		case <-ticker.C:
			a.checkTasks()

			// Piggyback state persistence (debounced)
			if a.needsSave.CompareAndSwap(true, false) {
				a.SaveState()
			}
		}
	}
}

// checkTasks checks process states and health, restarts if needed
func (a *Agent) checkTasks() {
	// Get snapshot of running tasks and their jobs
	type taskInfo struct {
		task *types.Task
		job  *types.Job
	}

	tasks := query(a, func(s *agentState) []taskInfo {
		var result []taskInfo
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				result = append(result, taskInfo{
					task: task,
					job:  s.jobs[task.JobID],
				})
			}
		}
		return result
	})

	// Check each task outside state loop (runner.Status can be slow)
	for _, info := range tasks {
		task := info.task
		job := info.job

		state, err := a.runner.Status(task)
		if err != nil {
			log.Printf("Failed to get status for task %s: %v", task.ID, err)
			continue
		}

		if state != types.TaskRunning {
			log.Printf("Task %s crashed (was %s, now %s)", task.ID, task.State, state)

			// Update state and restart
			a.do(func(s *agentState) {
				if t := s.tasks[task.ID]; t != nil {
					t.State = state
				}
			})
			go a.restartTask(task)
			continue
		}

		// Check health if configured (skip during initial startup grace period)
		if job != nil && job.HealthCheck != nil {
			if job.HealthCheck.InitialTimeout > 0 && time.Since(task.StartedAt) < job.HealthCheck.InitialTimeout {
				continue
			}
			if !a.checkHealth(task, job.HealthCheck) {
				log.Printf("Task %s failed health check", task.ID)

				// Stop and restart
				a.runner.Stop(task)
				a.do(func(s *agentState) {
					if t := s.tasks[task.ID]; t != nil {
						t.State = types.TaskFailed
					}
				})
				go a.restartTask(task)
			}
		}
	}
}

// checkHealth performs a health check on a task
func (a *Agent) checkHealth(task *types.Task, hc *types.HealthCheck) bool {
	timeout := hc.Timeout
	if timeout == 0 {
		timeout = defaultHealthTimeout
	}

	portName := hc.Port
	if portName == "" {
		portName = "http"
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
