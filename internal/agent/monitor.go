package agent

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"easyrun/internal/types"
)

const defaultFailureThreshold = 3

// checkState tracks consecutive health check failures and resource usage per task (monitor goroutine only)
type checkState struct {
	failCount       int
	lastCheckTime   time.Time // for file mtime comparison
	notifiedHealthy bool      // true after first "started" event fired

	// Resource usage tracking (for CPU delta calculation)
	lastCPUSeconds float64
	lastMeasureAt  time.Time
}

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

		state, err := a.runnerFor(task.Driver).Status(task)
		if err != nil {
			log.Printf("Failed to get status for task %s: %v", task.ID, err)
			continue
		}

		if state != types.TaskRunning {
			log.Printf("Task %s crashed (was %s, now %s)", task.ID, task.State, state)

			// Mark as Stopping (not Failed!) so it still counts for capacity.
			// restartTask will either atomic-swap (restart) or set Failed (maxRestarts exceeded).
			// Without this, there's a race: Failed frees capacity → leader dispatches new task
			// → restartTask also creates replacement → over-provisioning.
			a.do(func(s *agentState) {
				if t := s.tasks[task.ID]; t != nil {
					t.State = types.TaskStopping
				}
			})
			delete(a.checkStates, task.ID)
			go a.notifyLeader(task.JobName, "crash")
			go a.restartTask(task)
			continue
		}

		// Measure resource usage
		a.measureTaskUsage(task)

		// Check health if configured (skip during initial startup grace period)
		if job != nil && job.HealthCheck != nil {
			if job.HealthCheck.InitialTimeout > 0 && time.Since(task.StartedAt) < job.HealthCheck.InitialTimeout {
				continue
			}
			if !a.checkHealth(task, job.HealthCheck) {
				log.Printf("Task %s failed health check", task.ID)

				// Update state first, then stop async (runner.Stop can be slow)
				a.do(func(s *agentState) {
					if t := s.tasks[task.ID]; t != nil {
						t.State = types.TaskFailed
					}
				})
				delete(a.checkStates, task.ID)
				go func() {
					a.notifyLeader(task.JobName, "crash")
					_ = a.runnerFor(task.Driver).Stop(task)
					a.restartTask(task)
				}()
			} else if cs := a.checkStates[task.ID]; cs != nil && !cs.notifiedHealthy {
				// First health check pass → task is ready to serve traffic
				cs.notifiedHealthy = true
				go a.notifyLeader(task.JobName, "started")
			}
		}
	}
}

// measureTaskUsage measures CPU% and Mem% for a running task and updates it in state.
func (a *Agent) measureTaskUsage(task *types.Task) {
	cs := a.checkStates[task.ID]
	if cs == nil {
		cs = &checkState{}
		a.checkStates[task.ID] = cs
	}

	now := time.Now()
	var cpuPercent, memPercent float64

	hostCores := float64(a.sysInfo.CPUCores)
	allocCores := float64(task.CPUShares) / 1024.0
	if allocCores == 0 {
		allocCores = hostCores
	}
	allocMem := float64(task.MemoryLimit)
	if allocMem == 0 {
		allocMem = float64(a.sysInfo.MemoryBytes)
	}

	if task.Driver == types.DriverDocker {
		// Docker: CPUPerc 200% = 2 cores, MemPerc = % of container limit (or host if no limit)
		dockerCPU, dockerMem, err := getDockerUsage(task.ID)
		if err != nil {
			return
		}
		// CPU: dockerCPU/100 = cores used, scale to allocation
		cpuPercent = (dockerCPU / 100.0) / allocCores * 100
		// Mem: Docker already gives % of limit — use directly
		memPercent = dockerMem
	} else {
		// Exec: ps gives cumulative CPU seconds + RSS bytes
		cpuSec, memBytes, err := getProcessUsage(task.Pid)
		if err != nil {
			return
		}
		// CPU: delta since last measurement
		if !cs.lastMeasureAt.IsZero() {
			deltaCPU := cpuSec - cs.lastCPUSeconds
			deltaWall := now.Sub(cs.lastMeasureAt).Seconds()
			if deltaWall > 0 {
				cpuPercent = (deltaCPU / deltaWall) / allocCores * 100
			}
		}
		cs.lastCPUSeconds = cpuSec
		cs.lastMeasureAt = now
		// Mem: RSS bytes / allocation
		if allocMem > 0 {
			memPercent = float64(memBytes) / allocMem * 100
		}
	}

	a.do(func(s *agentState) {
		if t := s.tasks[task.ID]; t != nil {
			t.CPUPercent = cpuPercent
			t.MemPercent = memPercent
		}
	})
}

// checkHealth performs a health check with failure threshold counting.
// Returns true if task is healthy (or not yet at failure threshold).
func (a *Agent) checkHealth(task *types.Task, hc *types.HealthCheck) bool {
	var healthy bool
	switch hc.Type {
	case types.CheckTCP:
		healthy = a.checkHealthTCP(task, hc)
	case types.CheckFile:
		healthy = a.checkHealthFile(task, hc)
	default: // "" or "http"
		healthy = a.checkHealthHTTP(task, hc)
	}

	cs := a.checkStates[task.ID]
	if cs == nil {
		cs = &checkState{}
		a.checkStates[task.ID] = cs
	}

	if healthy {
		cs.failCount = 0
		cs.lastCheckTime = time.Now()
		return true
	}

	cs.failCount++
	cs.lastCheckTime = time.Now()

	threshold := hc.FailureThreshold
	if threshold <= 0 {
		threshold = defaultFailureThreshold
	}

	log.Printf("Task %s health check failed (%d/%d)", task.ID, cs.failCount, threshold)
	return cs.failCount < threshold // still under threshold = treat as healthy
}

// resolveHealthPort returns the port number and timeout for a health check.
func resolveHealthPort(task *types.Task, hc *types.HealthCheck) (int, time.Duration, bool) {
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
	}
	return port, timeout, ok
}

// checkHealthHTTP performs an HTTP health check
func (a *Agent) checkHealthHTTP(task *types.Task, hc *types.HealthCheck) bool {
	port, timeout, ok := resolveHealthPort(task, hc)
	if !ok {
		return false
	}

	resp, err := (&http.Client{Timeout: timeout}).Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, hc.Path))
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

// checkHealthTCP performs a TCP connect health check
func (a *Agent) checkHealthTCP(task *types.Task, hc *types.HealthCheck) bool {
	port, timeout, ok := resolveHealthPort(task, hc)
	if !ok {
		return false
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// checkHealthFile checks if a file has been modified since the last check
func (a *Agent) checkHealthFile(task *types.Task, hc *types.HealthCheck) bool {
	info, err := os.Stat(hc.Path)
	if err != nil {
		return false // file doesn't exist
	}

	cs := a.checkStates[task.ID]
	if cs == nil || cs.lastCheckTime.IsZero() {
		return true // first check: file exists = healthy
	}

	return info.ModTime().After(cs.lastCheckTime)
}
