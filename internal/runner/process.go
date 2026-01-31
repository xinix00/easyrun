package runner

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"easyrun/internal/types"

	"github.com/google/uuid"
)

// ProcessRunner runs processes with optional isolation
type ProcessRunner struct {
	config    *Config
	processes map[string]*exec.Cmd
	taskDirs  map[string]string // taskID -> work directory
	mu        sync.RWMutex
}

// NewProcessRunner creates a new process runner
func NewProcessRunner(config *Config) *ProcessRunner {
	return &ProcessRunner{
		config:    config,
		processes: make(map[string]*exec.Cmd),
		taskDirs:  make(map[string]string),
	}
}

// Run starts a job
func (r *ProcessRunner) Run(job *types.Job, ports map[string]int) (*types.Task, error) {
	if job.Command == "" {
		return nil, errors.New("command is required")
	}

	taskID := uuid.New().String()

	// Setup task directory
	taskDir, err := r.setupTaskDir(taskID, job)
	if err != nil {
		return nil, fmt.Errorf("failed to setup task directory: %w", err)
	}

	// Download artifact if specified
	if job.Artifact != nil {
		appDir := filepath.Join(taskDir, "app")
		if err := downloadArtifact(job.Artifact, appDir); err != nil {
			r.cleanupTaskDir(taskID)
			return nil, fmt.Errorf("failed to download artifact: %w", err)
		}
	}

	// Wrap command with memory limit (ulimit)
	command := r.wrapCommand(job.Command, job.MemoryLimit)
	cmd := exec.Command("/bin/sh", "-c", command)

	// Build port environment variables
	portEnvVars := r.buildPortEnvVars(ports)

	if r.config.Chroot {
		// Chroot mode: run inside chroot jail
		cmd.Dir = "/"
		cmd.Env = []string{
			"HOME=/",
			"TMPDIR=/tmp",
			"PATH=/bin:/usr/bin",
		}
		cmd.Env = append(cmd.Env, portEnvVars...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Chroot:  taskDir,
			Setpgid: true,
		}
	} else {
		// Non-chroot mode
		cmd.Dir = taskDir
		cmd.Env = []string{
			fmt.Sprintf("HOME=%s", taskDir),
			fmt.Sprintf("TMPDIR=%s/tmp", taskDir),
			"PATH=/usr/local/bin:/usr/bin:/bin",
		}
		cmd.Env = append(cmd.Env, portEnvVars...)
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}
	}

	for k, v := range job.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	// Start the process
	if err := cmd.Start(); err != nil {
		r.cleanupTaskDir(taskID)
		return nil, fmt.Errorf("failed to start process: %w", err)
	}

	r.mu.Lock()
	r.processes[taskID] = cmd
	r.taskDirs[taskID] = taskDir
	r.mu.Unlock()

	// Apply resource limits
	r.applyLimits(cmd.Process.Pid, job)

	// Wait for process in background
	go func() {
		cmd.Wait()
	}()

	return &types.Task{
		ID:        taskID,
		JobID:     job.ID,
		JobName:   job.Name,
		Ports:     ports,
		Pid:       cmd.Process.Pid,
		State:     types.TaskRunning,
		StartedAt: time.Now(),
	}, nil
}

// buildPortEnvVars creates environment variables for all ports
func (r *ProcessRunner) buildPortEnvVars(ports map[string]int) []string {
	var envVars []string

	// Set ER_PORT_<NAME> for each named port (uppercase)
	for name, port := range ports {
		upperName := ""
		for _, c := range name {
			if c >= 'a' && c <= 'z' {
				upperName += string(c - 32)
			} else if c >= 'A' && c <= 'Z' {
				upperName += string(c)
			} else if c >= '0' && c <= '9' {
				upperName += string(c)
			} else {
				upperName += "_"
			}
		}
		envVars = append(envVars, fmt.Sprintf("ER_PORT_%s=%d", upperName, port))
	}

	return envVars
}

// Stop stops a running task
func (r *ProcessRunner) Stop(task *types.Task) error {
	r.mu.Lock()
	cmd, ok := r.processes[task.ID]
	if ok {
		delete(r.processes, task.ID)
	}
	r.mu.Unlock()

	pid := task.Pid
	if cmd != nil && cmd.Process != nil {
		pid = cmd.Process.Pid
	}

	if pid <= 0 {
		r.cleanupTaskDir(task.ID)
		return nil
	}

	// Kill process group
	syscall.Kill(-pid, syscall.SIGTERM)

	// Wait for graceful shutdown
	done := make(chan struct{})
	go func() {
		if cmd != nil {
			cmd.Wait()
		} else {
			// Poll for process exit
			for i := 0; i < 50; i++ {
				if err := syscall.Kill(pid, 0); err != nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		syscall.Kill(-pid, syscall.SIGKILL)
		<-done
	}

	r.cleanupTaskDir(task.ID)
	return nil
}

// Status returns the current state of a task
func (r *ProcessRunner) Status(task *types.Task) (types.TaskState, error) {
	r.mu.RLock()
	cmd, ok := r.processes[task.ID]
	r.mu.RUnlock()

	if !ok {
		// Check by PID
		if task.Pid > 0 {
			if err := syscall.Kill(task.Pid, 0); err != nil {
				return types.TaskFailed, nil
			}
			return types.TaskRunning, nil
		}
		return types.TaskFailed, nil
	}

	if cmd.ProcessState != nil {
		if cmd.ProcessState.Success() {
			return types.TaskStopped, nil
		}
		return types.TaskFailed, nil
	}

	if cmd.Process != nil {
		if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
			return types.TaskFailed, nil
		}
	}

	return types.TaskRunning, nil
}

// applyLimits applies resource limits if set on the job
func (r *ProcessRunner) applyLimits(pid int, job *types.Job) {
	if job.CPUShares > 0 {
		r.applyNice(pid, job.CPUShares)
	}
	if job.MemoryLimit > 0 {
		r.applyMemoryLimit(pid, job.MemoryLimit)
	}
}

// applyNice sets process priority based on CPU shares
func (r *ProcessRunner) applyNice(pid int, cpuShares int) {
	maxShares := r.config.MaxCPUShares
	if maxShares <= 0 {
		maxShares = 14000
	}

	// More shares = lower nice = higher priority
	nice := 19 - (cpuShares * 19 / maxShares)
	if nice < 0 {
		nice = 0
	}
	if nice > 19 {
		nice = 19
	}

	if err := syscall.Setpriority(syscall.PRIO_PROCESS, pid, nice); err != nil {
		fmt.Printf("Warning: failed to set nice value: %v\n", err)
	}
}

// setupTaskDir creates an isolated directory for the task
func (r *ProcessRunner) setupTaskDir(taskID string, job *types.Job) (string, error) {
	// Base directory for all tasks
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/easyrun"
	}

	taskDir := filepath.Join(base, taskID)

	// Create directory structure
	dirs := []string{
		taskDir,
		filepath.Join(taskDir, "tmp"),
		filepath.Join(taskDir, "app"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	// Copy /etc/resolv.conf for DNS resolution
	r.copyFile("/etc/resolv.conf", filepath.Join(taskDir, "resolv.conf"))

	// Setup minimal shell environment for chroot
	if r.config.Chroot {
		r.setupChrootEnv(taskDir)
	}

	return taskDir, nil
}

// setupChrootEnv creates symlinks for minimal shell environment in chroot
func (r *ProcessRunner) setupChrootEnv(taskDir string) {
	// Create directories
	dirs := []string{
		filepath.Join(taskDir, "bin"),
		filepath.Join(taskDir, "usr", "bin"),
		filepath.Join(taskDir, "lib"),
		filepath.Join(taskDir, "usr", "lib"),
		filepath.Join(taskDir, "etc"),
	}
	for _, dir := range dirs {
		os.MkdirAll(dir, 0755)
	}

	// Symlink shell
	os.Symlink("/bin/sh", filepath.Join(taskDir, "bin", "sh"))

	// Copy resolv.conf to /etc
	r.copyFile("/etc/resolv.conf", filepath.Join(taskDir, "etc", "resolv.conf"))

	// Platform-specific library linking
	r.linkLibraries(taskDir)
}

// cleanupTaskDir removes the task directory
func (r *ProcessRunner) cleanupTaskDir(taskID string) {
	r.mu.Lock()
	taskDir, ok := r.taskDirs[taskID]
	if ok {
		delete(r.taskDirs, taskID)
	}
	r.mu.Unlock()

	if taskDir != "" {
		os.RemoveAll(taskDir)
	}
}

// CleanupAll removes all task directories (called at startup)
func (r *ProcessRunner) CleanupAll() error {
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/easyrun"
	}

	// Remove everything and recreate
	os.RemoveAll(base)
	return os.MkdirAll(base, 0755)
}

// copyFile copies a file from src to dst
func (r *ProcessRunner) copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
