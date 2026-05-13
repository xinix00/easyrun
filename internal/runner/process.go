package runner

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"hop/internal/types"
)

const (
	gracefulShutdownTimeout = 10 * time.Second
	killTimeout             = 5 * time.Second
	processExitPollInterval = 100 * time.Millisecond
	processExitPollAttempts = 50
	maxNiceValue            = 19
)

// ExecRunner runs processes with optional isolation
type ExecRunner struct {
	config    *Config
	processes map[string]*exec.Cmd
	taskDirs  map[string]string // taskID -> work directory
	stdoutLog map[string]*LogBroadcaster
	stderrLog map[string]*LogBroadcaster
	mu        sync.RWMutex
}

// NewExecRunner creates a new process runner
func NewExecRunner(config *Config) *ExecRunner {
	return &ExecRunner{
		config:    config,
		processes: make(map[string]*exec.Cmd),
		taskDirs:  make(map[string]string),
		stdoutLog: make(map[string]*LogBroadcaster),
		stderrLog: make(map[string]*LogBroadcaster),
	}
}

// Run starts a process for the job. The task is pre-created by the caller;
// Run fills in Pid and registers internal state (process handle, log broadcasters).
func (r *ExecRunner) Run(job *types.Job, task *types.Task) error {
	if job.Command == "" {
		return errors.New("command is required")
	}

	taskID := task.ID

	// Setup task directory
	taskDir, err := r.setupTaskDir(taskID, job)
	if err != nil {
		return fmt.Errorf("failed to setup task directory: %w", err)
	}

	// Download artifact if specified (agent already resolved to first matching entry)
	if len(job.Artifacts) > 0 {
		log.Printf("Downloading artifact for task %s: %s", taskID, job.Artifacts[0].URL)
		// Download directly to taskDir so commands like "./binary" work
		if err := downloadArtifact(&job.Artifacts[0], taskDir); err != nil {
			log.Printf("Artifact download failed for task %s: %v", taskID, err)
			r.cleanupTaskDir(taskID)
			return fmt.Errorf("failed to download artifact: %w", err)
		}
	}

	// Build port environment variables
	portEnvVars := PortEnvVars(task.Ports)

	// Setup command with platform-specific isolation
	cmd := r.setupCommand(job, taskDir, portEnvVars)

	// Setup log broadcasting
	stdoutBroadcaster := NewLogBroadcaster()
	stderrBroadcaster := NewLogBroadcaster()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		r.cleanupTaskDir(taskID)
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		r.cleanupTaskDir(taskID)
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the process
	if err := cmd.Start(); err != nil {
		r.cleanupTaskDir(taskID)
		return fmt.Errorf("failed to start process: %w", err)
	}

	// Start log broadcasting
	go PipeReader(stdoutBroadcaster, stdoutPipe)
	go PipeReader(stderrBroadcaster, stderrPipe)

	r.mu.Lock()
	r.processes[taskID] = cmd
	r.taskDirs[taskID] = taskDir
	r.stdoutLog[taskID] = stdoutBroadcaster
	r.stderrLog[taskID] = stderrBroadcaster
	r.mu.Unlock()

	// Apply resource limits
	r.applyLimits(cmd.Process.Pid, job)

	// Wait for process in background
	go func() {
		_ = cmd.Wait()
	}()

	task.Pid = cmd.Process.Pid
	return nil
}

// Stop stops a running task
func (r *ExecRunner) Stop(task *types.Task) error {
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
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		fmt.Printf("Warning: failed to send SIGTERM to process group %d: %v\n", pid, err)
	}

	// Wait for graceful shutdown by polling PID.
	// Don't call cmd.Wait() here — Run's background goroutine already does that,
	// and calling it twice on the same exec.Cmd is a data race.
	done := make(chan struct{})
	go func() {
		for i := 0; i < processExitPollAttempts; i++ {
			if err := syscall.Kill(-pid, 0); err != nil {
				break // entire process group dead
			}
			time.Sleep(processExitPollInterval)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(gracefulShutdownTimeout):
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			fmt.Printf("Warning: failed to send SIGKILL to process group %d: %v\n", pid, err)
		}
		select {
		case <-done:
		case <-time.After(killTimeout):
			log.Printf("Process group %d did not exit after SIGKILL, giving up", pid)
		}
	}

	r.cleanupTaskDir(task.ID)
	return nil
}

// Status returns the current state of a task.
func (r *ExecRunner) Status(task *types.Task) (types.TaskState, error) {
	// PID not yet set → process still starting (artifact download, etc.)
	if task.Pid == 0 {
		return types.TaskRunning, nil
	}

	// Check process group (covers parent + forked children)
	if err := syscall.Kill(-task.Pid, 0); err == nil {
		return types.TaskRunning, nil
	}

	// Dead — determine stopped vs failed
	r.mu.RLock()
	cmd, ok := r.processes[task.ID]
	r.mu.RUnlock()

	if ok && cmd.ProcessState != nil && cmd.ProcessState.Success() {
		return types.TaskStopped, nil
	}
	return types.TaskFailed, nil
}

// applyLimits applies resource limits if set on the job
func (r *ExecRunner) applyLimits(pid int, job *types.Job) {
	if job.CPUShares > 0 {
		r.applyNice(pid, job.CPUShares)
	}
	if job.MemoryLimit > 0 {
		r.applyMemoryLimit(pid, job.MemoryLimit)
	}
}

// applyNice sets process priority based on CPU shares
func (r *ExecRunner) applyNice(pid int, cpuShares int) {
	maxShares := r.config.MaxCPUShares
	if maxShares <= 0 {
		maxShares = runtime.NumCPU() * 1024
	}

	// More shares = lower nice = higher priority
	nice := maxNiceValue - (cpuShares * maxNiceValue / maxShares)
	if nice < 0 {
		nice = 0
	}
	if nice > maxNiceValue {
		nice = maxNiceValue
	}

	if err := syscall.Setpriority(syscall.PRIO_PROCESS, pid, nice); err != nil {
		fmt.Printf("Warning: failed to set nice value: %v\n", err)
	}
}

// setupTaskDir creates an isolated directory for the task
func (r *ExecRunner) setupTaskDir(taskID string, job *types.Job) (string, error) {
	// Base directory for all tasks
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}

	taskDir := filepath.Join(base, taskID)

	// Resolve user for ownership
	uid, gid := -1, -1
	if job.User != "" {
		if cred, _, err := lookupCredential(job.User); err == nil {
			uid, gid = int(cred.Uid), int(cred.Gid)
		}
	}

	// Create directory structure (chown if user is set so child entries inherit)
	dirs := []string{
		taskDir,
		filepath.Join(taskDir, "tmp"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create %s: %w", dir, err)
		}
		if uid >= 0 {
			_ = os.Chown(dir, uid, gid)
		}
	}

	// Setup volume mounts (symlinks from host to task dir)
	for hostPath, taskPath := range job.Volumes {
		// Ensure host path exists (create if needed)
		if err := os.MkdirAll(hostPath, 0755); err != nil {
			return "", fmt.Errorf("failed to create volume host path %s: %w", hostPath, err)
		}

		// Create target path inside task dir (strip leading / to keep it relative)
		target := filepath.Join(taskDir, strings.TrimPrefix(taskPath, "/"))
		targetDir := filepath.Dir(target)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create volume target dir %s: %w", targetDir, err)
		}

		// Mount host path to task path
		if err := r.mountVolume(hostPath, target); err != nil {
			return "", fmt.Errorf("failed to mount volume %s -> %s: %w", hostPath, target, err)
		}
	}

	// Setup environment for isolation (platform-specific)
	if r.config.Isolate {
		r.setupIsolationEnv(taskDir)
	}

	return taskDir, nil
}

// cleanupTaskDir removes the task directory
func (r *ExecRunner) cleanupTaskDir(taskID string) {
	r.mu.Lock()
	taskDir, ok := r.taskDirs[taskID]
	if ok {
		delete(r.taskDirs, taskID)
	}
	delete(r.stdoutLog, taskID)
	delete(r.stderrLog, taskID)
	r.mu.Unlock()

	if taskDir != "" {
		// Unmount any volumes before removing
		_ = filepath.Walk(taskDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() && path != taskDir {
				_ = r.unmountVolume(path) // ignore errors, may not be a mount
			}
			return nil
		})
		os.RemoveAll(taskDir)
	}
}

// GetStdout returns the stdout broadcaster for a task
func (r *ExecRunner) GetStdout(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stdoutLog[taskID]
}

// GetStderr returns the stderr broadcaster for a task
func (r *ExecRunner) GetStderr(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stderrLog[taskID]
}

// Cleanup removes all task directories (called at startup)
func (r *ExecRunner) Cleanup() error {
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}

	// Remove everything and recreate
	os.RemoveAll(base)
	if err := os.MkdirAll(base, 0777); err != nil {
		return err
	}
	return os.Chmod(base, 0777)
}

// lookupCredential resolves a username to UID/GID for process credential switching
func lookupCredential(username string) (*syscall.Credential, string, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return nil, "", fmt.Errorf("user %q not found: %w", username, err)
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(u.Gid)
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, u.HomeDir, nil
}

