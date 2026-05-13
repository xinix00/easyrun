package runner

import (
	"errors"
	"fmt"
	"log"
	"math"
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
	killTimeout             = 1 * time.Second // SIGKILL is enforced by the kernel; 1s is generous for reaping
	processExitPollInterval = 100 * time.Millisecond
	maxNiceValue            = 19
)

// ExecRunner runs processes with optional isolation
type ExecRunner struct {
	config    *Config
	processes map[string]*exec.Cmd
	taskDirs  map[string]string // taskID -> work directory
	mounts    map[string][]string // taskID -> bind mount targets we created, in setup order
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
		mounts:    make(map[string][]string),
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

	// Pre-create the cgroup so the child is born inside it via
	// clone3(CLONE_INTO_CGROUP). Doing this before exec avoids a race where
	// the task reads /proc/self/cgroup (and memory.max) before we've moved
	// it into the per-task cgroup — RavenDB and other runtimes cache that
	// path once at startup and would otherwise see stale/wrong values.
	cgroupFD, err := r.prepareCgroup(taskID, job.MemoryLimit)
	if err != nil {
		log.Printf("Warning: cgroup setup for %s failed, starting without limit: %v", taskID, err)
		cgroupFD = -1
	}

	// Setup command with platform-specific isolation
	cmd := r.setupCommand(job, taskDir, portEnvVars)
	if cgroupFD >= 0 {
		r.attachCgroup(cmd, cgroupFD)
	}

	// Setup log broadcasting
	stdoutBroadcaster := NewLogBroadcaster()
	stderrBroadcaster := NewLogBroadcaster()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		if cgroupFD >= 0 {
			_ = syscall.Close(cgroupFD)
			r.removeCgroup(taskID)
		}
		r.cleanupTaskDir(taskID)
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		if cgroupFD >= 0 {
			_ = syscall.Close(cgroupFD)
			r.removeCgroup(taskID)
		}
		r.cleanupTaskDir(taskID)
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the process
	startErr := cmd.Start()
	if cgroupFD >= 0 {
		_ = syscall.Close(cgroupFD)
	}
	if startErr != nil {
		if cgroupFD >= 0 {
			r.removeCgroup(taskID)
		}
		r.cleanupTaskDir(taskID)
		return fmt.Errorf("failed to start process: %w", startErr)
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

	// SIGTERM the whole process group, wait up to gracefulShutdownTimeout
	// for it to die, then SIGKILL if it didn't. Polling Kill(-pid, 0) is
	// safe; cmd.Wait() runs in Run's background goroutine and calling it
	// twice would be a data race.
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		log.Printf("Warning: failed to send SIGTERM to process group %d: %v", pid, err)
	}
	if !waitForPgroupExit(pid, gracefulShutdownTimeout) {
		log.Printf("Process group %d did not exit within %s, sending SIGKILL", pid, gracefulShutdownTimeout)
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			log.Printf("Warning: failed to send SIGKILL to process group %d: %v", pid, err)
		}
		if !waitForPgroupExit(pid, killTimeout) {
			log.Printf("Process group %d still alive after SIGKILL, giving up", pid)
		}
	}

	r.cleanupTaskDir(task.ID)
	r.removeCgroup(task.ID)
	return nil
}

// waitForPgroupExit polls Kill(-pid, 0) until the process group is gone or
// budget elapses. Returns true if the group exited in time.
func waitForPgroupExit(pid int, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pid, 0); err != nil {
			return true // ESRCH = pgroup gone
		}
		time.Sleep(processExitPollInterval)
	}
	return false
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

// applyLimits applies post-exec resource limits (nice only — memory is set
// pre-exec via the cgroup FD in Run so the child sees the right limit from
// /proc/self/cgroup at startup).
func (r *ExecRunner) applyLimits(pid int, job *types.Job) {
	if job.CPUShares > 0 {
		r.applyNice(pid, job.CPUShares)
	}
}

// applyNice sets process priority based on CPU shares. CFS weighs processes
// as weight = base / 1.25^nice, so to get an effective CPU share proportional
// to cpu_shares/max_shares we invert that: nice = log(max/shares) / log(1.25).
// Result: two jobs with shares 7000 and 1024 actually get ~86% / 14% of CPU
// under contention instead of the 94% / 6% the naive linear-to-nice mapping
// produced. Works the same on Linux and macOS — both use the nice 0..19 range.
func (r *ExecRunner) applyNice(pid int, cpuShares int) {
	maxShares := r.config.MaxCPUShares
	if maxShares <= 0 {
		maxShares = runtime.NumCPU() * 1024
	}

	nice := maxNiceValue
	if cpuShares >= maxShares {
		nice = 0
	} else if cpuShares > 0 {
		nice = int(math.Round(math.Log(float64(maxShares)/float64(cpuShares)) / math.Log(1.25)))
		if nice < 0 {
			nice = 0
		}
		if nice > maxNiceValue {
			nice = maxNiceValue
		}
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

	// Track every bind mount we create so cleanupTaskDir can unmount exactly
	// what was mounted — no /proc parsing needed. Job-script mounts live in
	// the child's mount namespace (CLONE_NEWNS) and die with the process, so
	// they're not our problem.
	var mounts []string

	for hostPath, taskPath := range job.Volumes {
		if err := os.MkdirAll(hostPath, 0755); err != nil {
			return "", fmt.Errorf("failed to create volume host path %s: %w", hostPath, err)
		}

		// Create target path inside task dir (strip leading / to keep it relative)
		target := filepath.Join(taskDir, strings.TrimPrefix(taskPath, "/"))
		targetDir := filepath.Dir(target)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create volume target dir %s: %w", targetDir, err)
		}

		if err := r.mountVolume(hostPath, target); err != nil {
			return "", fmt.Errorf("failed to mount volume %s -> %s: %w", hostPath, target, err)
		}
		mounts = append(mounts, target)
	}

	if r.config.Isolate {
		mounts = append(mounts, r.setupIsolationEnv(taskDir)...)
	}

	r.mu.Lock()
	r.mounts[taskID] = mounts
	r.mu.Unlock()

	return taskDir, nil
}

// cleanupTaskDir removes the task directory. Unmounts exactly what we
// tracked in setupTaskDir — reverse order so nested binds drop before
// their parents. With every bind explicitly detached, RemoveAll can no
// longer descend into a bound /dev and unlink host /dev/null.
func (r *ExecRunner) cleanupTaskDir(taskID string) {
	r.mu.Lock()
	taskDir := r.taskDirs[taskID]
	mounts := r.mounts[taskID]
	delete(r.taskDirs, taskID)
	delete(r.mounts, taskID)
	delete(r.stdoutLog, taskID)
	delete(r.stderrLog, taskID)
	r.mu.Unlock()

	for i := len(mounts) - 1; i >= 0; i-- {
		_ = r.unmountVolume(mounts[i])
	}
	if taskDir != "" {
		_ = os.RemoveAll(taskDir)
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

// Cleanup ensures the rootfs base exists and is writable. Stale taskdirs
// from a crashed predecessor (with bind mounts still attached) are NOT
// touched — RemoveAll into a bound /dev would nuke host /dev/null. After
// a hard crash, reboot or clean /tmp/hop manually with the agent stopped.
func (r *ExecRunner) Cleanup() error {
	base := r.config.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}
	if err := os.MkdirAll(base, 0777); err != nil {
		return err
	}
	if err := os.Chmod(base, 0777); err != nil {
		return err
	}
	ensureCgroupControllers()
	return nil
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

