package runner

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"hop/internal/types"
)

const (
	containerPrefix     = "hop-"
	dockerStopTimeout   = 10 // seconds (SIGTERM grace period, then SIGKILL)
	dockerCommandTimeout = 30 * time.Second // max time for docker stop + rm
)

// DockerRunner runs jobs as Docker containers
type DockerRunner struct {
	nodeAttrs map[string]string
	stdoutLog map[string]*LogBroadcaster
	stderrLog map[string]*LogBroadcaster
	logCmds   map[string]*exec.Cmd // taskID -> docker logs process
	mu        sync.RWMutex
}

// NewDockerRunner creates a new Docker runner
func NewDockerRunner(nodeAttrs map[string]string) *DockerRunner {
	return &DockerRunner{
		nodeAttrs: nodeAttrs,
		stdoutLog: make(map[string]*LogBroadcaster),
		stderrLog: make(map[string]*LogBroadcaster),
		logCmds:   make(map[string]*exec.Cmd),
	}
}

// Run starts a Docker container for the job. The task is pre-created by the caller;
// Run registers internal state (log streaming).
func (r *DockerRunner) Run(job *types.Job, task *types.Task) error {
	if job.Image == "" {
		return fmt.Errorf("image is required for docker runner")
	}

	containerName := containerPrefix + task.ID

	args := []string{"run", "-d", "--name", containerName}

	// Port mappings: -p hostPort:containerPort
	for name, hostPort := range task.Ports {
		containerPort := job.Ports[name]
		if containerPort == 0 {
			containerPort = hostPort
		}
		args = append(args, "-p", fmt.Sprintf("%d:%d", hostPort, containerPort))
	}

	// Environment variables
	for k, v := range job.Env {
		args = append(args, "-e", k+"="+v)
	}

	// Port env vars (same convention as ExecRunner)
	for _, env := range PortEnvVars(task.Ports) {
		args = append(args, "-e", env)
	}
	for _, env := range AttrEnvVars(r.nodeAttrs) {
		args = append(args, "-e", env)
	}

	// Volumes
	for hostPath, containerPath := range job.Volumes {
		args = append(args, "-v", hostPath+":"+containerPath)
	}

	// Resource limits
	if job.MemoryLimit > 0 {
		args = append(args, "--memory", fmt.Sprintf("%d", job.MemoryLimit))
	}
	if job.CPUShares > 0 {
		args = append(args, "--cpu-shares", fmt.Sprintf("%d", job.CPUShares))
	}

	// Image
	args = append(args, job.Image)

	// Command override (optional for Docker)
	if job.Command != "" {
		args = append(args, "/bin/sh", "-c", job.Command)
	}

	cmd := exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run failed: %w: %s", err, strings.TrimSpace(string(output)))
	}

	// Start log streaming
	r.startLogStreaming(task.ID, containerName)

	return nil
}

// Stop stops and removes a Docker container
func (r *DockerRunner) Stop(task *types.Task) error {
	containerName := containerPrefix + task.ID
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()

	// Stop container (SIGTERM → 10s → SIGKILL)
	stopCmd := exec.CommandContext(ctx, "docker", "stop", "-t", fmt.Sprintf("%d", dockerStopTimeout), containerName)
	if out, err := stopCmd.CombinedOutput(); err != nil {
		log.Printf("docker stop %s: %v: %s", containerName, err, strings.TrimSpace(string(out)))
	}

	// Remove container
	rmCmd := exec.CommandContext(ctx, "docker", "rm", containerName)
	if out, err := rmCmd.CombinedOutput(); err != nil {
		log.Printf("docker rm %s: %v: %s", containerName, err, strings.TrimSpace(string(out)))
	}

	// Stop log streaming process
	r.mu.Lock()
	if cmd := r.logCmds[task.ID]; cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	delete(r.logCmds, task.ID)
	if b := r.stdoutLog[task.ID]; b != nil {
		b.Close()
	}
	if b := r.stderrLog[task.ID]; b != nil {
		b.Close()
	}
	delete(r.stdoutLog, task.ID)
	delete(r.stderrLog, task.ID)
	r.mu.Unlock()

	return nil
}

// Status checks if a Docker container is running
func (r *DockerRunner) Status(task *types.Task) (types.TaskState, error) {
	containerName := containerPrefix + task.ID

	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", containerName).Output()
	if err != nil {
		return types.TaskFailed, nil
	}

	if strings.TrimSpace(string(out)) == "true" {
		return types.TaskRunning, nil
	}
	return types.TaskFailed, nil
}

// GetStdout returns the stdout log broadcaster for a task
func (r *DockerRunner) GetStdout(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stdoutLog[taskID]
}

// GetStderr returns the stderr log broadcaster for a task
func (r *DockerRunner) GetStderr(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stderrLog[taskID]
}

// Cleanup removes all hop containers
func (r *DockerRunner) Cleanup() error {
	out, err := exec.Command("docker", "ps", "-a", "--filter", "name="+containerPrefix, "--format", "{{.Names}}").Output()
	if err != nil {
		return nil // docker might not be available
	}

	for _, name := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if name == "" {
			continue
		}
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
	return nil
}

// startLogStreaming streams container logs to broadcasters
func (r *DockerRunner) startLogStreaming(taskID, containerName string) {
	stdoutB := NewLogBroadcaster()
	stderrB := NewLogBroadcaster()

	cmd := exec.Command("docker", "logs", "-f", containerName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("Failed to get docker logs stdout pipe for %s: %v", taskID, err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("Failed to get docker logs stderr pipe for %s: %v", taskID, err)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start docker logs for %s: %v", taskID, err)
		return
	}

	r.mu.Lock()
	r.stdoutLog[taskID] = stdoutB
	r.stderrLog[taskID] = stderrB
	r.logCmds[taskID] = cmd
	r.mu.Unlock()

	go PipeReader(stdoutB, stdout)
	go PipeReader(stderrB, stderr)

	go func() {
		_ = cmd.Wait()
	}()
}
