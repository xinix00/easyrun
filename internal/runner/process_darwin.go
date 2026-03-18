//go:build darwin

package runner

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"easyrun/internal/types"
)

// wrapCommand on macOS wraps the command with ulimit for memory limiting
func (r *ExecRunner) wrapCommand(command string, memoryLimit uint64) string {
	if memoryLimit == 0 {
		return command
	}
	// ulimit -v sets virtual memory limit in KB
	return fmt.Sprintf("ulimit -v %d; exec %s", memoryLimit/1024, command)
}

// applyMemoryLimit on macOS is a no-op
// Memory limiting is done via ulimit in wrapCommand (before exec)
func (r *ExecRunner) applyMemoryLimit(pid int, memoryLimit uint64) {
	// Already handled via ulimit wrapper
}

// mountVolume on macOS uses symlinks
func (r *ExecRunner) mountVolume(hostPath, targetPath string) error {
	return os.Symlink(hostPath, targetPath)
}

// unmountVolume removes the symlink
func (r *ExecRunner) unmountVolume(targetPath string) error {
	return os.Remove(targetPath)
}

// setupCommand configures the command with optional sandbox isolation
func (r *ExecRunner) setupCommand(job *types.Job, taskDir string, portEnvVars []string) *exec.Cmd {
	command := r.wrapCommand(job.Command, job.MemoryLimit)

	var cmd *exec.Cmd
	if r.config.Isolate {
		// Generate sandbox profile
		profile := r.generateSandboxProfile(taskDir, job)
		profilePath := filepath.Join(taskDir, "sandbox.sb")
		_ = os.WriteFile(profilePath, []byte(profile), 0644)
		log.Printf("Sandbox profile written to %s", profilePath)

		// Wrap with sandbox-exec
		// Use cd in shell command instead of cmd.Dir for sandbox compatibility
		shellCmd := fmt.Sprintf("cd %s && %s", taskDir, command)
		cmd = exec.Command("sandbox-exec", "-f", profilePath, "/bin/sh", "-c", shellCmd)
		log.Printf("Executing command: sandbox-exec -f %s /bin/sh -c '%s'", profilePath, shellCmd)
	} else {
		cmd = exec.Command("/bin/sh", "-c", command)
		cmd.Dir = taskDir
	}

	cmd.Env = append([]string{
		fmt.Sprintf("HOME=%s", taskDir),
		fmt.Sprintf("TMPDIR=%s/tmp", taskDir),
		"PATH=/usr/local/bin:/usr/bin:/bin",
	}, portEnvVars...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	for k, v := range job.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = append(cmd.Env, AttrEnvVars(r.config.NodeAttrs)...)

	return cmd
}

// generateSandboxProfile creates a sandbox profile for the task
func (r *ExecRunner) generateSandboxProfile(taskDir string, job *types.Job) string {
	// Convert taskDir to absolute path for sandbox
	absTaskDir, err := filepath.Abs(taskDir)
	if err != nil {
		absTaskDir = taskDir
	}

	var sb strings.Builder

	sb.WriteString("(version 1)\n")
	// Use (allow default) for simplicity - dyld needs many permissions
	// This is similar to Apple's built-in profiles like "no-network"
	sb.WriteString("(allow default)\n\n")

	// Add task directory comment for debugging
	sb.WriteString(fmt.Sprintf("; Task directory: %s\n\n", absTaskDir))

	// Allow volumes (if specified)
	if len(job.Volumes) > 0 {
		sb.WriteString("; Volumes\n")
		for hostPath := range job.Volumes {
			sb.WriteString(fmt.Sprintf("(allow file-read* file-write* (subpath \"%s\"))\n", hostPath))
		}
		sb.WriteString("\n")
	}

	// Network can be restricted later with job.Network field if needed
	// For now (allow default) already includes network access

	return sb.String()
}

// linkLibraries is a no-op on macOS with sandbox (not needed)
func (r *ExecRunner) linkLibraries(taskDir string) {
	// Sandbox allows access to system libraries directly
}
