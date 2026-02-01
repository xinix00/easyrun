//go:build darwin

package runner

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"easyrun/internal/types"
)

// wrapCommand on macOS wraps the command with ulimit for memory limiting
func (r *ProcessRunner) wrapCommand(command string, memoryLimit uint64) string {
	if memoryLimit == 0 {
		return command
	}
	// ulimit -v sets virtual memory limit in KB
	return fmt.Sprintf("ulimit -v %d; exec %s", memoryLimit/1024, command)
}

// applyMemoryLimit on macOS is a no-op
// Memory limiting is done via ulimit in wrapCommand (before exec)
func (r *ProcessRunner) applyMemoryLimit(pid int, memoryLimit uint64) {
	// Already handled via ulimit wrapper
}

// mountVolume on macOS uses symlinks
func (r *ProcessRunner) mountVolume(hostPath, targetPath string) error {
	return os.Symlink(hostPath, targetPath)
}

// unmountVolume removes the symlink
func (r *ProcessRunner) unmountVolume(targetPath string) error {
	return os.Remove(targetPath)
}

// setupCommand configures the command with optional sandbox isolation
func (r *ProcessRunner) setupCommand(job *types.Job, taskDir string, portEnvVars []string) *exec.Cmd {
	command := r.wrapCommand(job.Command, job.MemoryLimit)

	var cmd *exec.Cmd
	if r.config.Isolate {
		// Generate sandbox profile
		profile := r.generateSandboxProfile(taskDir, job)
		profilePath := filepath.Join(taskDir, "sandbox.sb")
		os.WriteFile(profilePath, []byte(profile), 0644)

		// Wrap with sandbox-exec
		cmd = exec.Command("sandbox-exec", "-f", profilePath, "/bin/sh", "-c", command)
		cmd.Dir = taskDir
		cmd.Env = []string{
			fmt.Sprintf("HOME=%s", taskDir),
			fmt.Sprintf("TMPDIR=%s/tmp", taskDir),
			"PATH=/usr/local/bin:/usr/bin:/bin",
		}
	} else {
		// Non-isolated mode
		cmd = exec.Command("/bin/sh", "-c", command)
		cmd.Dir = taskDir
		cmd.Env = []string{
			fmt.Sprintf("HOME=%s", taskDir),
			fmt.Sprintf("TMPDIR=%s/tmp", taskDir),
			"PATH=/usr/local/bin:/usr/bin:/bin",
		}
	}

	cmd.Env = append(cmd.Env, portEnvVars...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	for k, v := range job.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	return cmd
}

// generateSandboxProfile creates a sandbox profile for the task
func (r *ProcessRunner) generateSandboxProfile(taskDir string, job *types.Job) string {
	var sb strings.Builder

	sb.WriteString("(version 1)\n")
	sb.WriteString("(deny default)\n\n")

	// Allow basic operations
	sb.WriteString("; Allow process execution\n")
	sb.WriteString("(allow process-exec)\n")
	sb.WriteString("(allow process-fork)\n")
	sb.WriteString("(allow signal)\n\n")

	// Allow sysctl reads (needed for many programs)
	sb.WriteString("(allow sysctl-read)\n\n")

	// Allow task directory full access
	sb.WriteString("; Task directory - full access\n")
	sb.WriteString(fmt.Sprintf("(allow file-read* file-write* (subpath \"%s\"))\n\n", taskDir))

	// Allow system libraries (read-only)
	sb.WriteString("; System libraries - read only\n")
	sb.WriteString("(allow file-read* (subpath \"/usr/lib\"))\n")
	sb.WriteString("(allow file-read* (subpath \"/System/Library\"))\n")
	sb.WriteString("(allow file-read* (subpath \"/Library/Frameworks\"))\n")
	sb.WriteString("(allow file-read* (subpath \"/usr/share\"))\n")
	sb.WriteString("(allow file-read* (literal \"/dev/null\"))\n")
	sb.WriteString("(allow file-read* (literal \"/dev/urandom\"))\n")
	sb.WriteString("(allow file-read* (literal \"/etc/resolv.conf\"))\n\n")

	// Allow volumes (read-only by default)
	if len(job.Volumes) > 0 {
		sb.WriteString("; Volumes - read access\n")
		for hostPath := range job.Volumes {
			sb.WriteString(fmt.Sprintf("(allow file-read* (subpath \"%s\"))\n", hostPath))
		}
		sb.WriteString("\n")
	}

	// Allow network by default (can be restricted later with job.Network field)
	sb.WriteString("; Network access\n")
	sb.WriteString("(allow network*)\n")

	return sb.String()
}

// linkLibraries is a no-op on macOS with sandbox (not needed)
func (r *ProcessRunner) linkLibraries(taskDir string) {
	// Sandbox allows access to system libraries directly
}
