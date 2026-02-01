//go:build linux

package runner

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"easyrun/internal/types"
)

const cgroupBase = "/sys/fs/cgroup/easyrun"

// wrapCommand on Linux returns the command as-is
// Memory limiting is done via cgroups after process start
func (r *ProcessRunner) wrapCommand(command string, memoryLimit uint64) string {
	return command
}

// applyMemoryLimit applies memory limits using cgroups v2
func (r *ProcessRunner) applyMemoryLimit(pid int, memoryLimit uint64) {
	cgroupPath := fmt.Sprintf("%s/%d", cgroupBase, pid)

	if err := os.MkdirAll(cgroupPath, 0755); err != nil {
		fmt.Printf("Warning: failed to create cgroup: %v\n", err)
		return
	}

	memMaxPath := filepath.Join(cgroupPath, "memory.max")
	if err := os.WriteFile(memMaxPath, []byte(fmt.Sprintf("%d", memoryLimit)), 0644); err != nil {
		fmt.Printf("Warning: failed to set memory.max: %v\n", err)
		return
	}

	procsPath := filepath.Join(cgroupPath, "cgroup.procs")
	if err := os.WriteFile(procsPath, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
		fmt.Printf("Warning: failed to add process to cgroup: %v\n", err)
	}
}

// mountVolume bind-mounts a host path into the task directory
func (r *ProcessRunner) mountVolume(hostPath, targetPath string) error {
	return syscall.Mount(hostPath, targetPath, "", syscall.MS_BIND, "")
}

// unmountVolume cleans up a mounted volume
func (r *ProcessRunner) unmountVolume(targetPath string) error {
	return syscall.Unmount(targetPath, 0)
}

// setupCommand configures the command with optional chroot isolation
func (r *ProcessRunner) setupCommand(job *types.Job, taskDir string, portEnvVars []string) *exec.Cmd {
	command := r.wrapCommand(job.Command, job.MemoryLimit)
	cmd := exec.Command("/bin/sh", "-c", command)

	if r.config.Isolate {
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
		// Non-isolated mode
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

	return cmd
}

// linkLibraries symlinks required libraries for chroot on Linux
func (r *ProcessRunner) linkLibraries(taskDir string) {
	// Link common library paths
	libs := []string{
		"/lib",
		"/lib64",
		"/usr/lib",
	}
	for _, lib := range libs {
		if _, err := os.Stat(lib); err == nil {
			os.Symlink(lib, filepath.Join(taskDir, lib))
		}
	}
}
