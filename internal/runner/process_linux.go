//go:build linux

package runner

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"hop/internal/types"
)

const cgroupBase = "/sys/fs/cgroup/hop"

// wrapCommand on Linux returns the command as-is
// Memory limiting is done via cgroups after process start
func (r *ExecRunner) wrapCommand(command string, memoryLimit uint64) string {
	return command
}

// applyMemoryLimit applies memory limits using cgroups v2
func (r *ExecRunner) applyMemoryLimit(pid int, memoryLimit uint64) {
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
func (r *ExecRunner) mountVolume(hostPath, targetPath string) error {
	return syscall.Mount(hostPath, targetPath, "", syscall.MS_BIND, "")
}

// unmountVolume cleans up a mounted volume
func (r *ExecRunner) unmountVolume(targetPath string) error {
	return syscall.Unmount(targetPath, 0)
}

// setupCommand configures the command with optional namespace isolation
func (r *ExecRunner) setupCommand(job *types.Job, taskDir string, portEnvVars []string) *exec.Cmd {
	command := r.wrapCommand(job.Command, job.MemoryLimit)
	cmd := exec.Command("/bin/sh", "-c", command)

	if r.config.Isolate {
		// Full isolation: chroot + namespaces (container-like)
		cmd.Dir = "/"
		cmd.Env = []string{
			"HOME=/",
			"TMPDIR=/tmp",
			"PATH=/bin:/usr/bin",
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Chroot:  taskDir,
			Setpgid: true,
			Cloneflags: syscall.CLONE_NEWPID | // Own PID namespace (PID 1 inside)
				syscall.CLONE_NEWNS | // Own mount namespace
				syscall.CLONE_NEWUTS | // Own hostname
				syscall.CLONE_NEWIPC, // Own IPC namespace
			// Note: CLONE_NEWNET omitted - requires veth setup
		}
	} else {
		// Non-isolated mode
		sysProcAttr := &syscall.SysProcAttr{
			Setpgid: true,
		}
		if job.User != "" {
			cred, _, err := lookupCredential(job.User)
			if err != nil {
				log.Printf("Warning: %v, running as current user", err)
			} else {
				sysProcAttr.Credential = cred
			}
		}
		cmd.Dir = taskDir
		cmd.Env = []string{
			fmt.Sprintf("HOME=%s", taskDir),
			fmt.Sprintf("TMPDIR=%s/tmp", taskDir),
			"PATH=/usr/local/bin:/usr/bin:/bin",
		}
		cmd.SysProcAttr = sysProcAttr
	}

	cmd.Env = append(cmd.Env, portEnvVars...)
	for k, v := range job.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = append(cmd.Env, AttrEnvVars(r.config.NodeAttrs)...)

	return cmd
}

// linkLibraries symlinks required libraries for chroot on Linux
func (r *ExecRunner) linkLibraries(taskDir string) {
	// Link common library paths
	libs := []string{
		"/lib",
		"/lib64",
		"/usr/lib",
	}
	for _, lib := range libs {
		if _, err := os.Stat(lib); err == nil {
			_ = os.Symlink(lib, filepath.Join(taskDir, lib))
		}
	}
}
