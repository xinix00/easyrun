//go:build linux

package runner

import (
	"fmt"
	"os"
	"path/filepath"
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
