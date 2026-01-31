//go:build darwin

package runner

import (
	"fmt"
	"os"
	"path/filepath"
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

// linkLibraries symlinks required libraries for chroot on macOS
func (r *ProcessRunner) linkLibraries(taskDir string) {
	// macOS uses dyld and /usr/lib/dyld
	os.MkdirAll(filepath.Join(taskDir, "usr", "lib"), 0755)
	os.Symlink("/usr/lib/dyld", filepath.Join(taskDir, "usr", "lib", "dyld"))

	// Link common library paths
	os.Symlink("/usr/lib/libSystem.B.dylib", filepath.Join(taskDir, "usr", "lib", "libSystem.B.dylib"))
}
