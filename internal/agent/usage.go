package agent

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"hop/internal/types"
)

// refreshMeminfo rewrites the per-task synthetic /proc/meminfo with the
// freshly measured RSS so readers inside the chroot see live MemFree
// instead of the static snapshot from start. Safe on any platform/config:
// when fakeMeminfo didn't write the file (macOS, Isolate=false, no
// MemoryLimit), os.Stat fails and we return without touching anything.
func (a *Agent) refreshMeminfo(task *types.Task, used uint64) {
	if task.MemoryLimit == 0 {
		return
	}
	base := a.config.Paths.RootfsBase
	if base == "" {
		base = "/tmp/hop"
	}
	src := filepath.Join(base, task.ID, ".hop-meminfo")
	if _, err := os.Stat(src); err != nil {
		return
	}
	if used > task.MemoryLimit {
		used = task.MemoryLimit
	}
	free := task.MemoryLimit - used
	content := fmt.Sprintf(
		"MemTotal:       %d kB\n"+
			"MemFree:        %d kB\n"+
			"MemAvailable:   %d kB\n"+
			"Buffers:               0 kB\n"+
			"Cached:                0 kB\n"+
			"SwapTotal:             0 kB\n"+
			"SwapFree:              0 kB\n",
		task.MemoryLimit/1024, free/1024, free/1024,
	)
	// Write through the source path — the bind in the chroot points at this
	// same inode, so the next read on /proc/meminfo inside the chroot sees
	// the new content. Atomic-rename would break the bind (new inode).
	_ = os.WriteFile(src, []byte(content), 0644)
}

// getDockerUsage returns CPU% and Mem% of total host for a docker container.
// Uses docker stats with percentage format to avoid parsing human-readable byte strings.
func getDockerUsage(taskID string) (cpuPercent float64, memPercent float64, err error) {
	containerName := "hop-" + taskID
	out, err := exec.Command("docker", "stats", "--no-stream", "--format",
		"{{.CPUPerc}} {{.MemPerc}}", containerName).Output()
	if err != nil {
		return 0, 0, err
	}

	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected docker stats output: %q", string(out))
	}

	// Parse CPU% (e.g. "142.50%")
	cpuPercent, err = strconv.ParseFloat(strings.TrimSuffix(fields[0], "%"), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse cpu %q: %w", fields[0], err)
	}

	// Parse Mem% (e.g. "12.50%")
	memPercent, err = strconv.ParseFloat(strings.TrimSuffix(fields[1], "%"), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse mem %q: %w", fields[1], err)
	}

	return cpuPercent, memPercent, nil
}
