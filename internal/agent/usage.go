package agent

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

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
