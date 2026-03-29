package agent

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// getProcessUsage returns cumulative CPU time (seconds) and RSS memory (bytes) for a PID.
// Uses ps which works on both Linux and macOS.
func getProcessUsage(pid int) (cpuSeconds float64, memBytes uint64, err error) {
	out, err := exec.Command("ps", "-o", "cputime=,rss=", "-p", fmt.Sprintf("%d", pid)).Output()
	if err != nil {
		return 0, 0, err
	}

	fields := strings.Fields(strings.TrimSpace(string(out)))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected ps output: %q", string(out))
	}

	cpuSeconds, err = parseCPUTime(fields[0])
	if err != nil {
		return 0, 0, fmt.Errorf("parse cputime %q: %w", fields[0], err)
	}

	rssKB, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse rss %q: %w", fields[1], err)
	}

	return cpuSeconds, rssKB * 1024, nil
}

// parseCPUTime parses ps cputime format: "HH:MM:SS" or "MM:SS" or "MM:SS.xx"
func parseCPUTime(s string) (float64, error) {
	var h, m, sec float64
	if n, _ := fmt.Sscanf(s, "%f:%f:%f", &h, &m, &sec); n == 3 {
		return h*3600 + m*60 + sec, nil
	}
	if n, _ := fmt.Sscanf(s, "%f:%f", &m, &sec); n == 2 {
		return m*60 + sec, nil
	}
	return 0, fmt.Errorf("unexpected format: %q", s)
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
