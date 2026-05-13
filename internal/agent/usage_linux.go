//go:build linux

package agent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// userHZ is the kernel's clock-tick rate (sysconf(_SC_CLK_TCK)). Hardcoded to
// 100 — the value on every mainstream Linux build. Reading it via cgo would
// drag CGO into a pure-Go binary just to confirm a constant nobody changes.
const userHZ = 100

// getProcessUsage reads /proc/<pid>/stat directly. /proc avoids the procps
// dependency that `ps` would impose on minimal Debian / Alpine installs, and
// gives us monotonic CPU counters from boot rather than ps's human-formatted
// cputime string that rounds to whole seconds.
func getProcessUsage(pid int) (cpuSeconds float64, memBytes uint64, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}

	// The `comm` field is wrapped in parentheses and may itself contain spaces
	// or even ')'. The safe split is on the LAST ')' — fields after it are
	// whitespace-separated.
	s := string(data)
	end := strings.LastIndex(s, ")")
	if end < 0 || end+1 >= len(s) {
		return 0, 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(s[end+1:])

	// proc(5) field numbers (1-indexed): 14=utime, 15=stime, 24=rss. After
	// dropping pid+comm+state (the first three) the offsets here become 11/12/21.
	if len(fields) < 22 {
		return 0, 0, fmt.Errorf("/proc/%d/stat: only %d fields after comm", pid, len(fields))
	}

	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("utime %q: %w", fields[11], err)
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("stime %q: %w", fields[12], err)
	}
	rssPages, err := strconv.ParseUint(fields[21], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("rss %q: %w", fields[21], err)
	}

	cpuSeconds = float64(utime+stime) / float64(userHZ)
	memBytes = rssPages * uint64(os.Getpagesize())
	return cpuSeconds, memBytes, nil
}
