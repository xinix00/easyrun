//go:build linux

package agent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// userHZ is the kernel's clock-tick rate (sysconf(_SC_CLK_TCK)). Hardcoded
// to 100 — the value on every mainstream Linux build. Reading it via cgo
// would drag CGO into a pure-Go binary just to confirm a constant nobody
// changes.
const userHZ = 100

// getProcessUsage returns aggregated CPU and RSS for the task's process group.
// task.Pid is the /bin/sh wrapper (Setpgid=true makes it the pgrp leader);
// the real workload lives in its descendants. Summing the whole pgrp gives
// us "what is this task doing" instead of "what is the idle sh doing".
func getProcessUsage(pid int) (cpuSeconds float64, memBytes uint64, err error) {
	pids := pidsInGroup(pid)
	if len(pids) == 0 {
		pids = []int{pid}
	}
	var ticks, rssPages uint64
	for _, p := range pids {
		u, r, err := readPidStat(p)
		if err != nil {
			continue // process may have exited mid-scan; ignore
		}
		ticks += u
		rssPages += r
	}
	return float64(ticks) / float64(userHZ), rssPages * uint64(os.Getpagesize()), nil
}

// readPidStat parses utime+stime (ticks) and rss (pages) from /proc/<pid>/stat.
func readPidStat(pid int) (cpuTicks uint64, rssPages uint64, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	// `comm` is wrapped in parens and may itself contain spaces or ')'. The
	// safe split is on the LAST ')' — fields after it are whitespace-separated.
	s := string(data)
	end := strings.LastIndex(s, ")")
	if end < 0 || end+1 >= len(s) {
		return 0, 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(s[end+1:])
	// proc(5): 14=utime, 15=stime, 24=rss. After dropping pid+comm+state
	// the offsets here become 11/12/21.
	if len(fields) < 22 {
		return 0, 0, fmt.Errorf("/proc/%d/stat: only %d fields after comm", pid, len(fields))
	}
	utime, _ := strconv.ParseUint(fields[11], 10, 64)
	stime, _ := strconv.ParseUint(fields[12], 10, 64)
	rss, _ := strconv.ParseUint(fields[21], 10, 64)
	return utime + stime, rss, nil
}

// pidsInGroup walks /proc once and returns every PID whose process group
// equals pgid. proc(5) field 5 is pgrp; after the closing ')' it's at index 2.
func pidsInGroup(pgid int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // not a PID dir
		}
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			continue
		}
		s := string(data)
		end := strings.LastIndex(s, ")")
		if end < 0 || end+1 >= len(s) {
			continue
		}
		fields := strings.Fields(s[end+1:])
		if len(fields) < 3 {
			continue
		}
		if pg, err := strconv.Atoi(fields[2]); err == nil && pg == pgid {
			pids = append(pids, pid)
		}
	}
	return pids
}
