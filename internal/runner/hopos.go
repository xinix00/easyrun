package runner

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"hop/internal/types"
	"hop/pkg/hopos"
)

// HopRunner runs tasks on HopOS: each task is a native Go image started on a
// dedicated CPU core (a "slot") with its own memory partition. HopOS enforces
// isolation and MemoryLimit in hardware, so — like DockerRunner and unlike
// ExecRunner — this runner only passes image + env + limits along and does no
// isolation plumbing of its own.
//
// Ports are not supported yet (per-slot networking is a HopOS fase-4 design);
// jobs with ports are rejected explicitly rather than half-working.

// SlotAppStatus values mirror hop-os/metal/layout control-page states.
const (
	SlotEmpty   = 0
	SlotBooting = 1
	SlotReady   = 2
	SlotExited  = 3
)

// SlotStatus is a point-in-time view of one slot.
type SlotStatus struct {
	CoreOn    bool
	App       uint64 // Slot* constants
	ExitCode  uint64
	Heartbeat uint64
	RAMSize   uint64
}

// SlotManager abstracts HopOS' slot primitives (hop-os/metal/slots). The
// bare-metal implementation calls that package directly; tests use a fake.
type SlotManager interface {
	// NumSlots returns the number of app slots (cores) on this node.
	NumSlots() int
	// CoreClass returns the core class of a slot ("big", "mid" or "small").
	CoreClass(slot int) string
	// Start loads a signed app image into the slot's partition, applies the
	// memory limit (0 = whole partition) and env, and wakes the core.
	Start(slot int, image []byte, memLimit uint64, env map[string]string) error
	// Stop asks the app to exit (kill flag) and waits until the core is off.
	Stop(slot int, timeout time.Duration) error
	// Status reports the slot's current state.
	Status(slot int) SlotStatus
	// Logs returns the slot's log stream (hop-ABI outbox). The channel is
	// closed when the slot stops.
	Logs(slot int) <-chan string
}

const hopStopTimeout = 10 * time.Second

// HopRunner implements Runner against a SlotManager.
type HopRunner struct {
	sm        hopos.SlotManager
	nodeAttrs map[string]string

	mu        sync.RWMutex
	slots     map[string]int // taskID -> slot
	inUse     map[int]string // slot -> taskID
	stdoutLog map[string]*LogBroadcaster
	stderrLog map[string]*LogBroadcaster
}

// NewHopRunner creates a runner on top of a SlotManager. nodeAttrs are
// injected as ER_ATTR_* env vars, matching the other runners.
func NewHopRunner(sm hopos.SlotManager, nodeAttrs map[string]string) *HopRunner {
	return &HopRunner{
		sm:        sm,
		nodeAttrs: nodeAttrs,
		slots:     make(map[string]int),
		inUse:     make(map[int]string),
		stdoutLog: make(map[string]*LogBroadcaster),
		stderrLog: make(map[string]*LogBroadcaster),
	}
}

// Run loads the job's artifact (the native app image) and starts it on a free
// slot. The slot number is recorded as task.Pid ("process id" = core index).
func (r *HopRunner) Run(job *types.Job, task *types.Task) error {
	if job.Image != "" {
		return errors.New("hop driver: containers are not supported on HopOS")
	}
	if len(job.Ports) > 0 {
		return errors.New("hop driver: ports are not supported yet (HopOS per-slot networking is pending)")
	}
	if len(job.Artifacts) != 1 {
		return errors.New("hop driver: exactly one artifact (the app image) is required")
	}
	if job.Artifacts[0].Extract != "" {
		return errors.New("hop driver: artifact must be a raw app image (no extract)")
	}

	image, err := r.fetchImage(&job.Artifacts[0])
	if err != nil {
		return fmt.Errorf("hop driver: fetch image: %w", err)
	}

	env := make(map[string]string, len(job.Env)+len(r.nodeAttrs))
	for k, v := range job.Env {
		env[k] = v
	}
	for _, kv := range AttrEnvVars(r.nodeAttrs) {
		if i := indexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}

	r.mu.Lock()
	slot, err := r.allocateSlotLocked(job.Tags["core-class"])
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.inUse[slot] = task.ID
	r.slots[task.ID] = slot
	r.mu.Unlock()

	if err := r.sm.Start(slot, image, job.MemoryLimit, env); err != nil {
		r.release(task.ID)
		return fmt.Errorf("hop driver: start slot %d: %w", slot, err)
	}

	stdout := NewLogBroadcaster()
	stderr := NewLogBroadcaster() // hop apps have a single log ring; stderr stays empty
	r.mu.Lock()
	r.stdoutLog[task.ID] = stdout
	r.stderrLog[task.ID] = stderr
	r.mu.Unlock()

	go func() {
		for line := range r.sm.Logs(slot) {
			_, _ = stdout.Write([]byte(line))
		}
	}()

	task.Pid = slot
	return nil
}

// Stop asks the slot to shut down and frees it.
func (r *HopRunner) Stop(task *types.Task) error {
	r.mu.RLock()
	slot, ok := r.slots[task.ID]
	r.mu.RUnlock()
	if !ok {
		if task.Pid > 0 {
			slot = task.Pid
		} else {
			return nil
		}
	}

	err := r.sm.Stop(slot, hopStopTimeout)
	r.release(task.ID)
	return err
}

// Status maps slot state onto task state.
func (r *HopRunner) Status(task *types.Task) (types.TaskState, error) {
	r.mu.RLock()
	slot, ok := r.slots[task.ID]
	r.mu.RUnlock()
	if !ok {
		if task.Pid == 0 {
			return types.TaskRunning, nil // still starting (image download)
		}
		slot = task.Pid
	}

	s := r.sm.Status(slot)
	switch {
	case s.CoreOn:
		return types.TaskRunning, nil
	case s.App == hopos.SlotExited && s.ExitCode == 0:
		return types.TaskStopped, nil
	default:
		return types.TaskFailed, nil
	}
}

// GetStdout returns the log broadcaster fed by the slot's hop-ABI log ring.
func (r *HopRunner) GetStdout(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stdoutLog[taskID]
}

// GetStderr returns an empty broadcaster (hop apps have a single log stream).
func (r *HopRunner) GetStderr(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stderrLog[taskID]
}

// Cleanup is a no-op: every HopOS start loads a fresh image into the slot
// partition, and after a node reboot all cores are off by construction.
func (r *HopRunner) Cleanup() error { return nil }

// allocateSlotLocked picks a free slot, honoring an optional core-class tag.
// Caller holds r.mu.
func (r *HopRunner) allocateSlotLocked(coreClass string) (int, error) {
	for slot := 1; slot <= r.sm.NumSlots(); slot++ {
		if _, busy := r.inUse[slot]; busy {
			continue
		}
		if coreClass != "" && r.sm.CoreClass(slot) != coreClass {
			continue
		}
		return slot, nil
	}
	if coreClass != "" {
		return 0, fmt.Errorf("hop driver: no free %q slot", coreClass)
	}
	return 0, errors.New("hop driver: no free slot")
}

func (r *HopRunner) release(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if slot, ok := r.slots[taskID]; ok {
		delete(r.inUse, slot)
		delete(r.slots, taskID)
	}
	delete(r.stdoutLog, taskID)
	delete(r.stderrLog, taskID)
}

// fetchImage downloads the artifact and returns the raw image bytes fully
// in memory — HopOS has no filesystem, and images are small (a few MB).
// http/https only for now; s3 support moves here when needed (see download_s3).
func (r *HopRunner) fetchImage(artifact *types.Artifact) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, artifact.URL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range artifact.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", artifact.URL, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
