package runner

import (
	"errors"
	"fmt"
	"log"
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
// Ports: every task gets its own network stack on HopOS' internal net; the
// node publishes each allocated port (name -> port) via stateless DNAT to
// the task's stack. The task binds the same port number it is published on
// (read from ER_PORT_<NAME>, injected below), matching the other runners.
//
// Volumes (shared path -> local path) pass through to HopOS: the task gets
// its own empty root plus exactly the mounted shared dirs, served by HOP's
// NVMe-backed storage layer over the hop-ABI. No memory is ever shared.

// hopStopTimeout is the cooperative window before Stop escalates to the
// stage-2 revoke. A healthy app parks within ~100ms of the kill-flag (the
// watch loop polls every 50ms), so 3s is generous; the old 10s made every
// uncooperative stop in a delete-storm burn 10 serialized seconds (measured
// 15-07: 127 deletes took tens of minutes).
const hopStopTimeout = 3 * time.Second

// hopStageTimeout is the NO-PROGRESS window while the apploader downloads the
// real image: it resets on every heartbeat tick (awaitStaged), so a slow shared
// link never false-fails a living loader. hopStageHardTimeout is the absolute
// cap for a loader that is alive but will never finish (stuck stream).
const (
	hopStageTimeout     = 120 * time.Second
	hopStageHardTimeout = 10 * time.Minute
)

// HopRunner implements Runner against a SlotManager.
type HopRunner struct {
	sm        hopos.SlotManager
	nodeAttrs map[string]string

	mu        sync.RWMutex
	slots     map[string]int // taskID -> slot
	inUse     map[int]string // slot -> taskID
	stdoutLog map[string]*LogBroadcaster
	stderrLog map[string]*LogBroadcaster
	// faultLogged guards the once-per-task failure diagnostic in Status:
	// Status is polled continuously, the WHY of a failure should be logged
	// exactly once. Cleared in release alongside the other task maps.
	faultLogged map[string]bool
	// stopping markeert tasks waarvoor Stop is begonnen. awaitStaged leest
	// dit (onder r.mu) i.p.v. task.State: dat veld muteert in de
	// agent-state-loop en hier raw pollen was een data race. Gewist in
	// release, samen met de andere task-maps.
	stopping map[string]bool
}

// NewHopRunner creates a runner on top of a SlotManager. nodeAttrs are
// injected as ER_ATTR_* env vars, matching the other runners.
func NewHopRunner(sm hopos.SlotManager, nodeAttrs map[string]string) *HopRunner {
	return &HopRunner{
		sm:          sm,
		nodeAttrs:   nodeAttrs,
		slots:       make(map[string]int),
		inUse:       make(map[int]string),
		stdoutLog:   make(map[string]*LogBroadcaster),
		stderrLog:   make(map[string]*LogBroadcaster),
		faultLogged: make(map[string]bool),
		stopping:    make(map[string]bool),
	}
}

// Run loads the job's artifact (the native app image) and starts it on a free
// slot. The slot number is recorded as task.Pid ("process id" = core index).
func (r *HopRunner) Run(job *types.Job, task *types.Task) error {
	if job.Image != "" {
		return errors.New("hop driver: containers are not supported on HopOS")
	}
	if len(job.Artifacts) != 1 {
		return errors.New("hop driver: exactly one artifact (the app image) is required")
	}
	if job.Artifacts[0].Extract != "" {
		return errors.New("hop driver: artifact must be a raw app image (no extract)")
	}

	env := make(map[string]string, len(job.Env)+len(r.nodeAttrs)+len(task.Ports))
	for k, v := range job.Env {
		env[k] = v
	}
	for _, kv := range AttrEnvVars(r.nodeAttrs) {
		if i := indexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}
	for _, kv := range PortEnvVars(task.Ports) {
		if i := indexByte(kv, '='); i > 0 {
			env[kv[:i]] = kv[i+1:]
		}
	}

	// cores = CPUShares/1024 (Docker/Nomad-conventie: 1024 shares = 1 core),
	// minimaal 1. Met cores > 1 draait de app SMP op één primair slot plus de
	// volgende cores-1 cores (gedeelde heap); HopOS brengt die transparant op.
	cores := job.CPUShares / 1024
	if cores < 1 {
		cores = 1
	}

	// Sharegroup (tag): coöperatieve core-deling op HopOS. Zonder de tag is elke
	// app een eigen SMP-eenheid die `cores` aaneengesloten cores dedicated pakt
	// (het bestaande gedrag). Mét de tag deelt de app een POOL van `cores` hele
	// cores met gelijk-getagde apps: dan reserveren we hier maar één kooi (HopOS
	// stapelt ze op de pool), draait de app zelf op één core, en is `cores` de
	// poolgrootte. Andere drivers (exec/docker) negeren de tag.
	sharegroup := job.Tags["sharegroup"]
	appCores, poolCores, allocCages := cores, 1, cores
	if sharegroup != "" {
		appCores, poolCores, allocCages = 1, cores, 1
	}

	// Kooi EERST alloceren, dán pas downloaden: op een volle node reject dit
	// meteen (geen vrije kooi) zonder één byte te trekken. Zo kan een storm
	// van jobs nooit meer images tegelijk laten downloaden dan er kooien zijn
	// — en met StartStream landt elke download rechtstreeks in de eigen
	// partitie i.p.v. de HOP-kern (geen core-0-OOM meer, gemeten 14-07).
	r.mu.Lock()
	slot, err := r.allocateSlotLocked(job.Tags["core-class"], allocCages)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	// De kooi(en) van deze app als bezet vastleggen (SMP: primair + secundairen;
	// sharegroup: één kooi — de pool-cores beheert HopOS, niet HOP's kooi-tabel).
	for c := slot; c < slot+allocCages; c++ {
		r.inUse[c] = task.ID
	}
	r.slots[task.ID] = slot
	r.mu.Unlock()

	// De app downloadt zijn EIGEN image: HOP laadt eerst de universele apploader
	// in de slot (lokaal, uit de cache), die op zijn eigen core+netstack de echte
	// image fetcht en HOP "staged" seint; HOP plaatst 'm (StartStaged) en
	// her-dispatcht de core. Zo draagt de node-netstack nooit alle downloads
	// tegelijk (geen core-0-OOM bij een storm) en raakt een kapotte image hooguit
	// dat ene slot. runViaLoader geeft de slot bij een fout al vrij.
	started, err := r.runViaLoader(job, task, slot, appCores, sharegroup, poolCores, env)
	if err != nil {
		return err
	}
	if !started {
		// Task tijdens staging gestopt; Stop() ruimt de slot op (of deed
		// dat al). HIER stoppen: broadcasters registreren of task.Pid
		// zetten zou naar een vrijgegeven (en mogelijk hergebruikte) slot
		// wijzen — de ghost-guard's Stop(task) killde daarmee via de
		// task.Pid-fallback de task van een ANDER die de slot al had.
		return nil
	}

	stdout := NewLogBroadcaster()
	stderr := NewLogBroadcaster() // hop apps have a single log ring; stderr stays empty
	r.mu.Lock()
	r.stdoutLog[task.ID] = stdout
	r.stderrLog[task.ID] = stderr
	r.mu.Unlock()

	// What kind of cage did this task land in? The node knows, and on a headless
	// board its serial console is the only other place it would say so — a line
	// that drops bytes at 115200. Putting it first in the task's own log makes it
	// arrive intact, and puts it where an operator already looks (`hop logs`).
	// Empty on architectures where the cage is a stage-2 map: there is nothing
	// there that the Fault* fields do not already carry.
	if cage := r.sm.Status(slot).Cage; cage != "" {
		_, _ = stdout.Write([]byte(cage))
	}

	go func() {
		for line := range r.sm.Logs(slot) {
			_, _ = stdout.Write([]byte(line))
		}
	}()

	task.Pid = slot
	return nil
}

// placementErr maps HopOS' "cannot place the cage" (hopos.ErrNoCapacity, raised
// by the node's PlaceCage) onto the driver-agnostic runner.ErrNoCapacity, so the
// agent can act on it without importing the HopOS contract: this driver knows
// HopOS, the agent only knows runners. Anything else passes through untouched.
func placementErr(err error) error {
	if errors.Is(err, hopos.ErrNoCapacity) {
		return fmt.Errorf("%w: %v", ErrNoCapacity, err)
	}
	return err
}

// runViaLoader realiseert "de app downloadt zijn eigen image": HOP laadt de
// universele apploader (ingebakken in de node) in de slot met de echte URL in de
// env; de loader fetcht op zíjn eigen core+netstack, seint "staged", en HOP
// plaatst de echte app (StartStaged) over de loader heen en her-dispatcht de core.
// started=false (zonder fout) betekent: task tijdens staging verwijderd — de
// slot is dan al vrijgegeven en de aanroeper mag NIETS meer aan de slot koppelen.
func (r *HopRunner) runViaLoader(job *types.Job, task *types.Task, slot, cores int, sharegroup string, poolCores int, env map[string]string) (started bool, err error) {
	// Fase 1: de apploader (1 core, geen mounts) met de echte image-URL in de
	// env. Hij deelt de partitie die straks de echte app krijgt (op MemoryLimit
	// gealloceerd — de echte app-parameters volgen in fase 2).
	lenv := make(map[string]string, len(env)+1)
	for k, v := range env {
		lenv[k] = v
	}
	lenv["HOP_IMAGE_URL"] = job.Artifacts[0].URL
	if err := r.sm.StartLoader(slot, job.MemoryLimit, sharegroup, poolCores, lenv); err != nil {
		r.release(task.ID)
		return false, fmt.Errorf("hop driver: start apploader slot %d: %w", slot, placementErr(err))
	}
	// Fase 2: wachten tot de loader de echte image gestaged heeft (of de task
	// mid-download verwijderd is), dan de echte app plaatsen — met de ÉCHTE
	// cores/volumes/ports.
	staged, err := r.awaitStaged(task, slot, hopStageTimeout)
	if err != nil {
		_ = r.sm.Stop(slot, hopStopTimeout)
		r.release(task.ID)
		return false, fmt.Errorf("hop driver: slot %d: %w", slot, err)
	}
	if !staged {
		// Task tijdens staging gestopt (delete/preemptie). Stop() is de
		// eigenaar van de opruiming (kill + release) — hier NIET nogmaals
		// stoppen of releasen: de slot kan al aan een andere task zijn.
		log.Printf("hop driver: task %.8s stopped during staging — skipping start", task.ID)
		return false, nil
	}
	if err := r.sm.StartStaged(slot, job.MemoryLimit, cores, env, job.Volumes, task.Ports); err != nil {
		_ = r.sm.Stop(slot, hopStopTimeout)
		r.release(task.ID)
		return false, fmt.Errorf("hop driver: place staged slot %d (%d cores): %w", slot, cores, err)
	}
	return true, nil
}

// awaitStaged waits until the apploader has staged the real image (SlotStaged);
// (false, nil) means the task was stopped mid-download (Stop owns the cleanup
// then — detected via stagingCancelled, NOT via task.State: that field is
// mutated by the agent state loop and reading it raw here was a data race).
//
// Its patience follows LIFE, not the clock: `timeout` is a
// no-progress window that RESETS as long as the loader's heartbeat advances.
// A slow download is not a failure — 127 loaders sharing one uplink (measured
// 15-07: the fixed 2m window produced false timeouts whose retries re-downloaded
// and starved the still-running transfers, a self-amplifying churn). A loader
// whose core DIED (cage fault) fails fast instead — with the why — rather than
// burning the full window. hopStageHardTimeout caps a live-but-stuck loader
// (e.g. a TCP stream that will never finish; http.Get has no own deadline).
func (r *HopRunner) awaitStaged(task *types.Task, slot int, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	hard := time.Now().Add(hopStageHardTimeout)
	var lastHB uint64
	for {
		if r.stagingCancelled(task.ID, slot) {
			return false, nil
		}
		s := r.sm.Status(slot)
		switch s.App {
		case hopos.SlotStaged:
			return true, nil
		case hopos.SlotExited:
			if r.stagingCancelled(task.ID, slot) {
				return false, nil // Stop killde de loader — geen echte fout
			}
			return false, fmt.Errorf("apploader exited before staging the image")
		}
		if !s.CoreOn {
			if r.stagingCancelled(task.ID, slot) {
				return false, nil // Stop parkeerde de core — geen echte fout
			}
			// Dead before staging: the cage parked it (fault) — no point
			// waiting out any window. Surface the why (ESR/FAR) right here.
			if s.FaultVec != 0 {
				return false, fmt.Errorf("apploader died before staging: stage-2 fault (vec %d, ESR %#x, FAR %#x)",
					s.FaultVec-1, s.FaultESR, s.FaultFAR)
			}
			if s.Cage != "" {
				return false, fmt.Errorf("apploader died before staging (core off): %s", s.Cage)
			}
			return false, fmt.Errorf("apploader died before staging (core off, no fault recorded)")
		}
		if s.Heartbeat != lastHB {
			lastHB = s.Heartbeat
			deadline = time.Now().Add(timeout) // leven gezien: geduld verlengd
		}
		now := time.Now()
		if now.After(hard) {
			return false, fmt.Errorf("apploader alive but did not stage within the hard cap %s", hopStageHardTimeout)
		}
		if now.After(deadline) {
			return false, fmt.Errorf("apploader did not stage the image within %s (no heartbeat progress)", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Stop asks the slot to shut down and frees it.
func (r *HopRunner) Stop(task *types.Task) error {
	r.mu.Lock()
	slot, ok := r.slots[task.ID]
	if ok {
		// Vlag vóór de (blokkerende) sm.Stop: een awaitStaged die nog op
		// deze task pollt ziet 'm en stapt er graceful uit — Stop ruimt op.
		r.stopping[task.ID] = true
	}
	r.mu.Unlock()
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
	// task.Pid wordt pas gezet als Run VOLLEDIG klaar is: apploader laden →
	// image downloaden (app-kant) → stagen → de echte app plaatsen. Zolang het 0
	// is, is de task nog aan het starten — rapporteer Running. Anders ziet de
	// monitor de kort geparkeerde apploader (SlotStaged: CoreOn=false) als een
	// crash en killt 'm mid-start (gemeten 14-07: 3/5 taken sneuvelden zo).
	if task.Pid == 0 {
		return types.TaskRunning, nil
	}
	r.mu.RLock()
	slot, ok := r.slots[task.ID]
	r.mu.RUnlock()
	if !ok {
		slot = task.Pid
	}

	s := r.sm.Status(slot)
	switch {
	case s.CoreOn:
		return types.TaskRunning, nil
	case s.App == hopos.SlotExited && s.ExitCode == 0:
		return types.TaskStopped, nil
	default:
		// The WHY, once: a stage-2 fault (cage violation / hard-kill, with
		// ESR/FAR from the EL2 vectors) or a nonzero exit. Without this the
		// reason lives only on the node's console — invisible on a headless
		// Altra. Diagnostics only; the state machine stays CoreOn/Exited.
		r.mu.Lock()
		if !r.faultLogged[task.ID] {
			r.faultLogged[task.ID] = true
			switch {
			case s.FaultVec != 0:
				log.Printf("hop task %s failed: stage-2 fault on slot %d (vec %d, ESR %#x, FAR %#x)",
					task.ID, slot, s.FaultVec-1, s.FaultESR, s.FaultFAR)
			case s.Cage != "":
				log.Printf("hop task %s failed: exit code %d (slot %d) — %s", task.ID, s.ExitCode, slot, s.Cage)
			case s.ExitCode != 0:
				log.Printf("hop task %s failed: exit code %d (slot %d)", task.ID, s.ExitCode, slot)
			}
		}
		r.mu.Unlock()
		return types.TaskFailed, nil
	}
}

// Usage returns the task's CPU usage and actual memory draw, both reported
// by the node/app themselves: cpuPct is the percentage of the task's OWN
// cores (0-100, from the slot's idle-tick counter — an idle core ticks at
// event-stream tempo, a computing core doesn't; negative while no sample
// window has completed yet), memBytes is Go MemStats.Sys (refreshed ~2s over
// the control page next to the heartbeat). ok is false while no memory has
// been reported yet (task still starting, or a pre-MemSys image). The agent
// monitor feeds this into the same CPUPercent/MemPercent pipeline the
// exec/docker drivers use — so HOP knows what a task USES, not just what it
// was allotted.
func (r *HopRunner) Usage(task *types.Task) (cpuPct float64, memBytes uint64, ok bool) {
	if task.Pid == 0 {
		return -1, 0, false // still starting (loader/staging phase)
	}
	r.mu.RLock()
	slot, found := r.slots[task.ID]
	r.mu.RUnlock()
	if !found {
		slot = task.Pid
	}
	s := r.sm.Status(slot)
	return float64(s.CPUPct), s.MemSys, s.MemSys != 0
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

// allocateSlotLocked finds a run of `cores` contiguous free slots (each
// honoring an optional core-class tag) and returns the first (the primary).
// For cores == 1 this is just the first free slot; for an SMP app the run is
// the primary plus its secondary cores. Caller holds r.mu.
func (r *HopRunner) allocateSlotLocked(coreClass string, cores int) (int, error) {
	// Cage IDs are not a leader-facing capacity: HopOS stacks more cages than it
	// has cores (sharegroups) and enforces its own hard ceiling, so HOP just
	// hands out the next free ID and lets the node place it or reject — the real
	// core/RAM walls live in admission (agent) and PlaceCage (node). A
	// class-constrained job is the exception: a class only maps to real cores,
	// so it is bounded by the node's core count.
	limit := -1 // unbounded — the first free run always terminates the search
	if coreClass != "" {
		limit = r.sm.NumCores()
	}
	for slot := 1; limit < 0 || slot+cores-1 <= limit; slot++ {
		ok := true
		for c := slot; c < slot+cores; c++ {
			if _, busy := r.inUse[c]; busy {
				ok = false
				break
			}
			if coreClass != "" && r.sm.CoreClass(c) != coreClass {
				ok = false
				break
			}
		}
		if ok {
			return slot, nil
		}
	}
	return 0, fmt.Errorf("hop driver: no %d contiguous free %q cores", cores, coreClass)
}

func (r *HopRunner) release(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Free every core held by this task (an SMP app holds several).
	for slot, id := range r.inUse {
		if id == taskID {
			delete(r.inUse, slot)
		}
	}
	delete(r.slots, taskID)
	delete(r.stdoutLog, taskID)
	delete(r.stderrLog, taskID)
	delete(r.faultLogged, taskID)
	delete(r.stopping, taskID)
}

// stagingCancelled meldt of de task tijdens staging is gestopt: de stop-vlag
// staat (Stop is bezig) óf de slot-claim is al weg (Stop is klaar en heeft
// released). In beide gevallen is Stop de eigenaar van de opruiming — de
// staging-flow mag de slot dan niet meer aanraken (hij kan al van een
// ANDERE task zijn).
func (r *HopRunner) stagingCancelled(taskID string, slot int) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.stopping[taskID] {
		return true
	}
	s, ok := r.slots[taskID]
	return !ok || s != slot
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
