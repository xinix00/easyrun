// Package hopos defines the public contract between HOP and HopOS: the
// SlotManager interface that HopOS' slot primitives implement and HOP's
// HopRunner consumes. It lives outside internal/ so the HopOS repository can
// implement it (hop-os/metal/slotmgr wraps metal/slots).
package hopos

import (
	"errors"
	"time"
)

// ErrNoCapacity is what StartLoader/StartStaged wrap when the node cannot
// PLACE the cage: no free app core for a dedicated job, or not enough free
// cores to open the requested sharegroup pool. It is emphatically not a crash
// — the job is fine and will run as soon as a core frees up (or right away on
// another node), so HOP must reject it back to pending instead of restart-
// looping it. Restarting an unplaceable job is a storm: every retry re-downloads
// the image and re-fails, and the churn starves the tasks that DO run.
var ErrNoCapacity = errors.New("hopos: no free core to place the cage")

// App status values, mirroring hop-os/metal/layout control-page states.
const (
	SlotEmpty   = 0
	SlotBooting = 1
	SlotReady   = 2
	SlotExited  = 3
	SlotStaged  = 4 // apploader downloaded the real image and parked; awaiting StartStaged
)

// SlotStatus is a point-in-time view of one slot.
type SlotStatus struct {
	CoreOn    bool
	App       uint64 // Slot* constants
	ExitCode  uint64
	Heartbeat uint64
	RAMSize   uint64
	MemSys    uint64 // actual memory draw reported by the app (Go MemStats.Sys); 0 = not reported yet

	// CPUPct is the slot's CPU usage as a percentage of its OWN cores
	// (0-100, SMP-normalized), derived from the idle-tick counter every app
	// publishes on its control page: an idle core ticks at event-stream
	// tempo, a computing core doesn't — usage = 1 - measured/expected.
	// -1 = unknown (slot starting, first sample window, or no counter).
	CPUPct int

	// Diagnostics for an involuntary end, written by HopOS' EL2 vectors: a
	// stage-2 fault (cage violation) or HOP's hard-kill. FaultVec 0 = no
	// fault seen; nonzero = vector index + 1, with ESR/FAR then valid. Not
	// state-machine input — the runner logs it so operators see WHY a task
	// failed, which is otherwise invisible on a headless node.
	FaultVec uint64
	FaultESR uint64
	FaultFAR uint64

	// Cage is the node's own one-line account of the slot's cage, or empty when
	// there is nothing to say. Same reason the Fault* fields exist — a headless
	// node is otherwise silent — but for the architectures where the cage is not
	// a stage-2 map: what the loader stub reached, and what kind of hart is
	// underneath. It travels over the network, which the node's serial console
	// cannot be trusted to do: at 115200 that line drops bytes, and a mangled
	// hex value is worse than none.
	//
	// Diagnostics only, like Fault*: never state-machine input.
	Cage string
}

// SlotManager abstracts HopOS' slot primitives (hop-os/metal/slots). The
// bare-metal implementation calls that package directly; tests use a fake.
type SlotManager interface {
	// NumCores is the node's usable app-core count — the only capacity HOP
	// schedules and reports against (a 4-core Pi with core 0 running HOP => 3).
	// The number of cages (slot IDs) a node can hold is deliberately NOT in
	// this contract: sharegroups stack more cages than cores, HopOS enforces
	// its own hard ceiling, and HOP simply tries to place a job — the node
	// accepts it or it doesn't.
	NumCores() int
	// CoreClass returns the core class of a slot ("big", "mid" or "small").
	CoreClass(slot int) string
	// StartLoader is phase 1 of the two-phase load: it loads the universal
	// apploader — a small image baked into the node — into the slot on one core,
	// with env (including HOP_IMAGE_URL, the real app image). The apploader then
	// downloads that image on its OWN core and network stack, straight into the
	// top of its own partition, and signals SlotStaged. This is how the download
	// moves off the node's single netstack: one connection per app core instead
	// of every image funnelling through core 0 (which OOM'd the kernel heap).
	// memLimit sizes the partition the real app reuses in phase 2.
	//
	// sharegroup + poolCores drive cooperative core-sharing (HopOS-only; other
	// drivers ignore it). sharegroup == "" places the slot on its own dedicated
	// core (the default — whole cores, no sharing). A non-empty sharegroup packs
	// this slot onto a pool of poolCores whole cores shared with same-named
	// slots; poolCores is the pool size in whole cores (first job of a group
	// wins). It comes from the job's "sharegroup" tag and CPUShares — HopOS does
	// the packing, HOP just forwards the intent.
	StartLoader(slot int, memLimit uint64, sharegroup string, poolCores int, env map[string]string) error
	// StartStaged is phase 2: the apploader has staged the real image in the top
	// of its own partition (SlotStaged). StartStaged places it over the loader
	// and re-dispatches the parked core on the real app, with the real cores,
	// volumes and ports. It reuses the partition allocated in phase 1, so no extra
	// slot or pool memory is consumed. cores > 1 gives the app SMP on the primary
	// slot plus the next cores-1 cores, sharing one heap (the app is oblivious and
	// simply sees GOMAXPROCS=cores). mounts is the job's volume table (shared path
	// -> local path); ports (name -> port) are published on the node IP via
	// stateless DNAT to the task's per-slot stack.
	StartStaged(slot int, memLimit uint64, cores int, env map[string]string, mounts map[string]string, ports map[string]int) error
	// Stop asks the app to exit (kill flag) and waits until the core is off.
	Stop(slot int, timeout time.Duration) error
	// Status reports the slot's current state.
	Status(slot int) SlotStatus
	// Logs returns the slot's log stream (hop-ABI outbox). The channel is
	// closed when the slot stops.
	Logs(slot int) <-chan string
}
