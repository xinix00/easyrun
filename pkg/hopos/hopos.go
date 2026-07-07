// Package hopos defines the public contract between HOP and HopOS: the
// SlotManager interface that HopOS' slot primitives implement and HOP's
// HopRunner consumes. It lives outside internal/ so the HopOS repository can
// implement it (hop-os/metal/slotmgr wraps metal/slots).
package hopos

import "time"

// App status values, mirroring hop-os/metal/layout control-page states.
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
