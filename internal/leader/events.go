package leader

import "sync"

// EventBus broadcasts job change notifications to SSE subscribers.
// Channels have buffer 1 for natural coalescing. If a subscriber
// is slow, the event is dropped (next event catches up).
type EventBus struct {
	mu        sync.RWMutex
	listeners []chan string
}

// NewEventBus creates a new event bus
func NewEventBus() *EventBus {
	return &EventBus{}
}

// Subscribe returns a channel that receives job names on state changes
func (e *EventBus) Subscribe() chan string {
	ch := make(chan string, 1)
	e.mu.Lock()
	e.listeners = append(e.listeners, ch)
	e.mu.Unlock()
	return ch
}

// Unsubscribe removes a listener
func (e *EventBus) Unsubscribe(ch chan string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, l := range e.listeners {
		if l == ch {
			e.listeners = append(e.listeners[:i], e.listeners[i+1:]...)
			close(ch)
			return
		}
	}
}

// Notify sends a job change notification to all subscribers.
// If the channel is full, the event is dropped (safety ticker catches up).
func (e *EventBus) Notify(job string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, ch := range e.listeners {
		select {
		case ch <- job:
		default:
		}
	}
}
