package runner

import (
	"bufio"
	"io"
	"sync"
)

// LogBroadcaster broadcasts log lines to multiple listeners
type LogBroadcaster struct {
	listeners []chan string
	mu        sync.RWMutex
}

// NewLogBroadcaster creates a new log broadcaster
func NewLogBroadcaster() *LogBroadcaster {
	return &LogBroadcaster{
		listeners: make([]chan string, 0),
	}
}

// Write implements io.Writer interface
func (b *LogBroadcaster) Write(p []byte) (n int, err error) {
	line := string(p)

	b.mu.RLock()
	for _, ch := range b.listeners {
		select {
		case ch <- line:
		default:
			// Skip if listener is slow/blocked
		}
	}
	b.mu.RUnlock()

	return len(p), nil
}

// Subscribe adds a new listener and returns a channel for log lines
func (b *LogBroadcaster) Subscribe() chan string {
	ch := make(chan string, 100)

	b.mu.Lock()
	b.listeners = append(b.listeners, ch)
	b.mu.Unlock()

	return ch
}

// Unsubscribe removes a listener
func (b *LogBroadcaster) Unsubscribe(ch chan string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, listener := range b.listeners {
		if listener == ch {
			b.listeners = append(b.listeners[:i], b.listeners[i+1:]...)
			close(ch)
			break
		}
	}
}

// Close closes all listeners (call when process exits)
func (b *LogBroadcaster) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.listeners {
		close(ch)
	}
	b.listeners = nil
}

// PipeReader reads from reader and broadcasts to broadcaster until EOF
func PipeReader(broadcaster *LogBroadcaster, reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		_, _ = broadcaster.Write(append(scanner.Bytes(), '\n'))
	}
	// Reader closed (process exited), close broadcaster
	broadcaster.Close()
}
