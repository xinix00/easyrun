package runner

import (
	"bufio"
	"io"
	"sync"
)

const tailSize = 50

// LogBroadcaster broadcasts log lines to multiple listeners
// and keeps the last 50 lines in a ring buffer for post-crash debugging.
type LogBroadcaster struct {
	listeners []chan string
	tail      [tailSize]string
	tailPos   int
	tailCount int
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

	b.mu.Lock()
	b.tail[b.tailPos%tailSize] = line
	b.tailPos++
	if b.tailCount < tailSize {
		b.tailCount++
	}
	for _, ch := range b.listeners {
		select {
		case ch <- line:
		default:
		}
	}
	b.mu.Unlock()

	return len(p), nil
}

// Tail returns the last N lines (up to 50).
func (b *LogBroadcaster) Tail() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	lines := make([]string, b.tailCount)
	start := b.tailPos - b.tailCount
	for i := range b.tailCount {
		lines[i] = b.tail[(start+i)%tailSize]
	}
	return lines
}

// Subscribe adds a new listener and returns a channel for log lines.
// The tail buffer is pushed first so the subscriber sees recent history.
func (b *LogBroadcaster) Subscribe() chan string {
	ch := make(chan string, 100)

	b.mu.Lock()
	// Push tail history
	start := b.tailPos - b.tailCount
	for i := range b.tailCount {
		ch <- b.tail[(start+i)%tailSize]
	}
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
