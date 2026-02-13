package runner

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogBroadcasterWrite(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Write a line
	n, err := b.Write([]byte("hello world\n"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 12 {
		t.Errorf("Write returned %d, want 12", n)
	}

	// Read with timeout
	select {
	case line := <-ch:
		if line != "hello world\n" {
			t.Errorf("got %q, want %q", line, "hello world\n")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for log line")
	}
}

func TestLogBroadcasterMultipleSubscribers(t *testing.T) {
	b := NewLogBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()
	ch3 := b.Subscribe()

	_, _ = b.Write([]byte("test message\n"))

	// All subscribers should receive the message
	for i, ch := range []chan string{ch1, ch2, ch3} {
		select {
		case line := <-ch:
			if line != "test message\n" {
				t.Errorf("subscriber %d: got %q, want %q", i, line, "test message\n")
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("subscriber %d: timeout", i)
		}
	}
}

func TestLogBroadcasterUnsubscribe(t *testing.T) {
	b := NewLogBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()

	// Unsubscribe ch1
	b.Unsubscribe(ch1)

	// ch1 should be closed
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("ch1 should be closed")
		}
	default:
		// Channel is closed but empty, this is fine
	}

	// ch2 should still work
	_, _ = b.Write([]byte("after unsubscribe\n"))

	select {
	case line := <-ch2:
		if line != "after unsubscribe\n" {
			t.Errorf("got %q, want %q", line, "after unsubscribe\n")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for log line on ch2")
	}
}

func TestLogBroadcasterClose(t *testing.T) {
	b := NewLogBroadcaster()
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()

	b.Close()

	// Both channels should be closed
	for i, ch := range []chan string{ch1, ch2} {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("channel %d should be closed", i)
			}
		default:
			// Channel is closed, read the close
			_, ok := <-ch
			if ok {
				t.Errorf("channel %d should be closed", i)
			}
		}
	}
}

func TestLogBroadcasterSlowSubscriber(t *testing.T) {
	b := NewLogBroadcaster()

	// Create a subscriber but don't read from it
	_ = b.Subscribe()

	// Write more than the buffer size
	for i := 0; i < 150; i++ {
		_, _ = b.Write([]byte("line\n"))
	}

	// Should not block or panic
}

func TestLogBroadcasterConcurrentAccess(t *testing.T) {
	b := NewLogBroadcaster()

	var wg sync.WaitGroup

	// Concurrent subscribers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := b.Subscribe()
			time.Sleep(10 * time.Millisecond)
			b.Unsubscribe(ch)
		}()
	}

	// Concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = b.Write([]byte("concurrent write\n"))
			}
		}(i)
	}

	wg.Wait()
	b.Close()
}

func TestPipeReader(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Create a reader with multiple lines
	input := "line1\nline2\nline3\n"
	reader := strings.NewReader(input)

	// Run PipeReader in goroutine
	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	// Collect received lines
	var received []string
	timeout := time.After(500 * time.Millisecond)

loop:
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				break loop
			}
			received = append(received, line)
		case <-timeout:
			break loop
		}
	}

	<-done

	if len(received) != 3 {
		t.Errorf("received %d lines, want 3", len(received))
	}

	expected := []string{"line1\n", "line2\n", "line3\n"}
	for i, want := range expected {
		if i < len(received) && received[i] != want {
			t.Errorf("line %d: got %q, want %q", i, received[i], want)
		}
	}
}

func TestPipeReaderClosesOnEOF(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	reader := bytes.NewReader([]byte("single line\n"))

	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	// Wait for completion
	<-done

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			// Drain remaining message
			_, ok = <-ch
			if ok {
				t.Error("channel should eventually close")
			}
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for channel to close")
	}
}

func TestLogBroadcasterImplementsWriter(t *testing.T) {
	// Ensure LogBroadcaster implements io.Writer
	var _ interface{ Write([]byte) (int, error) } = &LogBroadcaster{}
}

// ============== EDGE CASE TESTS ==============

func TestLogBroadcasterWriteAfterClose(t *testing.T) {
	b := NewLogBroadcaster()
	_ = b.Subscribe()

	b.Close()

	// Write after close should not panic
	n, err := b.Write([]byte("after close\n"))
	if err != nil {
		t.Errorf("Write after close returned error: %v", err)
	}
	if n != 12 {
		t.Errorf("Write returned %d, want 12", n)
	}
}

func TestLogBroadcasterDoubleClose(t *testing.T) {
	b := NewLogBroadcaster()
	_ = b.Subscribe()

	b.Close()
	// Second close should not panic
	b.Close()
}

func TestLogBroadcasterUnsubscribeTwice(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	b.Unsubscribe(ch)
	// Second unsubscribe should not panic (channel already removed)
	b.Unsubscribe(ch)
}

func TestLogBroadcasterUnsubscribeNonExistent(t *testing.T) {
	b := NewLogBroadcaster()
	_ = b.Subscribe()

	// Unsubscribe a channel that was never subscribed
	otherCh := make(chan string)
	b.Unsubscribe(otherCh)
	// Should not panic
}

func TestLogBroadcasterEmptyWrite(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	n, err := b.Write([]byte{})
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != 0 {
		t.Errorf("Write returned %d, want 0", n)
	}

	// Empty string should still be sent
	select {
	case line := <-ch:
		if line != "" {
			t.Errorf("got %q, want empty string", line)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for empty line")
	}
}

func TestLogBroadcasterNoSubscribers(t *testing.T) {
	b := NewLogBroadcaster()

	// Write with no subscribers should not panic
	n, err := b.Write([]byte("no one listening\n"))
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != 17 {
		t.Errorf("Write returned %d, want 17", n)
	}
}

func TestLogBroadcasterSubscribeAfterClose(t *testing.T) {
	b := NewLogBroadcaster()
	b.Close()

	// Subscribe after close - channel should be added but immediately drained
	ch := b.Subscribe()

	// Write should work but channel won't receive (it's in closed state)
	_, _ = b.Write([]byte("test\n"))

	// Channel should be readable (might be empty or have messages)
	select {
	case <-ch:
		// Got a message or channel closed
	case <-time.After(50 * time.Millisecond):
		// Timeout is also acceptable (no subscribers when closed)
	}
}

func TestLogBroadcasterLargeMessage(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Write a large message (1MB)
	largeMsg := make([]byte, 1024*1024)
	for i := range largeMsg {
		largeMsg[i] = 'x'
	}

	n, err := b.Write(largeMsg)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(largeMsg) {
		t.Errorf("Write returned %d, want %d", n, len(largeMsg))
	}

	select {
	case line := <-ch:
		if len(line) != len(largeMsg) {
			t.Errorf("received %d bytes, want %d", len(line), len(largeMsg))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for large message")
	}
}

func TestLogBroadcasterRapidSubscribeUnsubscribe(t *testing.T) {
	b := NewLogBroadcaster()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := b.Subscribe()
			b.Unsubscribe(ch)
		}()
	}

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Write([]byte("test\n"))
		}()
	}

	wg.Wait()
	b.Close()
}

func TestPipeReaderEmptyInput(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	reader := strings.NewReader("")

	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	<-done

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed for empty input")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for channel to close")
	}
}

func TestPipeReaderNoNewlineAtEnd(t *testing.T) {
	b := NewLogBroadcaster()
	ch := b.Subscribe()

	// Input without trailing newline
	reader := strings.NewReader("line1\nline2")

	done := make(chan struct{})
	go func() {
		PipeReader(b, reader)
		close(done)
	}()

	var received []string
	timeout := time.After(500 * time.Millisecond)

loop:
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				break loop
			}
			received = append(received, line)
		case <-timeout:
			break loop
		}
	}

	<-done

	// Should receive 2 lines (scanner handles no trailing newline)
	if len(received) != 2 {
		t.Errorf("received %d lines, want 2", len(received))
	}
}
