package runner

import (
	"bufio"
	"io"
	"sync"
	"time"
)

const tailSize = 50

// LogBroadcaster broadcasts log lines to multiple listeners
// and keeps the last 50 lines in a ring buffer for post-crash debugging.
type LogBroadcaster struct {
	listeners []chan string
	tail      [tailSize]string
	tailPos   int
	tailCount int
	closed    bool
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
//
// On a finished task the channel is closed right after that history: the task is
// over, so its log stream is over too. Without this, asking a dead task for its
// logs would hand over the history and then hang until the client gave up —
// which reads like a stalled node instead of a completed one.
func (b *LogBroadcaster) Subscribe() chan string {
	ch := make(chan string, 100)

	b.mu.Lock()
	// Push tail history
	start := b.tailPos - b.tailCount
	for i := range b.tailCount {
		ch <- b.tail[(start+i)%tailSize]
	}
	if b.closed {
		close(ch)
	} else {
		b.listeners = append(b.listeners, ch)
	}
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
	// The tail stays readable — that is the whole point of keeping a finished
	// task's logs around. closed only means "no more lines will come".
	b.closed = true
}

// logRetention is hoe lang de logs van een AFGELOPEN task opvraagbaar blijven.
// Lang genoeg om ná de melding "task failed" te gaan kijken, kort genoeg dat een
// node die dagen restart-lussen draait geen geschiedenis opstapelt.
const logRetention = 5 * time.Minute

// logStore is de log-boekhouding van één runner: de broadcasters van de LOPENDE
// tasks, plus die van net-afgelopen tasks — die gaan niet weg maar met pensioen
// en blijven logRetention opvraagbaar.
//
// Alle drie de runners (exec, docker, hop) gebruiken deze ene store, want het
// probleem was voor alle drie hetzelfde: bij het opruimen van een task ging zijn
// broadcaster meteen mee, dus wie een gevallen task om zijn logs vroeg kreeg
// "task not found" — de log was weg op precies het moment dat hij telde, en op
// een headless node bestond het waarom dan nergens meer. In een restart-lus is
// dat elke keer.
type logStore struct {
	mu      sync.RWMutex
	live    map[string]logPair
	retired map[string]logPair
}

// logPair zijn de twee logstromen van één task; at is het moment van pensioen
// (nul zolang de task loopt).
type logPair struct {
	stdout *LogBroadcaster
	stderr *LogBroadcaster
	at     time.Time
}

func newLogStore() *logStore {
	return &logStore{
		live:    make(map[string]logPair),
		retired: make(map[string]logPair),
	}
}

// put legt de broadcasters van een startende task vast. Een hergebruikte taskID
// laat zijn pensioen achter zich: de nieuwe logs zijn dan de logs.
func (s *logStore) put(taskID string, stdout, stderr *LogBroadcaster) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live[taskID] = logPair{stdout: stdout, stderr: stderr}
	delete(s.retired, taskID)
}

// retire stuurt de logs van een afgelopen task met pensioen: Close() sluit
// lopende tails netjes af (er komt geen regel meer bij) maar laat de tail
// leesbaar, nog logRetention lang. Idempotent, en een no-op voor een task die
// nooit logs registreerde (mislukte start).
func (s *logStore) retire(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p, ok := s.live[taskID]; ok {
		if p.stdout != nil {
			p.stdout.Close()
		}
		if p.stderr != nil {
			p.stderr.Close()
		}
		p.at = time.Now()
		s.retired[taskID] = p
		delete(s.live, taskID)
	}

	// Opruimen gebeurt hier en niet op een achtergrond-timer: het juiste moment
	// om verlopen geschiedenis te lozen is precies wanneer er weer iets bij komt.
	for id, p := range s.retired {
		if time.Since(p.at) > logRetention {
			delete(s.retired, id)
		}
	}
}

// stdout geeft de broadcaster van een lopende task, of die van een task die
// minder dan logRetention geleden afliep (anders nil).
func (s *logStore) stdout(taskID string) *LogBroadcaster {
	p, _ := s.lookup(taskID)
	return p.stdout
}

// stderr doet hetzelfde voor de foutstroom.
func (s *logStore) stderr(taskID string) *LogBroadcaster {
	p, _ := s.lookup(taskID)
	return p.stderr
}

func (s *logStore) lookup(taskID string) (logPair, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.live[taskID]; ok {
		return p, true
	}
	if p, ok := s.retired[taskID]; ok && time.Since(p.at) <= logRetention {
		return p, true
	}
	return logPair{}, false
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
