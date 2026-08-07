package runner

import (
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
	"github.com/xinix00/hop/pkg/hopos"
)

// streamSlotManager is de fakeSlotManager plus het one-phase pad: StartStream
// leest de hele stroom uit en onthoudt wat er binnenkwam, zodat de tests de
// bytes kunnen vergelijken met wat de server verstuurde.
type streamSlotManager struct {
	*fakeSlotManager
	mu       sync.Mutex
	streamed map[int][]byte
	specs    map[int]hopos.StartSpec
	fail     error // als gezet: StartStream faalt hiermee ná het lezen
}

func newStreamSlotManager(cores int) *streamSlotManager {
	return &streamSlotManager{
		fakeSlotManager: newFakeSlotManager(cores),
		streamed:        make(map[int][]byte),
		specs:           make(map[int]hopos.StartSpec),
	}
}

func (s *streamSlotManager) StartStream(slot int, image io.Reader, size int64, spec hopos.StartSpec) error {
	b, err := io.ReadAll(image)
	if err != nil {
		return err
	}
	if int64(len(b)) != size {
		// Zelfde contract als de echte plaatser: een afgekapte stroom is een
		// luide fout, nooit een halve app.
		return io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	s.streamed[slot] = b
	s.specs[slot] = spec
	fail := s.fail
	s.mu.Unlock()
	if fail != nil {
		return fail
	}
	// Draaiend melden, zoals een echte node na armSlot.
	s.fakeSlotManager.mu.Lock()
	if s.fakeSlotManager.slots == nil {
		s.fakeSlotManager.slots = make(map[int]*fakeSlot)
	}
	s.fakeSlotManager.slots[slot] = &fakeSlot{coreOn: true, app: hopos.SlotReady,
		memLimit: spec.MemLimit, cores: spec.Cores, logs: make(chan string)}
	s.fakeSlotManager.mu.Unlock()
	return nil
}

// captureSink onthoudt de voortgangsmeldingen van de runner.
type captureSink struct {
	mu      sync.Mutex
	reports []uint64
	total   uint64
}

func (c *captureSink) TaskDownloading(taskID string, done, total uint64) {
	c.mu.Lock()
	c.reports = append(c.reports, done)
	c.total = total
	c.mu.Unlock()
}

// TestStreamPathDownloadsIntoTheSlot: het one-phase pad wint van de loader,
// de bytes komen integraal aan en de voortgang wordt gemeld.
func TestStreamPathDownloadsIntoTheSlot(t *testing.T) {
	img := make([]byte, 3<<20)
	for i := range img {
		img[i] = byte(i * 7)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "3145728")
		_, _ = w.Write(img)
	}))
	defer srv.Close()

	sm := newStreamSlotManager(4)
	r := NewHopRunner(sm, nil)
	sink := &captureSink{}
	r.SetProgressSink(sink)

	job := &types.Job{Name: "stream-me", Driver: "hop",
		Artifacts: []types.Artifact{{URL: srv.URL + "/app.elf"}}, MemoryLimit: 64 << 20}
	task := &types.Task{ID: "task-stream", JobName: job.Name, State: types.TaskQueued}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	sm.mu.Lock()
	got := sm.streamed[task.Pid]
	spec := sm.specs[task.Pid]
	sm.mu.Unlock()
	if sha256.Sum256(got) != sha256.Sum256(img) {
		t.Fatal("gestreamde bytes wijken af van wat de server verstuurde")
	}
	if spec.Job != "stream-me" || spec.MemLimit != 64<<20 {
		t.Errorf("StartSpec draagt de jobgegevens niet: %+v", spec)
	}
	sink.mu.Lock()
	n, total := len(sink.reports), sink.total
	sink.mu.Unlock()
	if n == 0 || total != uint64(len(img)) {
		t.Errorf("voortgang: %d meldingen, totaal %d — wil >0 meldingen met totaal %d", n, total, len(img))
	}
}

// TestStreamPathRejectsMissingContentLength: zonder maat geen plaatsing.
func TestStreamPathRejectsMissingContentLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		f, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("x")) // chunked: geen Content-Length
		if f != nil {
			f.Flush()
		}
	}))
	defer srv.Close()

	sm := newStreamSlotManager(4)
	r := NewHopRunner(sm, nil)
	job := &types.Job{Name: "no-length", Driver: "hop",
		Artifacts: []types.Artifact{{URL: srv.URL}}, MemoryLimit: 64 << 20}
	task := &types.Task{ID: "task-nolen", JobName: job.Name, State: types.TaskQueued}
	if err := r.Run(job, task); err == nil {
		t.Fatal("een antwoord zonder Content-Length werd geaccepteerd")
	}
}

// TestStopAbortsAQueuedDownload: een Stop tijdens de download breekt hem af en
// de runner-boekhouding komt vrij — geen slot dat blijft hangen.
func TestStopAbortsAQueuedDownload(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10485760")
		_, _ = w.Write(make([]byte, 1<<20)) // 1 van 10 MB, dan stilte
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-release
	}))
	defer srv.Close()
	defer close(release)

	sm := newStreamSlotManager(4)
	r := NewHopRunner(sm, nil)
	job := &types.Job{Name: "stop-me", Driver: "hop",
		Artifacts: []types.Artifact{{URL: srv.URL}}, MemoryLimit: 64 << 20}
	task := &types.Task{ID: "task-stop", JobName: job.Name, State: types.TaskQueued}

	done := make(chan error, 1)
	go func() { done <- r.Run(job, task) }()

	// Wacht tot de download loopt (de eerste MB is binnen), stop dan.
	deadline := time.After(5 * time.Second)
	for {
		r.mu.RLock()
		_, inflight := r.cancels[task.ID]
		r.mu.RUnlock()
		if inflight {
			break
		}
		select {
		case <-deadline:
			t.Fatal("download kwam nooit op gang")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run hoort (false, nil) te geven bij een gestopte download, kreeg: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run keerde niet terug na Stop")
	}
	r.mu.RLock()
	_, held := r.slots[task.ID]
	r.mu.RUnlock()
	if held {
		t.Error("slot nog bezet na Stop van een lopende download")
	}
}

// TestStreamPlacementFailureReleasesTheSlot: faalt de plaatsing node-side, dan
// is de kooi daarna weer uitdeelbaar.
func TestStreamPlacementFailureReleasesTheSlot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4")
		_, _ = w.Write([]byte("ELF!"))
	}))
	defer srv.Close()

	sm := newStreamSlotManager(1) // één kooi: hergebruik bewijst de vrijgave
	sm.fail = io.ErrClosedPipe
	r := NewHopRunner(sm, nil)
	job := &types.Job{Name: "fail-place", Driver: "hop",
		Artifacts: []types.Artifact{{URL: srv.URL}}, MemoryLimit: 64 << 20}
	if err := r.Run(job, &types.Task{ID: "t1", JobName: job.Name, State: types.TaskQueued}); err == nil {
		t.Fatal("plaatsingsfout kwam niet terug uit Run")
	}
	sm.mu.Lock()
	sm.fail = nil
	sm.mu.Unlock()
	if err := r.Run(job, &types.Task{ID: "t2", JobName: job.Name, State: types.TaskQueued}); err != nil {
		t.Fatalf("kooi niet herbruikbaar na plaatsingsfout: %v", err)
	}
}
