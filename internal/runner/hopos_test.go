package runner

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"hop/internal/types"
	"hop/pkg/hopos"
)

// fakeSlotManager is an in-memory SlotManager for lifecycle tests.
type fakeSlotManager struct {
	mu    sync.Mutex
	slots map[int]*fakeSlot

	num     int
	classes map[int]string
}

type fakeSlot struct {
	image    []byte
	memLimit uint64
	env      map[string]string
	coreOn   bool
	app      uint64
	exitCode uint64
	logs     chan string
}

func newFakeSlotManager(num int) *fakeSlotManager {
	classes := make(map[int]string, num)
	for i := 1; i <= num; i++ {
		switch {
		case i <= 3:
			classes[i] = "small"
		case i <= 7:
			classes[i] = "mid"
		default:
			classes[i] = "big"
		}
	}
	return &fakeSlotManager{
		slots:   make(map[int]*fakeSlot),
		num:     num,
		classes: classes,
	}
}

func (f *fakeSlotManager) NumSlots() int             { return f.num }
func (f *fakeSlotManager) CoreClass(slot int) string { return f.classes[slot] }

func (f *fakeSlotManager) Start(slot int, image []byte, memLimit uint64, env map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := &fakeSlot{
		image: image, memLimit: memLimit, env: env,
		coreOn: true, app: hopos.SlotReady,
		logs: make(chan string, 16),
	}
	s.logs <- "app leeft"
	f.slots[slot] = s
	return nil
}

func (f *fakeSlotManager) Stop(slot int, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.slots[slot]; ok && s.coreOn {
		s.coreOn = false
		s.app = hopos.SlotExited
		s.exitCode = 0
		close(s.logs)
	}
	return nil
}

func (f *fakeSlotManager) Status(slot int) hopos.SlotStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.slots[slot]
	if !ok {
		return hopos.SlotStatus{App: hopos.SlotEmpty}
	}
	return hopos.SlotStatus{CoreOn: s.coreOn, App: s.app, ExitCode: s.exitCode}
}

func (f *fakeSlotManager) Logs(slot int) <-chan string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slots[slot].logs
}

func (f *fakeSlotManager) slot(i int) *fakeSlot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slots[i]
}

// imageServer serves fake app-image bytes over HTTP for the artifact path.
func imageServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func hopJob(url string) *types.Job {
	return &types.Job{
		Name:        "demo",
		Driver:      types.DriverHop,
		Artifacts:   []types.Artifact{{URL: url + "/app.img"}},
		MemoryLimit: 64 << 20,
		Env:         map[string]string{"BUCKET": "hop-apps"},
	}
}

func TestHopRunnerLifecycle(t *testing.T) {
	image := []byte("ELF-achtige bytes")
	srv := imageServer(t, image)
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})

	job := hopJob(srv.URL)
	task := &types.Task{ID: "t1", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if task.Pid == 0 {
		t.Fatal("task.Pid (slot) not set")
	}

	s := sm.slot(task.Pid)
	if string(s.image) != string(image) {
		t.Fatal("image bytes not passed to slot")
	}
	if s.memLimit != 64<<20 {
		t.Fatalf("memLimit = %d, want %d (job.MemoryLimit moet doorkomen)", s.memLimit, 64<<20)
	}
	if s.env["BUCKET"] != "hop-apps" {
		t.Fatal("job env not passed")
	}
	if s.env["ER_ATTR_NODE_OS"] != "hopos" {
		t.Fatalf("node attrs not injected as ER_ATTR_*: %v", s.env)
	}

	if st, _ := r.Status(task); st != types.TaskRunning {
		t.Fatalf("Status = %v, want running", st)
	}

	// Log line must reach the stdout broadcaster.
	out := r.GetStdout(task.ID)
	if out == nil {
		t.Fatal("no stdout broadcaster")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if tail := out.Tail(); len(tail) == 0 || tail[0] != "app leeft" {
		t.Fatalf("log ring not bridged to broadcaster: %v", tail)
	}

	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st, _ := r.Status(task); st != types.TaskStopped {
		t.Fatalf("Status after stop = %v, want stopped", st)
	}
}

func TestHopRunnerCoreClassAllocation(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)

	job := hopJob(srv.URL)
	job.Tags = map[string]string{"core-class": "big"}
	task := &types.Task{ID: "t-big"}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := sm.CoreClass(task.Pid); got != "big" {
		t.Fatalf("allocated %q slot %d, want big", got, task.Pid)
	}
}

func TestHopRunnerRejections(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(2)
	r := NewHopRunner(sm, nil)

	cases := map[string]*types.Job{
		"container":   {Image: "nginx", Artifacts: []types.Artifact{{URL: srv.URL}}},
		"ports":       {Ports: map[string]int{"http": 80}, Artifacts: []types.Artifact{{URL: srv.URL}}},
		"no artifact": {Command: "./app"},
		"extract":     {Artifacts: []types.Artifact{{URL: srv.URL, Extract: "tar.gz"}}},
	}
	for name, job := range cases {
		if err := r.Run(job, &types.Task{ID: name}); err == nil {
			t.Fatalf("%s: expected rejection", name)
		}
	}

	// Slot exhaustion: 2 slots, third Run must fail.
	for i := 0; i < 2; i++ {
		if err := r.Run(hopJob(srv.URL), &types.Task{ID: string(rune('a' + i))}); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}
	if err := r.Run(hopJob(srv.URL), &types.Task{ID: "overflow"}); err == nil {
		t.Fatal("expected no-free-slot error")
	}
}
