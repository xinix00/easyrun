package runner

import (
	"fmt"
	"io"
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

	// cage is wat de node over de kooi van élk slot meldt (hopos.SlotStatus.Cage).
	cage string

	// Laatste sharegroup-plaatsing (voor assertions in de core-deling-test).
	lastSharegroup string
	lastPoolCores  int
}

type fakeSlot struct {
	image    []byte
	staged   []byte // door de "apploader" gedownloade image, wacht op StartStaged
	memLimit uint64
	cores    int
	env      map[string]string
	mounts   map[string]string
	ports    map[string]int
	coreOn   bool
	app      uint64
	exitCode uint64
	cage     string
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

func (f *fakeSlotManager) NumCores() int             { return f.num }
func (f *fakeSlotManager) CoreClass(slot int) string { return f.classes[slot] }

// StartLoader is phase 1: it loads the (embedded) apploader and simulates it —
// the real loader downloads the app image (env HOP_IMAGE_URL) on its own
// core+netstack and signals SlotStaged. The fake fetches that image and parks
// the slot at SlotStaged; StartStaged then promotes it (phase 2). This mirrors
// the real two-phase "app downloads its own image" flow.
func (f *fakeSlotManager) StartLoader(slot int, memLimit uint64, sharegroup string, poolCores int, env map[string]string) error {
	f.mu.Lock()
	f.lastSharegroup, f.lastPoolCores = sharegroup, poolCores
	f.mu.Unlock()
	url := env["HOP_IMAGE_URL"]
	if url == "" {
		return fmt.Errorf("fake: apploader started without HOP_IMAGE_URL")
	}
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	img, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slots[slot] = &fakeSlot{
		staged: img, memLimit: memLimit, env: env,
		app: hopos.SlotStaged, logs: make(chan string, 16),
	}
	return nil
}

// StartStaged promotes the staged image to the running app (phase 2), with the
// real cores/volumes/ports (the loader ran on 1 core, no mounts).
func (f *fakeSlotManager) StartStaged(slot int, memLimit uint64, cores int, env map[string]string, mounts map[string]string, ports map[string]int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.slots[slot]
	if !ok || s.staged == nil {
		return fmt.Errorf("fake: StartStaged on slot %d with nothing staged", slot)
	}
	s.image, s.staged = s.staged, nil
	s.memLimit, s.cores, s.env, s.mounts, s.ports = memLimit, cores, env, mounts, ports
	s.coreOn, s.app = true, hopos.SlotReady
	s.logs <- "app leeft"
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
	cage := s.cage
	if cage == "" {
		cage = f.cage
	}
	return hopos.SlotStatus{CoreOn: s.coreOn, App: s.app, ExitCode: s.exitCode, Cage: cage}
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
	job.Volumes = map[string]string{"/data": "/data"}
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
	if s.mounts["/data"] != "/data" {
		t.Fatalf("job.Volumes not passed to slot: %v", s.mounts)
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

// De kooi-regel van de node moet in de eigen log van de task belanden, en als
// eerste regel. Dat is de hele reden dat het veld bestaat: op een headless board
// is de andere plek waar de node dit zou zeggen zijn seriële console, en die
// verliest bytes op 115200 — een gehavend hex-getal is erger dan geen.
func TestHopRunnerCageLineReachesTaskLog(t *testing.T) {
	srv := imageServer(t, []byte("ELF-achtige bytes"))
	sm := newFakeSlotManager(11)
	sm.cage = "hart of slot 1: misa 0x800000000094112d (rv64 acdfimsux, S-mode present)"
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	task := &types.Task{ID: "t-cage", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(hopJob(srv.URL), task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := r.GetStdout(task.ID)
	if out == nil {
		t.Fatal("no stdout broadcaster")
	}
	tail := out.Tail()
	if len(tail) == 0 || tail[0] != sm.cage {
		t.Fatalf("kooi-regel staat niet vooraan in de task-log: %v", tail)
	}
}

// Zonder kooi-regel (de architecturen waar de kooi een stage-2-map is) mag er
// geen lege regel vooraan komen: dan is de eerste regel die van de app.
func TestHopRunnerNoCageLineWhenEmpty(t *testing.T) {
	srv := imageServer(t, []byte("ELF-achtige bytes"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, map[string]string{"node.os": "hopos"})
	task := &types.Task{ID: "t-nocage", JobName: "demo", Driver: types.DriverHop}

	if err := r.Run(hopJob(srv.URL), task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := r.GetStdout(task.ID)
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if tail := out.Tail(); len(tail) == 0 || tail[0] != "app leeft" {
		t.Fatalf("eerste regel moet van de app zijn: %v", tail)
	}
}

func TestHopRunnerPorts(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	job := hopJob(srv.URL)
	job.Ports = map[string]int{"http": 0} // 0 = dynamic; agent allocates
	task := &types.Task{ID: "t-ports", Ports: map[string]int{"http": 18080}}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	s := sm.slot(task.Pid)
	if s.ports["http"] != 18080 {
		t.Fatalf("task.Ports not passed to slot: %v", s.ports)
	}
	if s.env["ER_PORT_HTTP"] != "18080" {
		t.Fatalf("ER_PORT_HTTP not injected: %v", s.env)
	}
}

// TestHopRunnerDeletedDuringStaging: wordt de task tijdens staging gestopt
// (delete/preemptie → Stop), dan moet Run met nil eindigen ZONDER iets aan de
// (door Stop vrijgegeven) slot te koppelen — geen task.Pid, geen
// slots/inUse-entry, geen log-broadcaster. Anders stopte de ghost-guard via
// de task.Pid-fallback de slot van een ANDERE task die 'm intussen had.
func TestHopRunnerDeletedDuringStaging(t *testing.T) {
	// Trage download: Stop komt binnen terwijl de apploader nog fetcht.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("img"))
	}))
	t.Cleanup(srv.Close)

	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	job := hopJob(srv.URL)
	task := &types.Task{ID: "t-del", JobName: "demo", Driver: types.DriverHop}

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(job, task) }()

	time.Sleep(50 * time.Millisecond) // Run zit midden in de staging
	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Run hoort nil te geven bij stop-tijdens-staging, kreeg: %v", err)
	}
	if task.Pid != 0 {
		t.Fatalf("task.Pid = %d, hoort 0 te blijven (slot is vrijgegeven)", task.Pid)
	}
	r.mu.RLock()
	_, hasSlot := r.slots[task.ID]
	_, hasLog := r.stdoutLog[task.ID]
	busy := len(r.inUse)
	stopping := len(r.stopping)
	r.mu.RUnlock()
	if hasSlot || hasLog || busy != 0 || stopping != 0 {
		t.Fatalf("runner-state niet schoon: slot=%v log=%v inUse=%d stopping=%d", hasSlot, hasLog, busy, stopping)
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

func TestHopRunnerSMPCores(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	// CPUShares 3072 = 3 cores → één SMP-task op 3 aaneengesloten cores.
	job := hopJob(srv.URL)
	job.CPUShares = 3072
	task := &types.Task{ID: "t-smp"}
	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if s := sm.slot(task.Pid); s.cores != 3 {
		t.Fatalf("cores doorgegeven aan slot = %d, want 3", s.cores)
	}
	// Alle 3 cores (primair + 2 secundair) moeten bezet zijn: een tweede,
	// even grote task mag ze niet kunnen pakken maar wel de resterende.
	prim := task.Pid
	for c := prim; c < prim+3; c++ {
		if id := r.inUse[c]; id != task.ID {
			t.Fatalf("core %d bezet door %q, want %q", c, id, task.ID)
		}
	}

	// Vrijgeven geeft álle 3 de cores terug.
	if err := r.Stop(task); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	for c := prim; c < prim+3; c++ {
		if _, busy := r.inUse[c]; busy {
			t.Fatalf("core %d nog bezet na Stop", c)
		}
	}
}

func TestHopRunnerSharegroup(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(11)
	r := NewHopRunner(sm, nil)
	// sharegroup-tag + CPUShares 2048: een POOL van 2 hele cores, gedeeld door
	// gelijk-getagde apps. Anders dan SMP reserveert HOP hier maar ÉÉN kooi
	// (HopOS stapelt de apps op de pool) en draait de app zelf op één core.
	job := hopJob(srv.URL)
	job.CPUShares = 2048
	job.Tags = map[string]string{"sharegroup": "web"}
	task := &types.Task{ID: "t-sg"}
	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Naam + poolgrootte doorgegeven aan de SlotManager (uit tag + CPUShares).
	if sm.lastSharegroup != "web" || sm.lastPoolCores != 2 {
		t.Fatalf("StartLoader kreeg sharegroup=%q poolCores=%d, want web/2", sm.lastSharegroup, sm.lastPoolCores)
	}
	// De app draait op ÉÉN core (de 2 is de poolgrootte, niet de app-breedte).
	if s := sm.slot(task.Pid); s.cores != 1 {
		t.Fatalf("app-cores = %d, want 1", s.cores)
	}
	// Slechts één kooi bezet (niet 2 aaneengesloten zoals SMP): er blijft ruimte
	// voor een mede-bewoner.
	busy := 0
	for _, id := range r.inUse {
		if id == task.ID {
			busy++
		}
	}
	if busy != 1 {
		t.Fatalf("sharegroup-app bezet %d kooien, want 1", busy)
	}
}

func TestHopRunnerRejections(t *testing.T) {
	srv := imageServer(t, []byte("img"))
	sm := newFakeSlotManager(2)
	r := NewHopRunner(sm, nil)
	cases := map[string]*types.Job{
		"container":   {Image: "nginx", Artifacts: []types.Artifact{{URL: srv.URL}}},
		"no artifact": {Command: "./app"},
		"extract":     {Artifacts: []types.Artifact{{URL: srv.URL, Extract: "tar.gz"}}},
	}
	for name, job := range cases {
		if err := r.Run(job, &types.Task{ID: name}); err == nil {
			t.Fatalf("%s: expected rejection", name)
		}
	}

	// Cages are not capped to the core count: HopOS stacks more cages than it
	// has cores (sharegroups), and the node — not the runner — rejects an
	// unplaceable job. So on a 2-core fake a third unconstrained Run still
	// allocates; capacity is enforced by admission + PlaceCage, not here.
	for i := 0; i < 3; i++ {
		if err := r.Run(hopJob(srv.URL), &types.Task{ID: string(rune('a' + i))}); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}

	// A class-constrained job stays bounded by real cores: the 2-core fake has
	// only "small" cores, so a "big" request has nowhere to land.
	big := hopJob(srv.URL)
	big.Tags = map[string]string{"core-class": "big"}
	if err := r.Run(big, &types.Task{ID: "big"}); err == nil {
		t.Fatal("expected rejection: no big core on a small-only node")
	}
}
