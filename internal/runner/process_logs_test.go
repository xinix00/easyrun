//go:build darwin || linux

package runner

import (
	"strings"
	"testing"
	"time"

	"github.com/xinix00/hop/internal/types"
)

// Een gestopte exec-task blijft zijn logs houden: cleanupTaskDir gooit de taskdir
// weg (dus ook alles wat het proces op schijf zei), en juist ná een crash wil je
// weten wat er in de laatste regels stond.
func TestExecRunnerBewaartLogsNaDeStop(t *testing.T) {
	cfg := &Config{RootfsBase: t.TempDir(), Isolate: false}
	r := NewExecRunner(cfg)
	job := &types.Job{Name: "logbewaring", Command: "echo laatste-woorden"}
	task := &types.Task{ID: "t-exec-retire"}

	if err := r.Run(job, task); err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := r.GetStdout(task.ID)
	if out == nil {
		t.Fatal("geen stdout-broadcaster voor een lopende task")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(out.Tail()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(out.Tail()) == 0 {
		t.Fatal("geen logregel vóór de stop")
	}

	_ = r.Stop(task)

	after := r.GetStdout(task.ID)
	if after == nil {
		t.Fatal("logs zijn weg direct na de stop — de retentie doet niets")
	}
	if got := strings.Join(after.Tail(), ""); !strings.Contains(got, "laatste-woorden") {
		t.Fatalf("bewaarde tail = %q, mist de laatste regel van het proces", got)
	}
	if r.GetStderr(task.ID) == nil {
		t.Fatal("stderr is weg — juist daar staat waarom een proces viel")
	}

	// En na de termijn is het wél weg (geen groei op een node in een restart-lus).
	backdateRetired(r.logs, task.ID)
	if r.GetStdout(task.ID) != nil {
		t.Fatal("logs zijn na de retentietermijn nog opvraagbaar")
	}
}
