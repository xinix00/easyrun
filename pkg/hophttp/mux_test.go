package hophttp

import (
	"bufio"
	"errors"
	"net"
	"testing"
	"time"
)

// record is a handler that reports which pattern answered, plus the wildcards it
// captured.
func record(name string, got *string, vals map[string]string) Handler {
	return func(w ResponseWriter, r *Request) {
		*got = name
		for k := range vals {
			vals[k] = r.PathValue(k)
		}
		w.WriteHeader(StatusOK)
	}
}

// hopRoutes is hop's real route table (agent + leader API). Precedence bugs are
// invisible until a specific route gets answered by a generic one, so the table
// that ships is the table that gets tested.
func hopMux(got *string, vals map[string]string) *Mux {
	m := NewServeMux()
	for _, p := range []string{
		// agent
		"/health", "/capacity", "/tasks", "/run", "/delete/", "/stop/",
		"/stop-task/", "/logs/", "/leader",
		"/v1/agents", "/v1/agents/", "/v1/agents/{id}/logs/",
		"/v1/jobs", "/v1/jobs/", "/v1/status", "/v1/events",
		// leader API
		"GET /v1/agents", "POST /v1/agents", "POST /v1/heartbeat",
		"DELETE /v1/agents/", "GET /v1/agents/{agent_id}/capacity",
		"GET /v1/agents/{agent_id}/logs/{task_id}/{stream}",
		"GET /v1/jobs", "POST /v1/jobs", "DELETE /v1/jobs/",
		"GET /v1/status", "GET /v1/events", "POST /v1/notify",
		"GET /v1/jobs/{name}/status", "PATCH /v1/jobs/{name}/priority",
	} {
		// The agent and the leader never share one mux in production; loading
		// both sets at once is the harder case, so that is what gets tested.
		m.HandleFunc(p, record(p, got, vals))
	}
	return m
}

// TestMuxPrecedence is the case agent.go's comment calls out: a log tail must
// reach the STREAMING handler, not the buffered proxy on /v1/agents/. If the
// generic prefix wins, every log tail and every SSE stream silently buffers and
// the client sees nothing until the request ends — and a stream does not end.
func TestMuxPrecedence(t *testing.T) {
	var got string
	vals := map[string]string{"id": "", "agent_id": "", "task_id": "", "stream": "", "name": ""}
	m := hopMux(&got, vals)

	for _, tc := range []struct {
		method, path, want string
	}{
		{"GET", "/health", "/health"},
		{"POST", "/run", "/run"},
		{"GET", "/logs/abc", "/logs/"},
		{"DELETE", "/delete/task-1", "/delete/"},

		// The precedence case: specific streaming route over the generic proxy.
		{"GET", "/v1/agents/node-1/logs/task-2", "/v1/agents/{id}/logs/"},
		{"GET", "/v1/agents/node-1/logs/", "/v1/agents/{id}/logs/"},
		{"GET", "/v1/agents/node-1/other", "/v1/agents/"},

		// Method in the pattern beats one that takes any method.
		{"POST", "/v1/heartbeat", "POST /v1/heartbeat"},
		{"GET", "/v1/agents", "GET /v1/agents"},
		{"POST", "/v1/agents", "POST /v1/agents"},

		// Wildcards at depth, and a literal tail after them.
		{"GET", "/v1/agents/n1/capacity", "GET /v1/agents/{agent_id}/capacity"},
		{"GET", "/v1/agents/n1/logs/t1/stdout", "GET /v1/agents/{agent_id}/logs/{task_id}/{stream}"},
		{"GET", "/v1/jobs/web/status", "GET /v1/jobs/{name}/status"},
		{"PATCH", "/v1/jobs/web/priority", "PATCH /v1/jobs/{name}/priority"},

		// A method with no specific route falls back to the subtree that takes any.
		{"PUT", "/v1/jobs/web", "/v1/jobs/"},
	} {
		got = ""
		m.ServeHTTP(&nopWriter{hdr: Header{}}, &Request{Method: tc.method, Path: tc.path, Header: Header{}})
		if got != tc.want {
			t.Errorf("%s %s -> %q, want %q", tc.method, tc.path, got, tc.want)
		}
	}
}

// TestMuxPathValue: the wildcards a handler reads back.
func TestMuxPathValue(t *testing.T) {
	var got string
	vals := map[string]string{"agent_id": "", "task_id": "", "stream": ""}
	m := hopMux(&got, vals)

	m.ServeHTTP(&nopWriter{hdr: Header{}}, &Request{
		Method: "GET", Path: "/v1/agents/node-7/logs/task-9/stderr", Header: Header{},
	})
	for k, want := range map[string]string{"agent_id": "node-7", "task_id": "task-9", "stream": "stderr"} {
		if vals[k] != want {
			t.Errorf("PathValue(%q) = %q, want %q", k, vals[k], want)
		}
	}
	// A wildcard that is not in the matched pattern is empty, not a panic.
	m.ServeHTTP(&nopWriter{hdr: Header{}}, &Request{Method: "GET", Path: "/health", Header: Header{}})
}

// TestMuxNotFoundAndMethod: no route is a 404; a route for that path under a
// different method only is a 405. Answering 404 for a wrong method sends a
// client looking for a typo in the URL instead of in the verb.
func TestMuxNotFoundAndMethod(t *testing.T) {
	m := NewServeMux()
	m.HandleFunc("GET /only-get", func(w ResponseWriter, r *Request) { w.WriteHeader(StatusOK) })

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/only-get", StatusOK},
		{"POST", "/only-get", StatusMethodNotAllowed},
		{"GET", "/nope", StatusNotFound},
		{"GET", "/only-get/deeper", StatusNotFound}, // exact pattern, not a subtree
	} {
		w := &nopWriter{hdr: Header{}}
		m.ServeHTTP(w, &Request{Method: tc.method, Path: tc.path, Header: Header{}})
		if w.status != tc.want {
			t.Errorf("%s %s -> %d, want %d", tc.method, tc.path, w.status, tc.want)
		}
	}
}

// TestMuxWildcardEistSegment: {id} matches one non-empty segment, so
// /v1/agents//logs/ is not a log tail with an empty agent — it falls through.
func TestMuxWildcardEistSegment(t *testing.T) {
	var got string
	m := hopMux(&got, map[string]string{})
	m.ServeHTTP(&nopWriter{hdr: Header{}}, &Request{Method: "GET", Path: "/v1/agents//logs/", Header: Header{}})
	if got == "/v1/agents/{id}/logs/" {
		t.Error("an empty segment matched a wildcard")
	}
}

// TestMuxWeigertFouteWiring: a pattern without a leading slash and a duplicate
// registration are wiring mistakes that are present from the first run. Panic
// beats a route that silently never fires.
func TestMuxWeigertFouteWiring(t *testing.T) {
	for _, tc := range []struct {
		naam string
		fn   func(*Mux)
	}{
		{"geen leidende slash", func(m *Mux) { m.HandleFunc("health", nil) }},
		{"dubbel patroon", func(m *Mux) {
			m.HandleFunc("/x", nil)
			m.HandleFunc("/x", nil)
		}},
		{"dubbel patroon met methode", func(m *Mux) {
			m.HandleFunc("GET /x", nil)
			m.HandleFunc("GET /x", nil)
		}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: geen panic", tc.naam)
				}
			}()
			tc.fn(NewServeMux())
		}()
	}

	// Dezelfde weg onder een ándere methode is juist normaal.
	m := NewServeMux()
	m.HandleFunc("GET /x", nil)
	m.HandleFunc("POST /x", nil)
}

// nopWriter is a ResponseWriter that only remembers the status.
type nopWriter struct {
	hdr    Header
	status int
	body   []byte
}

func (n *nopWriter) Header() Header { return n.hdr }
func (n *nopWriter) WriteHeader(s int) {
	if n.status == 0 {
		n.status = s
	}
}
func (n *nopWriter) Write(p []byte) (int, error) {
	n.WriteHeader(StatusOK)
	n.body = append(n.body, p...)
	return len(p), nil
}
func (n *nopWriter) Flush() error { return nil }
func (n *nopWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("nopWriter cannot be hijacked")
}

// TestCleanPath: the transports hand the router a normalised path, so the node
// and the machine hop was developed on route identically — and a handler never
// sees "..".
func TestCleanPath(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "/"},
		{"/", "/"},
		{"health", "/health"},
		{"/v1//agents", "/v1/agents"},
		{"/v1/agents/", "/v1/agents/"}, // the trailing slash IS meaningful
		{"/v1/./agents", "/v1/agents"},
		{"/v1/agents/../jobs", "/v1/jobs"},
		{"/v1/agents/../..", "/"},
		{"/logs/task-1/", "/logs/task-1/"},
	} {
		if got := cleanPath(tc.in); got != tc.want {
			t.Errorf("cleanPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestContextIsLazy pins the cause of the 200/502/200/502 that showed up on
// hardware: on a node, asking for a request's lifetime puts a watchdog on the
// connection, and a watched connection cannot be reused. The node transport
// therefore hands over the DONE FUNCTION, not a context, and Context() calls it
// only when a handler actually wants it.
//
// A handler that answers and returns must not trigger it, or keep-alive dies for
// every request in the system.
func TestContextIsLazy(t *testing.T) {
	gevraagd := 0
	done := make(chan struct{})
	req := &Request{Method: "GET", Path: "/x", Header: Header{}}
	req.done = func() <-chan struct{} {
		gevraagd++
		return done
	}

	// Een handler die niets met de levensduur doet, raakt hem ook niet aan.
	if gevraagd != 0 {
		t.Fatalf("done was called %d times before anyone asked", gevraagd)
	}

	// En wie hem wél vraagt, krijgt een context die eindigt als de verbinding
	// eindigt.
	ctx := req.Context()
	if gevraagd != 1 {
		t.Fatalf("done was called %d times, want 1", gevraagd)
	}
	if ctx.Err() != nil {
		t.Errorf("a live request already reports %v", ctx.Err())
	}
	close(done)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("the context did not end when the connection did")
	}
	if ctx.Err() == nil {
		t.Error("Err() is nil after the connection ended")
	}
	if _, ok := ctx.Deadline(); ok {
		t.Error("this context has no deadline to report")
	}

	// Zonder naad is het gewoon een achtergrond-context, geen nil.
	if got := (&Request{}).Context(); got == nil {
		t.Error("Context() returned nil")
	}
}
