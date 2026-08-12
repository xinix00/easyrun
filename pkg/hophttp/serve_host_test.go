//go:build !tamago

package hophttp

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestBridgeRoundtrip: everything a handler reads about a request survives the
// trip through net/http and our conversion, and everything it writes comes back.
func TestBridgeRoundtrip(t *testing.T) {
	m := NewServeMux()
	m.HandleFunc("POST /v1/jobs/{name}/apply", func(w ResponseWriter, r *Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Echo-Name", r.PathValue("name"))
		w.Header().Set("X-Echo-Query", r.Query().Get("wait"))
		w.Header().Set("X-Echo-Auth", r.Header.Get("x-hop-auth")) // andere schrijfwijze
		w.WriteHeader(StatusAccepted)
		w.Write([]byte("got " + string(body) + " via " + r.Method + " " + r.Path))
	})
	srv := httptest.NewServer(bridge(m.Handler()))
	defer srv.Close()

	var cl Client
	call := Call{Method: MethodPost, URL: srv.URL + "/v1/jobs/web/apply?wait=1", Body: []byte("spec")}
	call.SetHeader("X-Hop-Auth", "sig123")
	resp, err := cl.Do(call)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != StatusAccepted {
		t.Errorf("status = %d, want 202", resp.StatusCode)
	}
	for k, want := range map[string]string{
		"X-Echo-Name": "web", "X-Echo-Query": "1", "X-Echo-Auth": "sig123",
	} {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	body, _ := io.ReadAll(resp.Body)
	if want := "got spec via POST /v1/jobs/web/apply"; string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

// TestBridgeFlushStreamt is the property that /v1/events depends on: what a
// handler flushes has to arrive while the handler is still running. If it
// buffers, an SSE client sees nothing at all — a stream never reaches the end
// where a buffer would be released.
func TestBridgeFlushStreamt(t *testing.T) {
	release := make(chan struct{})
	m := NewServeMux()
	m.HandleFunc("GET /v1/events", func(w ResponseWriter, r *Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(StatusOK)
		w.Write([]byte("data: eerste\n\n"))
		if err := w.Flush(); err != nil {
			t.Errorf("Flush: %v", err)
		}
		<-release // de handler is nog NIET klaar
		w.Write([]byte("data: tweede\n\n"))
		w.Flush()
	})
	srv := httptest.NewServer(bridge(m.Handler()))
	defer srv.Close()
	defer close(release)

	resp, err := http.Get(srv.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}

	got := make(chan string, 1)
	go func() {
		line, err := bufio.NewReader(resp.Body).ReadString('\n')
		if err != nil {
			return
		}
		got <- strings.TrimSpace(line)
	}()
	select {
	case line := <-got:
		if line != "data: eerste" {
			t.Errorf("eerste regel = %q", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("niets ontvangen terwijl de handler nog liep: de stream buffert")
	}
}

// TestBridgeImpliciete200: schrijven zonder WriteHeader is een 200, zoals bij
// net/http — anders zou elke handler die dat weglaat stil een 0 sturen.
func TestBridgeImpliciete200(t *testing.T) {
	srv := httptest.NewServer(bridge(func(w ResponseWriter, r *Request) {
		w.Header().Set("X-Set-Voor-Write", "ja")
		w.Write([]byte("kaal"))
	}))
	defer srv.Close()

	var cl Client
	resp, err := cl.Do(Call{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("X-Set-Voor-Write") != "ja" {
		t.Error("een header die vóór de eerste Write gezet is, ging verloren")
	}
}

// TestClientTimeout: de termijn geldt en levert een fout, geen half antwoord.
func TestClientTimeout(t *testing.T) {
	blok := make(chan struct{})
	srv := httptest.NewServer(bridge(func(w ResponseWriter, r *Request) { <-blok }))
	defer srv.Close()
	defer close(blok)

	cl := Client{Timeout: 150 * time.Millisecond}
	start := time.Now()
	if _, err := cl.Do(Call{URL: srv.URL}); err == nil {
		t.Fatal("geen fout op een server die niet antwoordt")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("Do duurde %v op een termijn van 150ms", d)
	}
}
