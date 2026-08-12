//go:build !tamago

package hophttp

// serve_host.go — net/http underneath, for the orchestrator on linux/darwin.
//
// On a host net/http costs nothing (the binary is a normal binary) and it brings
// a decade of hardening: header limits, slowloris guards, connection reuse. So
// the host keeps it. What it does NOT keep is net/http's ServeMux — routing is
// ours on both platforms (see mux.go), so this file is only the transport: parse
// a request, hand it to our Handler, write the response back.

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// ErrServerClosed is returned by ListenAndServe after Shutdown.
var ErrServerClosed = http.ErrServerClosed

// Server serves one address.
type Server struct {
	srv *http.Server
}

// NewServer prepares a server for addr. Nothing listens until ListenAndServe.
//
// The timeouts are the ones hop already ran with: headers (including the
// X-Hop-Auth signature) must arrive promptly, and there is deliberately no
// write timeout because /v1/events is a long-lived SSE stream that would
// otherwise be cut off mid-flight.
func NewServer(addr string, h Handler) *Server {
	return &Server{srv: &http.Server{
		Addr:              addr,
		Handler:           bridge(h),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}}
}

// Addr is the address the server was built for.
func (s *Server) Addr() string { return s.srv.Addr }

// ListenAndServe serves until Shutdown, then returns ErrServerClosed.
func (s *Server) ListenAndServe() error { return s.srv.ListenAndServe() }

// Shutdown stops accepting and waits for in-flight requests, bounded by ctx.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// bridge turns our Handler into an http.Handler: it converts the request into
// our shape and wraps the writer.
func bridge(h Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := make(Header, len(r.Header))
		for k, v := range r.Header {
			hdr[k] = flatten(v)
		}
		h(&hostWriter{w: w, hdr: make(Header, 4)}, &Request{
			Method:     r.Method,
			Path:       cleanPath(r.URL.Path),
			RawQuery:   r.URL.RawQuery,
			Header:     hdr,
			Body:       r.Body,
			RemoteAddr: r.RemoteAddr,
			ctx:        r.Context(),
		})
	})
}

// hostWriter is our ResponseWriter over net/http's. It keeps its own header map
// (one value per name, like leanhttp) and copies it over on WriteHeader — after
// that point net/http ignores header changes anyway, so there is nothing to
// keep in sync.
type hostWriter struct {
	w     http.ResponseWriter
	hdr   Header
	wrote bool
}

func (h *hostWriter) Header() Header { return h.hdr }

func (h *hostWriter) WriteHeader(status int) {
	if h.wrote {
		return // a second status line is a bug, not something to send twice
	}
	h.wrote = true
	dst := h.w.Header()
	for k, v := range h.hdr {
		dst.Set(k, v)
	}
	h.w.WriteHeader(status)
}

func (h *hostWriter) Write(p []byte) (int, error) {
	h.WriteHeader(StatusOK) // no-op once written; mirrors net/http's implicit 200
	return h.w.Write(p)
}

// Flush is never absent here in practice (net/http's writer flushes), but the
// assertion can fail behind a middleware that wraps the writer, and then a
// stream would silently buffer. Say so instead.
func (h *hostWriter) Flush() error {
	f, ok := h.w.(http.Flusher)
	if !ok {
		return errors.New("hophttp: this response writer cannot flush, so a stream would buffer")
	}
	h.WriteHeader(StatusOK)
	f.Flush()
	return nil
}

func (h *hostWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := h.w.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hophttp: this response writer cannot be hijacked")
	}
	return hj.Hijack()
}
