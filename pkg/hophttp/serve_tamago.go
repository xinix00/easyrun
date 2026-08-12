//go:build tamago

package hophttp

// serve_tamago.go — leanhttp underneath, for hop inside the HopOS kernel.
//
// This is the file that pays for the whole package: it is the one that does not
// import net/http, and therefore does not drag crypto/tls into a kernel image
// that never opens an https URL.
//
// leanhttp's response writer already satisfies our ResponseWriter method for
// method (that is why the shape is leanhttp's), so there is no writer wrapper
// here — only the request conversion and the listener.

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/xinix00/lean/leanhttp"
)

// ErrServerClosed is returned by ListenAndServe after Shutdown.
var ErrServerClosed = errors.New("hophttp: server closed")

// Server serves one address.
type Server struct {
	addr string
	h    Handler

	mu     sync.Mutex
	l      net.Listener
	closed bool
}

// NewServer prepares a server for addr. Nothing listens until ListenAndServe.
//
// There are no timeouts to set here: leanhttp caps the request head and body
// itself, and a node has no untrusted internet-facing port to guard against
// slowloris — hop's port lives on the node network behind the switch.
func NewServer(addr string, h Handler) *Server { return &Server{addr: addr, h: h} }

// Addr is the address the server was built for.
func (s *Server) Addr() string { return s.addr }

// ListenAndServe serves until Shutdown, then returns ErrServerClosed.
func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed { // Shutdown won the race: do not start accepting after all
		s.mu.Unlock()
		l.Close()
		return ErrServerClosed
	}
	s.l = l
	s.mu.Unlock()

	err = leanhttp.Serve(l, s.serve)
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrServerClosed // the accept error was our own Close
	}
	return err
}

// Shutdown stops accepting. Unlike the host it does not wait for in-flight
// requests: on a node the process is the node, and what follows a shutdown is a
// reboot — there is nothing to hand over to.
func (s *Server) Shutdown(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.l != nil {
		return s.l.Close()
	}
	return nil
}

// serve converts one leanhttp request into ours. The writer passes straight
// through: leanhttp's ResponseWriter has the same methods as ours.
func (s *Server) serve(w leanhttp.ResponseWriter, r *leanhttp.Request) {
	s.h(w, &Request{
		Method:     r.Method,
		Path:       cleanPath(r.Path),
		RawQuery:   r.RawQuery,
		Header:     r.Header,
		Body:       r.Body,
		RemoteAddr: r.RemoteAddr,
		done:       r.Done, // the method, NOT called: see Request.done
	})
}
