// Package hophttp is the HTTP layer of hop: one shape for handlers and
// clients, two transports underneath.
//
// Why the seam exists: hop runs in two places. On a host (linux, darwin) it is
// the orchestrator and net/http is free — the binary is a normal binary and the
// stdlib is already there. Inside a HopOS node it runs in the kernel image,
// where net/http costs about 2 MB. Most of that is crypto/tls: net/http links
// it unconditionally, whether or not a single https URL is ever opened. That is
// the whole reason this package is here.
//
// The shape is leanhttp's, not net/http's, and that is deliberate. leanhttp is
// the smaller of the two, so the intersection lives there; writing handlers
// against net/http's shape and shrinking later would mean discovering, one
// method at a time, what the small side cannot do. On a host serve_host.go puts
// net/http underneath, on a node serve_tamago.go puts leanhttp. Handler code
// never sees the difference.
//
// The routing is ours on BOTH platforms (see mux.go). That matters more than it
// looks: if the host used net/http's ServeMux and the node used ours, the two
// would disagree about precedence in exactly the corner cases nobody tests, and
// the node would route differently from the machine it was developed on.
package hophttp

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/xinix00/lean/leanhttp"
)

// Header is one set of headers. Names are case-insensitive on both get and
// set. It is leanhttp's type, so on a node the server hands its headers over
// without a copy.
type Header = leanhttp.Header

// Status codes used across hop. The list is deliberately not exhaustive: a code
// belongs here once something sends it.
const (
	StatusOK                    = 200
	StatusCreated               = 201
	StatusAccepted              = 202
	StatusNoContent             = 204
	StatusMovedPermanently      = 301
	StatusFound                 = 302
	StatusBadRequest            = 400
	StatusUnauthorized          = 401
	StatusNotFound              = 404
	StatusMethodNotAllowed      = 405
	StatusNotAcceptable         = 406
	StatusConflict              = 409
	StatusRequestEntityTooLarge = 413
	StatusInternalServerError   = 500
	StatusBadGateway            = 502
	StatusServiceUnavailable    = 503
)

// Request methods.
const (
	MethodGet    = "GET"
	MethodPost   = "POST"
	MethodPut    = "PUT"
	MethodPatch  = "PATCH"
	MethodDelete = "DELETE"
)

// ResponseWriter is the handler's side of one response. It is method-for-method
// leanhttp's interface, so a leanhttp response writer satisfies it as-is.
//
// Flush is on the interface rather than behind a type assertion (net/http's
// http.Flusher) because every response writer here can flush, and an
// assertion that can fail invites a handler that silently buffers a stream
// that was supposed to arrive live. /v1/events is exactly that stream.
type ResponseWriter interface {
	// Header is the response header set. Changes after WriteHeader are ignored.
	Header() Header

	// WriteHeader sends the status line and headers. The first Write does this
	// with 200 if the handler did not.
	WriteHeader(status int)

	Write(p []byte) (int, error)

	// Flush pushes what is buffered onto the connection. Required for SSE and
	// for log tailing: without it the client waits for the response to end,
	// and a stream never ends.
	Flush() error

	// Hijack takes the raw connection over (websocket upgrades, raw proxying).
	Hijack() (net.Conn, *bufio.ReadWriter, error)
}

// Request is one inbound request. It carries what hop's handlers actually read;
// anything absent here was absent from the handlers too.
type Request struct {
	Method     string // "GET", "POST", …
	Path       string // %-decoded path, always starting with "/"
	RawQuery   string // everything after "?" (see Query)
	Header     Header
	Body       io.Reader // never nil
	RemoteAddr string

	ctx  context.Context
	vals map[string]string // filled by the mux from {wildcards}

	// done is set by the node transport and is NOT called until a handler asks
	// for Context(). That laziness is load-bearing: on a node, asking for the
	// request's lifetime puts a watchdog on the connection (leanhttp's
	// Request.Done reads it to notice the client leaving), and a connection with
	// a watchdog on it can never be reused. Building the context eagerly for
	// every request therefore killed keep-alive for every request — MEASURED on
	// hardware as 200/502/200/502 through hop's agent proxy: the server closed
	// each connection after answering while its own response header still said
	// keep-alive, so the client pooled a dead one and every second call failed.
	done func() <-chan struct{}
}

// PathValue returns the value a {wildcard} in the matched pattern captured, or
// "" when the pattern had no such wildcard.
func (r *Request) PathValue(name string) string { return r.vals[name] }

// Query parses RawQuery. Malformed pairs are dropped rather than failing the
// request: a query string comes from the outside and half of one is not a
// reason to refuse the whole call.
func (r *Request) Query() url.Values {
	v, _ := url.ParseQuery(r.RawQuery)
	return v
}

// Context is the request's lifetime. It is cancelled when the client goes away,
// which is what a long-lived stream watches to stop producing.
//
// On a node the lifetime is not free: see the done field. So only ask for it if
// you are going to watch it — a handler that answers and returns should not.
func (r *Request) Context() context.Context {
	switch {
	case r.ctx != nil:
		return r.ctx
	case r.done != nil:
		return connContext{done: r.done()}
	}
	return context.Background()
}

// connContext is a lifetime with nothing but an end: no deadline, no values.
// It exists because the node transport has a done-channel where the host has a
// context, and hop asks for neither a deadline nor a value.
type connContext struct {
	done <-chan struct{}
}

func (connContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c connContext) Done() <-chan struct{}     { return c.done }
func (connContext) Value(any) any               { return nil }

func (c connContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// WithContext returns a copy of the request bound to ctx. The transports set the
// lifetime themselves, so this is for a test that needs to cancel a handler the
// way a client walking away would, and for middleware that narrows it.
func (r *Request) WithContext(ctx context.Context) *Request {
	cp := *r
	cp.ctx = ctx
	return &cp
}

// Handler serves one request. It is a func and not an interface: hop wraps
// handlers in middleware (auth, CORS) far more often than it implements them on
// a type, and a func type makes that wrapping the trivial case.
type Handler func(w ResponseWriter, r *Request)

// Error replies with a plain-text error and that status. Nothing may be written
// to w before this.
func Error(w ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	w.Write([]byte(msg + "\n"))
}

// Redirect replies with a redirect to location.
func Redirect(w ResponseWriter, location string, status int) {
	w.Header().Set("Location", location)
	w.WriteHeader(status)
}

// cleanPath normalises a request path the way net/http's ServeMux does: a
// leading slash, no "." or ".." elements, no doubled slashes. It runs in the
// transports, so a handler and the router always see the same clean path.
//
// Without it the two platforms would agree with each other but not with the
// stdlib, and "/v1/agents/../.." would reach a subtree handler that net/http
// would never have given it. Nothing here maps a path to a file, so this is not
// a hole being closed — it is one less difference between the node and the
// machine hop was developed on.
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	// path.Clean drops a trailing slash, and that slash is meaningful here: it
	// is what makes a request match a subtree pattern.
	if cleaned != "/" && strings.HasSuffix(p, "/") {
		cleaned += "/"
	}
	return cleaned
}

// flatten joins a multi-value header into the one-value form this package uses
// (RFC 9110 §5.3). Set-Cookie is the one header that must never be folded — its
// values contain commas — but that is a RESPONSE header, and this only ever
// sees a request.
func flatten(vals []string) string {
	if len(vals) == 1 {
		return vals[0]
	}
	return strings.Join(vals, ", ")
}
