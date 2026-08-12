package hophttp

// recorder.go — the test double for a response, the same idea as
// httptest.ResponseRecorder. It lives in the package rather than in a test file
// because handler tests all over hop need it, and it must exist on a node build
// too (a handler test is not host-only just because it happens to run there).

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
)

// Recorder records what a handler wrote. Use NewRecorder.
type Recorder struct {
	// Code is the status the handler sent; 0 means it never sent one, which is
	// itself worth asserting (a handler that returns without writing leaves the
	// client waiting for the transport's implicit 200).
	Code int

	// Hdr is the response header set.
	Hdr Header

	// Body is everything written. A pointer, like httptest.ResponseRecorder's, so
	// json.NewDecoder(rec.Body) reads without a & at every call site.
	Body *bytes.Buffer

	// Flushes counts Flush calls, so a test can prove a stream actually
	// streamed instead of buffering to the end.
	Flushes int
}

// NewRecorder returns a Recorder ready for use.
func NewRecorder() *Recorder { return &Recorder{Hdr: make(Header, 4), Body: new(bytes.Buffer)} }

func (r *Recorder) Header() Header { return r.Hdr }

func (r *Recorder) WriteHeader(status int) {
	if r.Code == 0 {
		r.Code = status
	}
}

func (r *Recorder) Write(p []byte) (int, error) {
	r.WriteHeader(StatusOK)
	return r.Body.Write(p)
}

func (r *Recorder) Flush() error {
	r.WriteHeader(StatusOK)
	r.Flushes++
	return nil
}

// Hijack always fails: a recorder has no connection to take over. A handler
// that needs one needs a real transport.
func (r *Recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("hophttp: a Recorder has no connection to hijack")
}

// String is the recorded body, for assertions.
func (r *Recorder) String() string { return r.Body.String() }

// NewRequest builds a request for a handler test. target is "/path?query"; body
// may be nil for a bodyless request. Same shape as httptest.NewRequest, so a
// handler test reads the same as it did before the seam existed.
func NewRequest(method, target string, body io.Reader) *Request {
	path, query := target, ""
	if i := strings.IndexByte(target, '?'); i >= 0 {
		path, query = target[:i], target[i+1:]
	}
	if body == nil {
		body = strings.NewReader("")
	}
	return &Request{
		Method:     method,
		Path:       path,
		RawQuery:   query,
		Header:     make(Header, 4),
		Body:       body,
		RemoteAddr: "127.0.0.1:0",
	}
}

// WithPathValues sets the wildcard values a mux would have captured, so a
// handler can be tested without routing it.
func (r *Request) WithPathValues(kv map[string]string) *Request {
	r.vals = kv
	return r
}
