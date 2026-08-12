//go:build tamago

package hophttp

// client_tamago.go — leanhttp underneath, for hop inside the HopOS kernel.
//
// Keep-alive is on (leanhttp.Client pools per host) and that is not a nicety
// here: an agent heartbeats its leader every second, so a fresh TCP handshake
// per call would be the dominant cost of the whole conversation.

import (
	"context"
	"time"

	"github.com/xinix00/lean/leanhttp"
)

// Client sends calls. The zero value works and is safe for concurrent use.
type Client struct {
	// Timeout covers a whole call unless the Call sets its own; 0 = none.
	Timeout time.Duration

	pool leanhttp.Client
}

// Do sends one call.
func (cl *Client) Do(call Call) (*Response, error) {
	return cl.DoContext(context.Background(), call)
}

// DoContext is Do with a lifetime. On a node the cancellation is not plumbed
// into the transport: leanhttp has no context and adding one would mean a
// goroutine per call to watch it. A stream stops when its write fails, which is
// what happens the moment the reader is gone — and hop's own callers pass a
// context that is cancelled at exactly that point, so nothing hangs longer than
// one write.
func (cl *Client) DoContext(_ context.Context, call Call) (*Response, error) {
	timeout := call.Timeout
	if timeout == 0 {
		timeout = cl.Timeout
	}
	resp, err := cl.pool.Do(leanhttp.Call{
		Method:        call.Method,
		URL:           call.URL,
		Header:        call.Header,
		Body:          call.Body,
		Timeout:       timeout,
		HeaderTimeout: call.HeaderTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Header:     resp.Header,
		Body:       resp.Body,
		Length:     resp.Length,
	}, nil
}
