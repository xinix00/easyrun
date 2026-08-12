//go:build !tamago

package hophttp

// client_host.go — net/http underneath, for the orchestrator and the CLI.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

// Client sends calls. The zero value works. Safe for concurrent use, and
// connections are reused between calls (hop's heartbeat is once a second per
// agent, so a fresh handshake per call is pure waste).
type Client struct {
	// Timeout covers a whole call unless the Call sets its own; 0 = none.
	Timeout time.Duration

	c http.Client
}

// Do sends one call.
func (cl *Client) Do(call Call) (*Response, error) {
	return cl.DoContext(context.Background(), call)
}

// DoContext is Do with a lifetime: cancelling ctx aborts the call, which is how
// a proxied stream stops when its own client walks away.
func (cl *Client) DoContext(ctx context.Context, call Call) (*Response, error) {
	method := call.Method
	if method == "" {
		method = MethodGet
	}
	var body io.Reader
	if call.Body != nil {
		body = bytes.NewReader(call.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, call.URL, body)
	if err != nil {
		return nil, err
	}
	for k, v := range call.Header {
		req.Header.Set(k, v)
	}

	c := cl.c
	c.Timeout = call.Timeout
	if c.Timeout == 0 {
		c.Timeout = cl.Timeout
	}
	if call.HeaderTimeout > 0 {
		// A transport per header-deadline, because that is where net/http keeps
		// the setting. Cloned from the default so everything else (proxy, dial
		// timeouts) stays what the stdlib decided. A clone brings its own
		// connection pool, so this belongs on a download client, not on a hot
		// path that leans on reuse.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.ResponseHeaderTimeout = call.HeaderTimeout
		c.Transport = tr
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	hdr := make(Header, len(resp.Header))
	for k, v := range resp.Header {
		hdr[k] = flatten(v)
	}
	return &Response{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Header:     hdr,
		Body:       resp.Body,
		Length:     resp.ContentLength, // -1 when chunked or unknown, like ours
	}, nil
}

// CloseIdle drops the pooled connections. Same method as the node transport has,
// so a caller does not need to know which one it is talking to.
func (cl *Client) CloseIdle() { cl.c.CloseIdleConnections() }
