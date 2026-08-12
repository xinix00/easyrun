package hophttp

// client.go — the outbound shape. Two transports underneath, same three files
// as the server side.
//
// The bodies here are all []byte and that is not a limitation we regret: every
// request hop sends is a JSON document it already has in memory, and the HMAC in
// pkg/httputil signs sha256(body), so the bytes have to exist before the request
// goes out anyway. Downloads stream in the other direction (Response.Body), and
// that side is a reader.

import (
	"io"
	"time"
)

// Call is one outbound request.
type Call struct {
	Method string // "" = GET
	URL    string
	Header Header // may be nil
	Body   []byte // nil = no body

	// Timeout covers the whole call including reading the body; 0 = the
	// Client's. A long-lived stream (SSE, log tailing) wants 0 on both.
	Timeout time.Duration

	// HeaderTimeout bounds only the wait for the response head, not the body.
	// A 30 MB artifact download wants no total timeout (it would cut the file
	// off) but must not hang forever on a server that accepts the connection
	// and then says nothing. That is two different deadlines, so it is two
	// fields; 0 = no separate bound.
	HeaderTimeout time.Duration
}

// Response is one answer. Close the Body; that releases the connection.
type Response struct {
	StatusCode int
	Status     string // code with reason, e.g. "404 Not Found"
	Header     Header
	Body       io.ReadCloser

	// Length is the announced Content-Length, or -1 when the answer is chunked
	// or runs to EOF. It is a field rather than a header lookup because the two
	// are not the same question: a chunked response may still CARRY a
	// Content-Length header (request smuggling looks exactly like that), and a
	// downloader that trusts the header would then size its file wrong. Both
	// transports set this from what they actually decided to read.
	Length int64
}

// SetHeader sets one header on the call, allocating the map if needed. Saves
// every caller the nil check.
func (c *Call) SetHeader(key, value string) {
	if c.Header == nil {
		c.Header = make(Header, 2)
	}
	c.Header.Set(key, value)
}
