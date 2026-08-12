//go:build !tamago

package httputil

// sign_host.go — SignRequest for the callers that hold a *http.Request.
//
// The CLI is one of them, and it is host-only by nature: it talks to a cluster,
// it never runs on a node. Dragging it onto hophttp would buy nothing (net/http
// is free in a host binary) and would churn a file that works. Everything that
// DOES run on a node signs through SignCall in auth.go, which is also the safer
// of the two: it signs the bytes it is about to send instead of taking them as a
// separate argument that a caller can get wrong.

import "net/http"

// SignRequest sets the X-Hop-Auth header on req. body must be the exact bytes
// that will be sent (nil for bodyless requests). No-op when key is empty, so
// empty-key mode keeps dev/standalone setups unauthenticated.
func SignRequest(req *http.Request, key string, body []byte) {
	if key == "" {
		return
	}
	req.Header.Set(AuthHeader, Sign(key, req.Method, req.URL.Path, body))
}
