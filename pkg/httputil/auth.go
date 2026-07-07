package httputil

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
)

// AuthHeader carries the request signature. The shared key never travels on
// the wire; only this HMAC does.
const AuthHeader = "X-Hop-Auth"

// SigningString builds the canonical string that gets signed:
//
//	METHOD \n PATH \n hex(sha256(body))
//
// PATH is the decoded URL path (no query string); body is the exact request
// body bytes (nil/empty hashes to sha256("")). Both client and server compute
// this identically so the HMAC matches.
func SigningString(method, path string, body []byte) string {
	sum := sha256.Sum256(body)
	return method + "\n" + path + "\n" + hex.EncodeToString(sum[:])
}

// Sign returns the hex HMAC-SHA256 of the canonical string, keyed by key.
func Sign(key, method, path string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(SigningString(method, path, body)))
	return hex.EncodeToString(mac.Sum(nil))
}

// SignRequest sets the X-Hop-Auth header on req. body must be the exact bytes
// that will be sent (nil for bodyless requests). No-op when key is empty, so
// empty-key mode keeps dev/standalone setups unauthenticated.
func SignRequest(req *http.Request, key string, body []byte) {
	if key == "" {
		return
	}
	req.Header.Set(AuthHeader, Sign(key, req.Method, req.URL.Path, body))
}

// RequireHMAC returns middleware that verifies the X-Hop-Auth signature.
// If key is empty, authentication is disabled (all requests pass through).
//
// The request body is buffered to hash it, then restored so the handler reads
// it normally. The signature binds method + path + body, so a captured request
// cannot be replayed against a different endpoint nor have its body tampered —
// and the key itself never appears on the wire, so it cannot be lifted and
// reused to forge new requests. (A verbatim replay of a captured request is
// still possible; see SECURITY.md for the threat model.)
func RequireHMAC(key string, next http.HandlerFunc) http.HandlerFunc {
	if key == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		if r.Body != nil {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				WriteError(w, http.StatusBadRequest, "failed to read body")
				return
			}
			body = b
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		expected := Sign(key, r.Method, r.URL.Path, body)
		got := r.Header.Get(AuthHeader)
		// Constant-time compare — avoids a timing side channel on the signature.
		if !hmac.Equal([]byte(expected), []byte(got)) {
			WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}
