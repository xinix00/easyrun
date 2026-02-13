package httputil

import (
	"encoding/json"
	"net/http"
)

// WriteJSON writes a JSON response with the given status code
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

// WriteError writes a JSON error response
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

// SSEWriter wraps a ResponseWriter for Server-Sent Events streaming.
// Returns nil if the ResponseWriter doesn't support flushing.
func SSEWriter(w http.ResponseWriter) *SSE {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	return &SSE{w: w, f: flusher}
}

// SSE writes Server-Sent Events to a ResponseWriter
type SSE struct {
	w http.ResponseWriter
	f http.Flusher
}

// WriteEvent writes a named SSE event with data and flushes
func (s *SSE) WriteEvent(event, data string) {
	_, _ = s.w.Write([]byte("event: " + event + "\ndata: " + data + "\n\n"))
	s.f.Flush()
}

// WriteData writes an SSE data-only event and flushes
func (s *SSE) WriteData(data string) {
	_, _ = s.w.Write([]byte("data: " + data + "\n\n"))
	s.f.Flush()
}

// Flush exposes the underlying flusher for raw forwarding use cases
func (s *SSE) Flush() { s.f.Flush() }
