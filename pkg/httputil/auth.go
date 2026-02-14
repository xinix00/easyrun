package httputil

import "net/http"

// RequireAPIKey returns middleware that checks the X-API-Key header.
// If apiKey is empty, authentication is disabled (all requests pass through).
func RequireAPIKey(apiKey string, next http.HandlerFunc) http.HandlerFunc {
	if apiKey == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != apiKey {
			WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(w, r)
	}
}
