package httputil

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func signed(t *testing.T, key, method, path string, body []byte) *http.Request {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, path, r)
	SignRequest(req, key, body)
	return req
}

func TestRequireHMAC_EmptyKeyPassesThrough(t *testing.T) {
	handler := RequireHMAC("", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRequireHMAC_ValidSignature(t *testing.T) {
	handler := RequireHMAC("secret123", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := signed(t, "secret123", "GET", "/v1/jobs", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestRequireHMAC_ValidSignatureWithBody(t *testing.T) {
	body := []byte(`{"name":"api","count":3}`)
	var got []byte
	handler := RequireHMAC("secret123", func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body) // body must survive the middleware
		w.WriteHeader(http.StatusOK)
	})

	req := signed(t, "secret123", "POST", "/v1/jobs", body)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body not preserved: got %q want %q", got, body)
	}
}

func TestRequireHMAC_MissingSignature(t *testing.T) {
	handler := RequireHMAC("secret123", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/v1/jobs", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestRequireHMAC_WrongKey(t *testing.T) {
	handler := RequireHMAC("secret123", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := signed(t, "wrongkey", "GET", "/v1/jobs", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestRequireHMAC_TamperedPath(t *testing.T) {
	handler := RequireHMAC("secret123", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Signature made for /v1/jobs, replayed against a destructive endpoint.
	req := signed(t, "secret123", "GET", "/v1/jobs", nil)
	req.URL.Path = "/v1/agents/node-1"
	req.Method = "DELETE"
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for path/method tamper, got %d", rec.Code)
	}
}

func TestRequireHMAC_TamperedBody(t *testing.T) {
	handler := RequireHMAC("secret123", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := signed(t, "secret123", "POST", "/v1/jobs", []byte(`{"count":1}`))
	req.Body = io.NopCloser(bytes.NewReader([]byte(`{"count":9999}`))) // swap body after signing
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for body tamper, got %d", rec.Code)
	}
}
