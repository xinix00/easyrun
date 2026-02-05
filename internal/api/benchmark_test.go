package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"easyrun/internal/leader"
	"easyrun/internal/types"
)

// BenchmarkGetAgentsEndpoint measures /v1/agents endpoint throughput
func BenchmarkGetAgentsEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	server := NewServer(l, ":9080")

	// Register 100 agents
	for i := 0; i < 100; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{})
	}

	req := httptest.NewRequest("GET", "/v1/agents", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkGetJobsEndpoint measures /v1/jobs endpoint throughput
func BenchmarkGetJobsEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	server := NewServer(l, ":9080")

	// Create 1000 jobs
	for i := 0; i < 1000; i++ {
		job := &types.Job{
			ID:      fmt.Sprintf("job-%d", i),
			Name:    fmt.Sprintf("job-%d", i),
			Command: "echo test",
		}
		store.jobs[job.Name] = job
	}

	req := httptest.NewRequest("GET", "/v1/jobs", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkPostJobEndpoint measures POST /v1/jobs throughput
func BenchmarkPostJobEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	server := NewServer(l, ":9080")

	// Register agents
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		job := types.Job{
			Name:    fmt.Sprintf("bench-job-%d", i),
			Command: "echo test",
			Count:   1,
		}
		body, _ := json.Marshal(job)
		req := httptest.NewRequest("POST", "/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(w, req)
		if w.Code != 201 && w.Code != 200 {
			b.Fatalf("Expected 201 or 200, got %d", w.Code)
		}
	}
}

// BenchmarkHeartbeatEndpoint measures POST /v1/heartbeat throughput
func BenchmarkHeartbeatEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	server := NewServer(l, ":9080")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hb := map[string]string{
			"id":       fmt.Sprintf("agent-%d", i%100),
			"endpoint": fmt.Sprintf("http://10.0.0.%d:8080", i%100),
		}
		body, _ := json.Marshal(hb)
		req := httptest.NewRequest("POST", "/v1/heartbeat", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkStatusEndpoint measures /v1/status endpoint throughput
func BenchmarkStatusEndpoint(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	server := NewServer(l, ":9080")

	// Register agents
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{})
	}

	req := httptest.NewRequest("GET", "/v1/status", nil)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		server.server.Handler.ServeHTTP(w, req)
		if w.Code != 200 {
			b.Fatalf("Expected 200, got %d", w.Code)
		}
	}
}

// BenchmarkConcurrentRequests measures concurrent API request handling
func BenchmarkConcurrentRequests(b *testing.B) {
	store := newMockJobStore()
	l := leader.New("local-agent", store, nil)
	server := NewServer(l, ":9080")

	// Register agents
	for i := 0; i < 10; i++ {
		agentID := fmt.Sprintf("agent-%d", i)
		l.Heartbeat(agentID, fmt.Sprintf("http://10.0.0.%d:8080", i), nil, time.Time{})
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			var req *http.Request
			switch i % 3 {
			case 0:
				req = httptest.NewRequest("GET", "/v1/agents", nil)
			case 1:
				req = httptest.NewRequest("GET", "/v1/jobs", nil)
			case 2:
				req = httptest.NewRequest("GET", "/v1/status", nil)
			}

			w := httptest.NewRecorder()
			server.server.Handler.ServeHTTP(w, req)
			i++
		}
	})
}

// BenchmarkJSONEncoding measures JSON encoding overhead
func BenchmarkJSONEncoding(b *testing.B) {
	agents := make([]*types.Agent, 100)
	for i := 0; i < 100; i++ {
		agents[i] = &types.Agent{
			ID:       fmt.Sprintf("agent-%d", i),
			Endpoint: fmt.Sprintf("http://10.0.0.%d:8080", i),
			LastSeen: time.Now(),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(agents)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJSONDecoding measures JSON decoding overhead
func BenchmarkJSONDecoding(b *testing.B) {
	job := types.Job{
		Name:        "test-job",
		Command:     "echo test",
		Count:       3,
		CPUShares:   2048,
		MemoryLimit: 1024 * 1024 * 1024,
		Env:         map[string]string{"KEY": "value"},
		Tags:        map[string]string{"service": "api"},
	}
	data, _ := json.Marshal(job)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var decoded types.Job
		err := json.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// mockJobStore for API benchmarks
type mockJobStore struct {
	jobs map[string]*types.Job
}

func newMockJobStore() *mockJobStore {
	return &mockJobStore{jobs: make(map[string]*types.Job)}
}

func (m *mockJobStore) GetJobs() []*types.Job {
	jobs := make([]*types.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (m *mockJobStore) GetJob(id string) *types.Job {
	return m.jobs[id]
}

func (m *mockJobStore) StoreJob(job *types.Job) {
	m.jobs[job.Name] = job
}

func (m *mockJobStore) DeleteJob(jobName string) {
	delete(m.jobs, jobName)
}

func (m *mockJobStore) GetStateTime() time.Time {
	return time.Now()
}

func (m *mockJobStore) SyncJobs(jobs []*types.Job, updated time.Time) {
	for _, job := range jobs {
		m.jobs[job.Name] = job
	}
}
