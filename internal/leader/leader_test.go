package leader

import (
	"net/http"
	"testing"
	"time"

	"easyrun/internal/types"
)

// ============== BASIC LEADER TESTS ==============

func TestLeaderNew(t *testing.T) {
	store := NewMockJobStore()
	leader := New("agent-1", store, nil)

	if leader == nil {
		t.Fatal("New returned nil")
	}
	if leader.localAgentID != "agent-1" {
		t.Errorf("localAgentID = %q, want %q", leader.localAgentID, "agent-1")
	}
}

func TestLeaderNewWithCustomClient(t *testing.T) {
	store := NewMockJobStore()
	customClient := &http.Client{Timeout: 30 * time.Second}
	leader := New("agent-1", store, customClient)

	if leader.httpClient != customClient {
		t.Error("httpClient should be the custom client")
	}
}

func TestLeaderGetJobs(t *testing.T) {
	store := NewMockJobStore()
	store.StoreJob(&types.Job{ID: "job-1", Name: "job1"})
	store.StoreJob(&types.Job{ID: "job-2", Name: "job2"})

	leader := New("local-agent", store, nil)

	jobs := leader.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("GetJobs() returned %d jobs, want 2", len(jobs))
	}
}

func TestContainsString(t *testing.T) {
	tests := []struct {
		slice []string
		s     string
		want  bool
	}{
		{[]string{"a", "b", "c"}, "b", true},
		{[]string{"a", "b", "c"}, "d", false},
		{[]string{}, "a", false},
		{nil, "a", false},
	}

	for _, tt := range tests {
		got := containsString(tt.slice, tt.s)
		if got != tt.want {
			t.Errorf("containsString(%v, %q) = %v, want %v", tt.slice, tt.s, got, tt.want)
		}
	}
}
