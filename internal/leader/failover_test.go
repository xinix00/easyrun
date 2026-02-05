package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"easyrun/internal/types"
)

// ============== MOCK AGENT FOR INTEGRATION TESTS ==============

// mockAgent simulates an easyrun agent for testing
type mockAgent struct {
	server   *httptest.Server
	mu       sync.Mutex
	jobs     map[string]*types.Job // jobID -> job (for backwards compat)
	tasks    []*types.Task         // all running tasks
	runCalls int
	taskSeq  int
	failRuns bool // if true, all /run requests will fail
}

func newMockAgent() *mockAgent {
	ma := &mockAgent{
		jobs: make(map[string]*types.Job),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/run", ma.handleRun)
	mux.HandleFunc("/tasks", ma.handleTasks)
	mux.HandleFunc("/delete/", ma.handleDelete)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ma.server = httptest.NewServer(mux)
	return ma
}

func (ma *mockAgent) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ma.mu.Lock()
	if ma.failRuns {
		ma.mu.Unlock()
		http.Error(w, "simulated failure", http.StatusServiceUnavailable)
		return
	}
	ma.mu.Unlock()

	var job types.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	ma.mu.Lock()
	ma.jobs[job.Name] = &job
	ma.runCalls++
	ma.taskSeq++
	task := &types.Task{
		ID:      fmt.Sprintf("task-%s-%d", job.Name, ma.taskSeq),
		JobName: job.Name,
		State:   types.TaskRunning,
	}
	ma.tasks = append(ma.tasks, task)
	ma.mu.Unlock()

	json.NewEncoder(w).Encode(task)
}

// SetFailRuns makes all /run requests fail when set to true
func (ma *mockAgent) SetFailRuns(fail bool) {
	ma.mu.Lock()
	ma.failRuns = fail
	ma.mu.Unlock()
}

func (ma *mockAgent) URL() string {
	return ma.server.URL
}

func (ma *mockAgent) Close() {
	ma.server.Close()
}

func (ma *mockAgent) GetJobs() []*types.Job {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	jobs := make([]*types.Job, 0, len(ma.jobs))
	for _, j := range ma.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (ma *mockAgent) RunCallCount() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	return ma.runCalls
}

func (ma *mockAgent) TaskCount() int {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	return len(ma.tasks)
}

func (ma *mockAgent) handleTasks(w http.ResponseWriter, r *http.Request) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	json.NewEncoder(w).Encode(ma.tasks)
}

func (ma *mockAgent) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobName := r.URL.Path[len("/delete/"):]

	ma.mu.Lock()
	// Remove ONE task with this jobName
	deleted := 0
	for i, task := range ma.tasks {
		if task.JobName == jobName {
			ma.tasks = append(ma.tasks[:i], ma.tasks[i+1:]...)
			deleted = 1
			break
		}
	}
	delete(ma.jobs, jobName)
	ma.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]int{"deleted": deleted})
}

// ============== LEADER FAILOVER TESTS ==============
// These tests verify that state is correctly transferred when a new leader takes over

func TestFailoverNewLeaderLearnsJobsFromAgents(t *testing.T) {
	// Scenario: Old leader dies, new leader comes up empty, agents heartbeat with their jobs

	// NEW leader starts with empty store
	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Agent 1 heartbeats with its jobs (from old leader era)
	agent1Jobs := []*types.Job{
		{ID: "webapp-id", Name: "webapp", Command: "./webapp", Count: 2},
		{ID: "api-id", Name: "api", Command: "./api", Count: 1},
	}
	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agent1Jobs, time.Now(, ""))

	// Agent 2 heartbeats with its jobs
	agent2Jobs := []*types.Job{
		{ID: "webapp-id", Name: "webapp", Command: "./webapp", Count: 2}, // Same job, running on both
		{ID: "worker-id", Name: "worker", Command: "./worker", Count: 1},
	}
	newLeader.Heartbeat("agent-2", "http://10.0.0.2:8080", agent2Jobs, time.Now(, ""))

	time.Sleep(20 * time.Millisecond)

	// Verify new leader learned all unique jobs
	jobs := newLeaderStore.GetJobs()
	if len(jobs) != 3 {
		t.Errorf("New leader should have 3 jobs, got %d", len(jobs))
	}

	// Verify placement is tracked (by job ID)
	placement := newLeader.GetPlacement("webapp-id")
	if len(placement) != 2 {
		t.Errorf("webapp should be placed on 2 agents, got %d", len(placement))
	}
}

func TestFailoverStateTimeBasedSync(t *testing.T) {
	// Scenario: Agent has NEWER state than new leader (agent was more recently synced with old leader)

	newLeaderStore := NewMockJobStore()
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Hour) // New leader has old/empty state

	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Agent has newer state (was synced with old leader just before crash)
	newerStateTime := time.Now()
	agentJobs := []*types.Job{
		{Name: "critical", Command: "./critical"},
	}

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agentJobs, newerStateTime, "")
	time.Sleep(20 * time.Millisecond)

	// Verify new leader adopted the newer state
	if !newLeaderStore.stateTime.Equal(newerStateTime) {
		t.Error("New leader should have adopted agent's newer stateTime")
	}

	jobs := newLeaderStore.GetJobs()
	if len(jobs) != 1 || jobs[0].Name != "critical" {
		t.Error("New leader should have synced critical from agent")
	}
}

func TestFailoverCountMinusOneDispatchesToNewAgents(t *testing.T) {
	// Scenario: count=-1 job exists, new agent joins, should receive the job
	// This tests the FULL flow with actual HTTP calls

	// Create mock agents
	agent1 := newMockAgent()
	defer agent1.Close()
	agent2 := newMockAgent()
	defer agent2.Close()

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Agent 1 heartbeats with a count=-1 job (daemon that runs everywhere)
	daemonJob := &types.Job{
		ID:      "daemon-id",
		Name:    "daemon",
		Command: "./daemon",
		Count:   -1,
	}
	newLeader.Heartbeat("agent-1", agent1.URL(), []*types.Job{daemonJob}, time.Now(, ""))
	time.Sleep(20 * time.Millisecond)

	// Now agent 2 joins - it should receive the count=-1 job via dispatch
	newLeader.Heartbeat("agent-2", agent2.URL(), nil, time.Time{}, "")
	time.Sleep(50 * time.Millisecond)

	// Verify agent 2 received the job via /run
	if agent2.RunCallCount() != 1 {
		t.Errorf("Agent 2 should have received 1 /run call, got %d", agent2.RunCallCount())
	}

	agent2Jobs := agent2.GetJobs()
	if len(agent2Jobs) != 1 || agent2Jobs[0].Name != "daemon" {
		t.Errorf("Agent 2 should have daemon job, got %v", agent2Jobs)
	}

	// Verify placement now includes both agents
	placement := newLeader.GetPlacement("daemon-id")
	if len(placement) != 2 {
		t.Errorf("daemon-job should be on 2 agents, got %d: %v", len(placement), placement)
	}
}

func TestFailoverMultipleAgentsWithDaemonJob(t *testing.T) {
	// Multiple agents heartbeat, all should have the daemon job tracked
	// Note: When agents heartbeat sequentially, new agents will receive /run calls
	// for count=-1 jobs even if they report having them (ensureAllAgentJobs runs
	// before placement is updated). This is acceptable - agents handle idempotent dispatch.

	// Create 3 mock agents
	agents := make([]*mockAgent, 3)
	for i := range agents {
		agents[i] = newMockAgent()
		defer agents[i].Close()
	}

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	daemonJob := &types.Job{
		ID:      "monitoring-id",
		Name:    "monitoring",
		Command: "./monitor",
		Count:   -1,
	}

	// All agents heartbeat saying they have the daemon job
	for i, agent := range agents {
		agentID := string(rune('a' + i))
		newLeader.Heartbeat("agent-"+agentID, agent.URL(), []*types.Job{daemonJob}, time.Now(, ""))
	}
	time.Sleep(50 * time.Millisecond)

	// Verify all 3 agents are in placement
	placement := newLeader.GetPlacement("monitoring-id")
	if len(placement) != 3 {
		t.Errorf("monitoring should be on 3 agents, got %d: %v", len(placement), placement)
	}

	// First agent reports the job so no dispatch needed
	// Subsequent agents get /run calls because ensureAllAgentJobs runs before placement update
	if agents[0].RunCallCount() != 0 {
		t.Errorf("First agent should have 0 /run calls (introduced the job), got %d", agents[0].RunCallCount())
	}
	// Agents 1 and 2 will receive /run because they're "new" and placement wasn't updated yet
	for i := 1; i < len(agents); i++ {
		if agents[i].RunCallCount() != 1 {
			t.Errorf("Agent %d should have 1 /run call (new agent), got %d", i, agents[i].RunCallCount())
		}
	}
}

func TestFailoverNewAgentGetsAllDaemonJobs(t *testing.T) {
	// New agent joins cluster with multiple count=-1 jobs - should get all of them

	existingAgent := newMockAgent()
	defer existingAgent.Close()
	newAgent := newMockAgent()
	defer newAgent.Close()

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Existing agent has 3 daemon jobs
	daemonJobs := []*types.Job{
		{ID: "easydns-id", Name: "easydns", Command: "./easydns", Count: -1},
		{ID: "easylb-id", Name: "easylb", Command: "./easylb", Count: -1},
		{ID: "monitoring-id", Name: "monitoring", Command: "./monitor", Count: -1},
	}
	newLeader.Heartbeat("existing", existingAgent.URL(), daemonJobs, time.Now(, ""))
	time.Sleep(20 * time.Millisecond)

	// New agent joins with no jobs
	newLeader.Heartbeat("new-agent", newAgent.URL(), nil, time.Time{}, "")
	time.Sleep(100 * time.Millisecond)

	// New agent should have received all 3 daemon jobs
	if newAgent.RunCallCount() != 3 {
		t.Errorf("New agent should have received 3 /run calls, got %d", newAgent.RunCallCount())
	}

	newAgentJobs := newAgent.GetJobs()
	if len(newAgentJobs) != 3 {
		t.Errorf("New agent should have 3 jobs, got %d", len(newAgentJobs))
	}
}

func TestFailoverPreservesJobMetadata(t *testing.T) {
	// Verify that job metadata (tags, ports, resources) survives failover

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	originalJob := &types.Job{
		Name:        "complex",
		Command:     "./complex",
		Count:       3,
		CPUShares:   100,
		MemoryLimit: 512 * 1024 * 1024,
		Ports:       map[string]int{"http": 8080, "grpc": 9090},
		Env:         map[string]string{"ENV": "prod", "DEBUG": "false"},
		Tags:        map[string]string{"urlprefix": "api.example.com", "lb": "main"},
	}

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", []*types.Job{originalJob}, time.Now(, ""))
	time.Sleep(20 * time.Millisecond)

	// Retrieve and verify
	recoveredJob := newLeaderStore.GetJob("complex")
	if recoveredJob == nil {
		t.Fatal("Job not recovered")
	}

	if recoveredJob.CPUShares != 100 {
		t.Errorf("CPUShares = %d, want 100", recoveredJob.CPUShares)
	}
	if recoveredJob.MemoryLimit != 512*1024*1024 {
		t.Errorf("MemoryLimit = %d, want %d", recoveredJob.MemoryLimit, 512*1024*1024)
	}
	if recoveredJob.Ports["http"] != 8080 || recoveredJob.Ports["grpc"] != 9090 {
		t.Errorf("Ports not preserved: %v", recoveredJob.Ports)
	}
	if recoveredJob.Env["ENV"] != "prod" {
		t.Errorf("Env not preserved: %v", recoveredJob.Env)
	}
	if recoveredJob.Tags["urlprefix"] != "api.example.com" {
		t.Errorf("Tags not preserved: %v", recoveredJob.Tags)
	}
}

func TestFailoverAgentWithEmptyJobsStillRegisters(t *testing.T) {
	// Agent with no jobs should still register (might be new/fresh agent)

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Agent heartbeats with no jobs
	newLeader.Heartbeat("fresh-agent", "http://10.0.0.99:8080", nil, time.Time{}, "")
	time.Sleep(10 * time.Millisecond)

	agents := newLeader.GetAgents()
	if len(agents) != 1 {
		t.Errorf("Expected 1 agent registered, got %d", len(agents))
	}
	if agents[0].ID != "fresh-agent" {
		t.Errorf("Agent ID = %q, want %q", agents[0].ID, "fresh-agent")
	}
}

func TestFailoverOlderAgentStateIgnored(t *testing.T) {
	// If new leader has newer state than agent, agent state should not overwrite

	newLeaderStore := NewMockJobStore()
	newLeaderStore.stateTime = time.Now() // New leader has current state
	newLeaderStore.StoreJob(&types.Job{ID: "leader-job-id", Name: "from-leader", Command: "echo"})

	newLeader := New("new-leader", newLeaderStore, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Agent has OLDER state
	olderTime := time.Now().Add(-1 * time.Hour)
	agentJobs := []*types.Job{
		{ID: "old-job-id", Name: "old-job", Command: "old"},
	}

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agentJobs, olderTime, "")
	time.Sleep(20 * time.Millisecond)

	// Leader should still learn about placement (agent IS running the job)
	// but should NOT replace leader's newer stateTime
	jobs := newLeaderStore.GetJobs()

	// Should have both jobs (leader learns placement regardless of stateTime)
	foundLeaderJob := false
	foundAgentJob := false
	for _, j := range jobs {
		if j.Name == "from-leader" {
			foundLeaderJob = true
		}
		if j.Name == "old-job" {
			foundAgentJob = true
		}
	}

	if !foundLeaderJob {
		t.Error("Leader's own job should be preserved")
	}
	if !foundAgentJob {
		t.Error("Agent's job should still be learned for placement tracking")
	}
}

func TestFailoverDispatchFailureDoesNotBreakHeartbeat(t *testing.T) {
	// If dispatch to agent fails, heartbeat should still work and agent should be registered

	// Agent that rejects /run requests
	rejectingAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" {
			http.Error(w, "no capacity", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer rejectingAgent.Close()

	newLeaderStore := NewMockJobStore()
	newLeader := New("new-leader", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// First agent has daemon job
	goodAgent := newMockAgent()
	defer goodAgent.Close()
	daemonJob := &types.Job{ID: "daemon-id", Name: "daemon", Command: "./d", Count: -1}
	newLeader.Heartbeat("good-agent", goodAgent.URL(), []*types.Job{daemonJob}, time.Now(, ""))
	time.Sleep(20 * time.Millisecond)

	// Rejecting agent joins - dispatch will fail but heartbeat should succeed
	newLeader.Heartbeat("rejecting-agent", rejectingAgent.URL, nil, time.Time{}, "")
	time.Sleep(50 * time.Millisecond)

	// Agent should still be registered
	agents := newLeader.GetAgents()
	if len(agents) != 2 {
		t.Errorf("Expected 2 agents registered, got %d", len(agents))
	}

	// Placement should only have good-agent (rejecting agent failed to receive job)
	placement := newLeader.GetPlacement("daemon-id")
	if len(placement) != 1 {
		t.Errorf("daemon should only be on 1 agent (good-agent), got %d: %v", len(placement), placement)
	}
}
