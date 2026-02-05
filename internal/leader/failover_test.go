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

func (ma *mockAgent) ResetRunCount() {
	ma.mu.Lock()
	defer ma.mu.Unlock()
	ma.runCalls = 0
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

// taskCounts creates a map of jobID -> 1 for each job (1 task per job)
func taskCounts(jobs []*types.Job) map[string]int {
	counts := make(map[string]int)
	for _, j := range jobs {
		counts[j.ID] = 1
	}
	return counts
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
	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agent1Jobs, taskCounts(agent1Jobs), time.Now(), "")

	// Agent 2 heartbeats with its jobs
	agent2Jobs := []*types.Job{
		{ID: "webapp-id", Name: "webapp", Command: "./webapp", Count: 2}, // Same job, running on both
		{ID: "worker-id", Name: "worker", Command: "./worker", Count: 1},
	}
	newLeader.Heartbeat("agent-2", "http://10.0.0.2:8080", agent2Jobs, taskCounts(agent2Jobs), time.Now(), "")

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
		{ID: "critical-id", Name: "critical", Command: "./critical"},
	}

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agentJobs, taskCounts(agentJobs), newerStateTime, "")
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
	newLeader.Heartbeat("agent-1", agent1.URL(), []*types.Job{daemonJob}, taskCounts([]*types.Job{daemonJob}), time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Now agent 2 joins - it should receive the count=-1 job via dispatch
	newLeader.Heartbeat("agent-2", agent2.URL(), nil, nil, time.Time{}, "")
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
	// When agents report having a job, they should NOT receive duplicate /run calls
	// because placement is learned BEFORE ensureAllAgentJobs runs.

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

	// All agents heartbeat saying they ALREADY have the daemon job
	for i, agent := range agents {
		agentID := string(rune('a' + i))
		newLeader.Heartbeat("agent-"+agentID, agent.URL(), []*types.Job{daemonJob}, taskCounts([]*types.Job{daemonJob}), time.Now(), "")
	}
	time.Sleep(50 * time.Millisecond)

	// Verify all 3 agents are in placement
	placement := newLeader.GetPlacement("monitoring-id")
	if len(placement) != 3 {
		t.Errorf("monitoring should be on 3 agents, got %d: %v", len(placement), placement)
	}

	// All agents report having the job, so NONE should receive /run calls
	// (placement is learned before ensureAllAgentJobs runs)
	for i, agent := range agents {
		if agent.RunCallCount() != 0 {
			t.Errorf("Agent %d should have 0 /run calls (already has job), got %d", i, agent.RunCallCount())
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
	newLeader.Heartbeat("existing", existingAgent.URL(), daemonJobs, taskCounts(daemonJobs), time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// New agent joins with no jobs
	newLeader.Heartbeat("new-agent", newAgent.URL(), nil, nil, time.Time{}, "")
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
		ID:          "complex-id",
		Name:        "complex",
		Command:     "./complex",
		Count:       3,
		CPUShares:   100,
		MemoryLimit: 512 * 1024 * 1024,
		Ports:       map[string]int{"http": 8080, "grpc": 9090},
		Env:         map[string]string{"ENV": "prod", "DEBUG": "false"},
		Tags:        map[string]string{"urlprefix": "api.example.com", "lb": "main"},
	}

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", []*types.Job{originalJob}, taskCounts([]*types.Job{originalJob}), time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Retrieve and verify
	recoveredJob := newLeaderStore.GetJobByName("complex")
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
	newLeader.Heartbeat("fresh-agent", "http://10.0.0.99:8080", nil, nil, time.Time{}, "")
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
	agentJobs := []*types.Job{
		{ID: "old-job-id", Name: "old-job", Command: "old"},
	}

	newLeader.Heartbeat("agent-1", "http://10.0.0.1:8080", agentJobs, taskCounts(agentJobs), time.Now(), "")
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
	newLeader.Heartbeat("good-agent", goodAgent.URL(), []*types.Job{daemonJob}, taskCounts([]*types.Job{daemonJob}), time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Rejecting agent joins - dispatch will fail but heartbeat should succeed
	newLeader.Heartbeat("rejecting-agent", rejectingAgent.URL, nil, nil, time.Time{}, "")
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

func TestFailoverNewLeaderDoesNotDuplicateOwnTasks(t *testing.T) {
	// BUG REPRODUCTION: When a node becomes leader, it should NOT redispatch
	// jobs that it already has running locally.
	//
	// Scenario:
	// 1. Node A (old leader) and Node B both run "my-api" (count=2, one each)
	// 2. Node A fails
	// 3. Node B becomes new leader WITH PERSISTED STATE (knows about the job)
	// 4. Node B's task from before should NOT be duplicated
	//
	// The bug: New leader doesn't learn placement from its OWN local agent
	// because of the `id != l.localAgentID` check in Heartbeat().
	// Combined with tryRescheduleUnderscheduled running BEFORE placement is learned,
	// this causes duplicates.

	// Mock agent representing the NEW LEADER's local agent
	localAgent := newMockAgent()
	defer localAgent.Close()

	// The job that was already running BEFORE this node became leader
	existingJob := &types.Job{
		ID:      "my-api-id",
		Name:    "my-api",
		Command: "./api",
		Count:   2, // Desired: 2 instances total
	}

	// New leader has PERSISTED STATE from before the failover
	// (it was synced by the old leader, or loaded from disk)
	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(existingJob) // Job already in store!
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute) // State from before

	// The new leader's ID matches the local agent
	newLeader := New("local-agent-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// The local agent already has 1 running task for this job (from before failover)
	localAgent.mu.Lock()
	localAgent.jobs[existingJob.Name] = existingJob
	localAgent.tasks = append(localAgent.tasks, &types.Task{
		ID:      "existing-task-1",
		JobName: "my-api",
		State:   types.TaskRunning,
	})
	localAgent.mu.Unlock()

	// LOCAL agent heartbeats to the new leader (itself)
	// Note: stateTime is SAME as leader's (not newer), so sync path is NOT triggered
	newLeader.Heartbeat("local-agent-id", localAgent.URL(), []*types.Job{existingJob}, taskCounts([]*types.Job{existingJob}), time.Now(), "")
	time.Sleep(50 * time.Millisecond)

	// BUG: The new leader dispatches instances to itself because:
	// 1. Local agent is "new" (not in agents map)
	// 2. tryRescheduleUnderscheduled runs → finds my-api in store with count=2
	// 3. Placement is EMPTY → job appears under-scheduled (0/2) → dispatch 2!
	// 4. The leader SKIPS learning placement from local agent (id != l.localAgentID)

	// EXPECTED: 0 new /run calls (1 instance already running locally, wait for other agents)
	// ACTUAL (BUG): Should dispatch, but not to itself for instances it already has!
	runCalls := localAgent.RunCallCount()
	if runCalls > 1 {
		// At most 1 call could be justified (the "missing" second instance)
		// But 2 calls means it dispatched both instances ignoring the existing one
		t.Errorf("BUG: New leader dispatched %d tasks, ignoring its own existing task", runCalls)
	}

	// Verify placement includes the local agent (critical for correct scheduling)
	placement := newLeader.GetPlacement("my-api-id")
	hasLocalAgent := false
	for _, p := range placement {
		if p == "local-agent-id" {
			hasLocalAgent = true
			break
		}
	}
	if !hasLocalAgent {
		t.Errorf("BUG: Placement should include local-agent-id, got: %v", placement)
	}
}

func TestFailoverNewLeaderLearnsFromLocalAgent(t *testing.T) {
	// Verify that the new leader learns job placement from its LOCAL agent,
	// not just remote agents.
	//
	// BUG: The `id != l.localAgentID` check in Heartbeat() skips learning
	// placement from the local agent, leaving placement empty.

	localAgent := newMockAgent()
	defer localAgent.Close()

	// Local agent reports jobs it's running
	localJobs := []*types.Job{
		{ID: "job-a-id", Name: "job-a", Command: "./a", Count: 1},
		{ID: "job-b-id", Name: "job-b", Command: "./b", Count: 1},
	}

	// New leader has persisted state (jobs already known)
	newLeaderStore := NewMockJobStore()
	for _, job := range localJobs {
		newLeaderStore.StoreJob(job)
	}
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute)

	newLeader := New("local-agent-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Simulate local agent already having these tasks running
	localAgent.mu.Lock()
	for _, job := range localJobs {
		localAgent.jobs[job.Name] = job
		localAgent.tasks = append(localAgent.tasks, &types.Task{
			ID:      "task-" + job.Name,
			JobName: job.Name,
			State:   types.TaskRunning,
		})
	}
	localAgent.mu.Unlock()

	// Local agent heartbeats (same stateTime, not newer)
	newLeader.Heartbeat("local-agent-id", localAgent.URL(), localJobs, taskCounts(localJobs), time.Now(), "")
	time.Sleep(50 * time.Millisecond)

	// Jobs should still be in store
	jobs := newLeaderStore.GetJobs()
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs in store, got %d", len(jobs))
	}

	// BUG CHECK: Placement should be tracked for local agent
	// The bug is that placement is NOT tracked because of `id != l.localAgentID`
	for _, job := range localJobs {
		placement := newLeader.GetPlacement(job.ID)
		if len(placement) != 1 || placement[0] != "local-agent-id" {
			t.Errorf("BUG: Job %s should have local-agent-id in placement, got: %v", job.Name, placement)
		}
	}

	// BUG CHECK: No new dispatches should have happened (tasks already running)
	// But with the bug, tryRescheduleUnderscheduled dispatches because placement is empty
	if localAgent.RunCallCount() != 0 {
		t.Errorf("BUG: Local agent should not receive new /run calls, got %d", localAgent.RunCallCount())
	}
}

func TestFailoverNewLeaderReschedulesOrphanedJobs(t *testing.T) {
	// When a new leader takes over and loads jobs from disk,
	// jobs that were running on the OLD leader (now dead) should be rescheduled.

	localAgent := newMockAgent()
	defer localAgent.Close()

	// Jobs that were running on the OLD leader (now persisted in state)
	jobs := []*types.Job{
		{ID: "daemon-id", Name: "daemon", Command: "./daemon", Count: -1},
		{ID: "job-1-id", Name: "job-1", Command: "./job1", Count: 1},
		{ID: "job-2-id", Name: "job-2", Command: "./job2", Count: 1},
		{ID: "job-3-id", Name: "job-3", Command: "./job3", Count: 1},
		{ID: "job-4-id", Name: "job-4", Command: "./job4", Count: 1},
		{ID: "job-5-id", Name: "job-5", Command: "./job5", Count: 1},
		{ID: "job-6-id", Name: "job-6", Command: "./job6", Count: 1},
	}

	// New leader loads jobs from persisted state (old leader's state)
	newLeaderStore := NewMockJobStore()
	for _, job := range jobs {
		newLeaderStore.StoreJob(job)
	}
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute)

	newLeader := New("new-leader-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// New leader's local agent heartbeats with NO jobs running
	newLeader.Heartbeat("new-leader-id", localAgent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(100 * time.Millisecond)

	// All 7 jobs should be dispatched
	runCalls := localAgent.RunCallCount()
	if runCalls != 7 {
		t.Errorf("Expected 7 /run calls (1 daemon + 6 regular), got %d", runCalls)
	}

	// Verify all jobs are in placement
	for _, job := range jobs {
		placement := newLeader.GetPlacement(job.ID)
		if len(placement) == 0 {
			t.Errorf("Job %s should be in placement, got empty", job.Name)
		}
	}
}

func TestFailoverNewLeaderWithExistingDaemon(t *testing.T) {
	// BUG REPRODUCTION: New leader already has daemon running (from before failover),
	// heartbeats with that daemon, but orphaned jobs from old leader are NOT rescheduled.
	//
	// Scenario:
	// 1. Old leader was running 6 regular jobs
	// 2. New leader already had the daemon running (count=-1, runs everywhere)
	// 3. Old leader dies, new leader takes over
	// 4. New leader heartbeats reporting the daemon it already has
	// 5. Expected: 6 regular jobs should be dispatched
	// 6. Actual (bug?): Nothing dispatched because agent "already has jobs"?

	localAgent := newMockAgent()
	defer localAgent.Close()

	daemon := &types.Job{ID: "daemon-id", Name: "daemon", Command: "./daemon", Count: -1}
	regularJobs := []*types.Job{
		{ID: "job-1-id", Name: "job-1", Command: "./job1", Count: 1},
		{ID: "job-2-id", Name: "job-2", Command: "./job2", Count: 1},
		{ID: "job-3-id", Name: "job-3", Command: "./job3", Count: 1},
		{ID: "job-4-id", Name: "job-4", Command: "./job4", Count: 1},
		{ID: "job-5-id", Name: "job-5", Command: "./job5", Count: 1},
		{ID: "job-6-id", Name: "job-6", Command: "./job6", Count: 1},
	}

	// New leader loads ALL jobs from persisted state
	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(daemon)
	for _, job := range regularJobs {
		newLeaderStore.StoreJob(job)
	}
	newLeaderStore.stateTime = time.Now().Add(-1 * time.Minute)

	newLeader := New("new-leader-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Simulate: local agent already has the daemon running (from before failover)
	localAgent.mu.Lock()
	localAgent.jobs[daemon.Name] = daemon
	localAgent.tasks = append(localAgent.tasks, &types.Task{
		ID:      "daemon-task",
		JobName: daemon.Name,
		State:   types.TaskRunning,
	})
	localAgent.mu.Unlock()

	// New leader's local agent heartbeats WITH the daemon it already has
	newLeader.Heartbeat("new-leader-id", localAgent.URL(), []*types.Job{daemon}, taskCounts([]*types.Job{daemon}), time.Now(), "")
	time.Sleep(100 * time.Millisecond)

	// EXPECTED: 6 regular jobs should be dispatched (daemon already running, no new dispatch)
	// daemon: already running, placement learned → no dispatch
	// job-1 to job-6: not running, under-scheduled → dispatch all 6
	runCalls := localAgent.RunCallCount()
	if runCalls != 6 {
		t.Errorf("BUG: Expected 6 /run calls (regular jobs only), got %d", runCalls)
	}

	// Verify placement
	daemonPlacement := newLeader.GetPlacement(daemon.ID)
	if len(daemonPlacement) != 1 {
		t.Errorf("Daemon should have 1 placement (no duplicate), got %d", len(daemonPlacement))
	}

	for _, job := range regularJobs {
		placement := newLeader.GetPlacement(job.ID)
		if len(placement) == 0 {
			t.Errorf("Job %s should be in placement, got empty", job.Name)
		}
	}
}

func TestFailoverJobsPinnedToDeadAgent(t *testing.T) {
	// Note: tryRescheduleUnderscheduled uses sendJobToAgent directly,
	// which doesn't check AgentID constraints. This test documents current behavior.
	// TODO: Should pinned jobs be skipped in tryRescheduleUnderscheduled?

	localAgent := newMockAgent()
	defer localAgent.Close()

	oldLeaderID := "old-leader-id"

	// Jobs pinned to old leader
	pinnedJobs := []*types.Job{
		{ID: "job-1-id", Name: "job-1", Command: "./job1", Count: 1, AgentID: oldLeaderID},
		{ID: "job-2-id", Name: "job-2", Command: "./job2", Count: 1, AgentID: oldLeaderID},
		{ID: "job-3-id", Name: "job-3", Command: "./job3", Count: 1, AgentID: oldLeaderID},
	}
	// Jobs NOT pinned (can run anywhere)
	unpinnedJobs := []*types.Job{
		{ID: "job-4-id", Name: "job-4", Command: "./job4", Count: 1},
		{ID: "job-5-id", Name: "job-5", Command: "./job5", Count: 1},
		{ID: "job-6-id", Name: "job-6", Command: "./job6", Count: 1},
	}
	daemon := &types.Job{ID: "daemon-id", Name: "daemon", Command: "./daemon", Count: -1}

	// New leader loads all jobs from persisted state
	newLeaderStore := NewMockJobStore()
	newLeaderStore.StoreJob(daemon)
	for _, job := range pinnedJobs {
		newLeaderStore.StoreJob(job)
	}
	for _, job := range unpinnedJobs {
		newLeaderStore.StoreJob(job)
	}

	newLeader := New("new-leader-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// New leader's local agent heartbeats
	newLeader.Heartbeat("new-leader-id", localAgent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(100 * time.Millisecond)

	// Current behavior: ALL jobs dispatched (pinned jobs not checked)
	// This might be intentional - "rescue" pinned jobs when their node is dead
	runCalls := localAgent.RunCallCount()
	if runCalls != 7 {
		t.Errorf("Expected 7 /run calls (all jobs), got %d", runCalls)
	}
}

func TestFailoverSecondHeartbeatDoesNotReschedule(t *testing.T) {
	// Jobs only get rescheduled on FIRST heartbeat (isNew=true).
	// This documents current behavior - not necessarily a bug.

	localAgent := newMockAgent()
	defer localAgent.Close()

	jobs := []*types.Job{
		{ID: "daemon-id", Name: "daemon", Command: "./daemon", Count: -1},
		{ID: "job-1-id", Name: "job-1", Command: "./job1", Count: 1},
	}

	newLeaderStore := NewMockJobStore()
	for _, job := range jobs {
		newLeaderStore.StoreJob(job)
	}

	newLeader := New("new-leader-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// First heartbeat - agent rejects all /run calls
	localAgent.SetFailRuns(true)
	newLeader.Heartbeat("new-leader-id", localAgent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(50 * time.Millisecond)

	// Now agent is healthy
	localAgent.SetFailRuns(false)

	// Second heartbeat - agent is no longer "new", no rescheduling
	newLeader.Heartbeat("new-leader-id", localAgent.URL(), nil, nil, time.Time{}, "")
	time.Sleep(50 * time.Millisecond)

	// Current behavior: no rescheduling on second heartbeat
	t.Logf("Run calls after second heartbeat: %d", localAgent.RunCallCount())
}

func TestFailoverAgentReportsOnlyRunningJobs(t *testing.T) {
	// TEST: Agent sends only RUNNING jobs (not all known jobs)
	// This allows the leader to correctly reschedule missing jobs.
	//
	// Scenario:
	// 1. Agent was synced with old leader, knows about 7 jobs
	// 2. Agent was only RUNNING the daemon (count=-1)
	// 3. Old leader dies, agent becomes new leader
	// 4. Agent heartbeats with only the daemon (the RUNNING job)
	// 5. Placement learned only for daemon
	// 6. tryRescheduleUnderscheduled sees 6 jobs missing → dispatches them!
	// 7. All 7 jobs now running

	localAgent := newMockAgent()
	defer localAgent.Close()

	daemon := &types.Job{ID: "daemon-id", Name: "daemon", Command: "./daemon", Count: -1}
	regularJobs := []*types.Job{
		{ID: "job-1-id", Name: "job-1", Command: "./job1", Count: 1},
		{ID: "job-2-id", Name: "job-2", Command: "./job2", Count: 1},
		{ID: "job-3-id", Name: "job-3", Command: "./job3", Count: 1},
		{ID: "job-4-id", Name: "job-4", Command: "./job4", Count: 1},
		{ID: "job-5-id", Name: "job-5", Command: "./job5", Count: 1},
		{ID: "job-6-id", Name: "job-6", Command: "./job6", Count: 1},
	}

	// All jobs known to the agent (from SyncJobs with old leader)
	allKnownJobs := append([]*types.Job{daemon}, regularJobs...)

	// New leader store has all jobs
	newLeaderStore := NewMockJobStore()
	for _, job := range allKnownJobs {
		newLeaderStore.StoreJob(job)
	}

	newLeader := New("new-leader-id", newLeaderStore, &http.Client{Timeout: 1 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newLeader.stateLoop(ctx)

	// Simulate: local agent only has daemon RUNNING
	localAgent.mu.Lock()
	localAgent.jobs[daemon.Name] = daemon
	localAgent.tasks = append(localAgent.tasks, &types.Task{
		ID:      "daemon-task",
		JobName: daemon.Name,
		State:   types.TaskRunning,
	})
	localAgent.mu.Unlock()

	// FIX: Agent heartbeats with only RUNNING jobs (the daemon)
	// Agent.GetRunningJobs() returns only jobs with running tasks
	runningJobs := []*types.Job{daemon}
	newLeader.Heartbeat("new-leader-id", localAgent.URL(), runningJobs, taskCounts(runningJobs), time.Now(), "")
	time.Sleep(100 * time.Millisecond)

	// Placement only learned for daemon
	// tryRescheduleUnderscheduled sees 6 jobs under-scheduled → dispatches them

	runCalls := localAgent.RunCallCount()

	// EXPECTED: 6 regular jobs should be dispatched (daemon already running)
	if runCalls != 6 {
		t.Errorf("Expected 6 /run calls for regular jobs, got %d", runCalls)
	}
}

func TestFailoverAgentDiesTasksRescheduled(t *testing.T) {
	// This test uses manual placement setup - see TestFailoverAgentDiesRealisticHeartbeat
	// for the real bug reproduction.

	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &http.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	job := &types.Job{
		ID:      "ticker-id",
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	store.StoreJob(job)

	leader.Heartbeat("agent-a", agentA.URL(), []*types.Job{job}, map[string]int{job.ID: 1}, time.Now(), "")
	leader.Heartbeat("agent-b", agentB.URL(), []*types.Job{job}, map[string]int{job.ID: 1}, time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Manual placement: 11 on A, 9 on B (total 20)
	leader.do(func(s *leaderState) {
		s.placement[job.ID] = []string{
			"agent-a", "agent-a", "agent-a", "agent-a", "agent-a", "agent-a",
			"agent-a", "agent-a", "agent-a", "agent-a", "agent-a",
			"agent-b", "agent-b", "agent-b", "agent-b", "agent-b",
			"agent-b", "agent-b", "agent-b", "agent-b",
		}
	})

	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	leader.Heartbeat("agent-b", agentB.URL(), []*types.Job{job}, map[string]int{job.ID: 9}, time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	leader.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	runCalls := agentB.RunCallCount()
	if runCalls != 11 {
		t.Errorf("Agent B should receive 11 /run calls, got %d", runCalls)
	}

	placement := leader.GetPlacement(job.ID)
	if len(placement) != 20 {
		t.Errorf("Final placement should be 20, got %d", len(placement))
	}
}

func TestFailoverHeartbeatLearnsWrongPlacementCount(t *testing.T) {
	// BUG REPRODUCTION: When leader learns placement from heartbeat,
	// it only creates 1 entry per job per agent, not per task.
	//
	// FIX: Change heartbeat to send task COUNTS per job:
	//   running_job_ids: ["ticker-id"]           ← OLD (broken)
	//   running_tasks:   {"ticker-id": 11}       ← NEW (correct)

	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentA.Close()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &http.Client{Timeout: 100 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	job := &types.Job{
		ID:      "ticker-id",
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	store.StoreJob(job)

	// Agents heartbeat with task COUNTS
	// Agent A has 11 tasks, Agent B has 9 tasks
	leader.Heartbeat("agent-a", agentA.URL(), []*types.Job{job}, map[string]int{job.ID: 11}, time.Now(), "")
	leader.Heartbeat("agent-b", agentB.URL(), []*types.Job{job}, map[string]int{job.ID: 9}, time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Placement should now have 20 entries (11 + 9)
	placement := leader.GetPlacement(job.ID)
	t.Logf("Placement entries from heartbeat: %d", len(placement))

	if len(placement) != 20 {
		t.Errorf("Placement should have 20 entries (11+9), got %d", len(placement))
	}

	// Count per agent
	countA := 0
	countB := 0
	for _, p := range placement {
		if p == "agent-a" {
			countA++
		}
		if p == "agent-b" {
			countB++
		}
	}
	if countA != 11 {
		t.Errorf("Agent A should have 11 placements, got %d", countA)
	}
	if countB != 9 {
		t.Errorf("Agent B should have 9 placements, got %d", countB)
	}
}

func TestFailoverAgentDiesRealisticHeartbeat(t *testing.T) {
	// BUG REPRODUCTION: Realistic heartbeat flow where placement is learned from heartbeats.
	//
	// The problem: Heartbeat only sends job IDs, not task counts.
	// So placement[job.ID] only has 1 entry per agent, regardless of how many tasks.
	//
	// Scenario:
	// 1. Leader dispatches job with count=20 to 2 agents (10 each via round-robin)
	// 2. Agent A crashes
	// 3. Placement only knows "agent-a has ticker, agent-b has ticker" (2 entries)
	// 4. After A dies, placement = ["agent-b"] (1 entry)
	// 5. Leader thinks 1 instance running, should reschedule 19
	// 6. BUG: Heartbeat learns placement as 1 entry per agent, not per task!

	agentA := newMockAgent()
	agentB := newMockAgent()
	defer agentB.Close()

	store := NewMockJobStore()
	leader := New("leader", store, &http.Client{Timeout: 100 * time.Millisecond})
	leader.agentTimeout = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go leader.stateLoop(ctx)

	// Register agents FIRST (before job exists)
	leader.Heartbeat("agent-a", agentA.URL(), nil, nil, time.Time{}, "")
	leader.Heartbeat("agent-b", agentB.URL(), nil, nil, time.Time{}, "")
	time.Sleep(20 * time.Millisecond)

	// Create and dispatch job with count=20
	job := &types.Job{
		ID:      "ticker-id",
		Name:    "ticker",
		Command: "sh -c 'while :; do echo tick; sleep 1; done'",
		Count:   20,
	}
	store.StoreJob(job)

	// Dispatch job - should create 20 tasks round-robin across 2 agents
	err := leader.DispatchJob(job)
	if err != nil {
		t.Fatalf("Failed to dispatch: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Check initial dispatch
	aRuns := agentA.RunCallCount()
	bRuns := agentB.RunCallCount()
	t.Logf("Initial dispatch: agent-a got %d, agent-b got %d (total %d)", aRuns, bRuns, aRuns+bRuns)

	if aRuns+bRuns != 20 {
		t.Fatalf("Should have dispatched 20 tasks, got %d", aRuns+bRuns)
	}

	// Check placement
	placement := leader.GetPlacement(job.ID)
	t.Logf("Initial placement entries: %d", len(placement))

	// Reset counters for the crash test
	agentA.ResetRunCount()
	agentB.ResetRunCount()

	// CHAOS: Agent A crashes
	agentA.Close()
	time.Sleep(300 * time.Millisecond)

	// Keep agent B alive - report task count based on dispatch (placement already tracked)
	leader.Heartbeat("agent-b", agentB.URL(), []*types.Job{job}, map[string]int{job.ID: bRuns}, time.Now(), "")
	time.Sleep(20 * time.Millisecond)

	// Trigger dead agent check
	leader.checkDeadAgents()
	time.Sleep(100 * time.Millisecond)

	// Count how many were rescheduled to B
	rescheduled := agentB.RunCallCount()
	t.Logf("After agent-a death: agent-b received %d /run calls", rescheduled)

	// Agent A had ~10 tasks (round-robin), so B should get ~10 more
	if rescheduled < 8 { // Allow some variance from round-robin
		t.Errorf("BUG: Agent B should receive ~10 /run calls to reschedule, got %d", rescheduled)
	}

	// Final placement should be back to 20
	placement = leader.GetPlacement(job.ID)
	if len(placement) != 20 {
		t.Errorf("BUG: Final placement should be 20, got %d", len(placement))
	}
}
