package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"easyrun/internal/runner"
	"easyrun/internal/types"
	"easyrun/pkg/config"
)

// State represents persisted state
type State struct {
	Jobs    []*types.Job `json:"jobs"`
	Updated time.Time    `json:"updated"`
}

const (
	defaultMaxRestarts     = 5
	taskMonitorInterval    = 5 * time.Second
	defaultHealthTimeout   = 5 * time.Second
	shutdownTimeout        = 5 * time.Second
	proxyTimeout           = 10 * time.Second
	stateChannelBufferSize = 64
)

// agentState holds all mutable state (owned by single goroutine)
type agentState struct {
	jobs      map[string]*types.Job
	tasks     map[string]*types.Task
	stateTime time.Time

	// Capacity reservations: resources claimed by accepted-but-not-yet-started jobs.
	// Prevents TOCTOU race where concurrent /run requests all pass hasCapacity
	// before any task appears in state.
	reservedCPU int
	reservedMem uint64
}

// Agent runs jobs and reports status
type Agent struct {
	id       string
	endpoint string
	config   *config.Config
	execRunner runner.Runner
	dockerRunner  runner.Runner
	sysInfo  SystemInfo // detected once at startup

	ops chan func(*agentState) // all state access goes through here

	server     *http.Server
	getLeader  func() string // returns current leader address (for proxying)
	httpClient *http.Client

	needsSave atomic.Bool // flag for debounced persistence
}

// New creates a new agent with optional runner (nil uses default ExecRunner)
func New(cfg *config.Config, id string, r runner.Runner) *Agent {
	if r == nil {
		runnerCfg := &runner.Config{
			RootfsBase:   cfg.Paths.RootfsBase,
			ArtifactsDir: cfg.Paths.Artifacts,
			MaxCPUShares: cfg.Capacity.CPUShares,
			Isolate:      cfg.Runner.Isolate,
		}
		r = runner.NewExecRunner(runnerCfg)
	}

	endpoint := fmt.Sprintf("http://%s:%d", cfg.Node.IP, cfg.Node.Port)

	return &Agent{
		id:           id,
		endpoint:     endpoint,
		config:       cfg,
		execRunner: r,
		dockerRunner: runner.NewDockerRunner(),
		sysInfo:      GetSystemInfo(), // detect once at startup
		ops:          make(chan func(*agentState), stateChannelBufferSize),
		httpClient:   &http.Client{Timeout: proxyTimeout},
		// needsSave is zero-initialized (false)
	}
}

// SetLeaderFunc sets the function to get the current leader address (for proxying cluster requests)
func (a *Agent) SetLeaderFunc(fn func() string) {
	a.getLeader = fn
}

// SetSysInfo overrides detected system info (for testing)
func (a *Agent) SetSysInfo(info SystemInfo) {
	a.sysInfo = info
}

// ID returns the agent ID
func (a *Agent) ID() string {
	return a.id
}

// Endpoint returns the agent's HTTP endpoint
func (a *Agent) Endpoint() string {
	return a.endpoint
}

// Init performs startup cleanup (removes old task directories and containers)
func (a *Agent) Init() error {
	if err := a.execRunner.Cleanup(); err != nil {
		return err
	}
	return a.dockerRunner.Cleanup()
}

// runnerFor returns the appropriate runner based on driver
func (a *Agent) runnerFor(driver string) runner.Runner {
	if driver == types.DriverDocker {
		return a.dockerRunner
	}
	return a.execRunner
}

// stateLoop is the single goroutine that owns all mutable state
func (a *Agent) stateLoop(ctx context.Context) {
	state := &agentState{
		jobs:  make(map[string]*types.Job),
		tasks: make(map[string]*types.Task),
	}

	for {
		select {
		case <-ctx.Done():
			return
		case op := <-a.ops:
			op(state)
		}
	}
}

// do executes an operation on state (fire-and-forget)
func (a *Agent) do(op func(*agentState)) {
	a.ops <- op
}

// query executes an operation and waits for result
func query[T any](a *Agent, fn func(*agentState) T) T {
	result := make(chan T, 1)
	a.ops <- func(s *agentState) {
		result <- fn(s)
	}
	return <-result
}

// Run starts the agent HTTP server and state loop
func (a *Agent) Run(ctx context.Context) error {
	// Start the state loop
	go a.stateLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/capacity", a.handleCapacity)
	mux.HandleFunc("/tasks", a.handleTasks)
	mux.HandleFunc("/run", a.handleRun)
	mux.HandleFunc("/delete/", a.handleDelete)
	mux.HandleFunc("/logs/", a.handleLogs)
	mux.HandleFunc("/leader", a.handleLeader)

	// Proxy endpoints - forward to leader for cluster-wide operations
	mux.HandleFunc("/v1/agents", a.proxyToLeader)
	mux.HandleFunc("/v1/jobs", a.proxyToLeader)
	mux.HandleFunc("/v1/jobs/", a.proxyToLeader) // DELETE /v1/jobs/{id}
	mux.HandleFunc("/v1/status", a.proxyToLeader)

	addr := fmt.Sprintf("%s:%d", a.config.Node.IP, a.config.Node.Port)
	a.server = &http.Server{
		Addr:    addr,
		Handler: corsMiddleware(mux),
	}

	go a.monitorTasks(ctx)

	go func() {
		<-ctx.Done()
		a.shutdown()
	}()

	log.Printf("Agent listening on %s", addr)
	if err := a.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// StopAllTasks stops all running tasks and removes them from state (used when agent is isolated)
func (a *Agent) StopAllTasks() {
	tasks := query(a, func(s *agentState) []*types.Task {
		var running []*types.Task
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				task.State = types.TaskStopping
				running = append(running, task)
			}
		}
		return running
	})

	if len(tasks) == 0 {
		return
	}

	for _, task := range tasks {
		if err := a.runnerFor(task.Driver).Stop(task); err != nil {
			log.Printf("Failed to stop task %s: %v", task.ID, err)
		}
	}

	a.do(func(s *agentState) {
		for _, task := range tasks {
			delete(s.tasks, task.ID)
		}
	})
	a.scheduleSave()

	log.Printf("Isolation mode: stopped and removed %d tasks", len(tasks))
}

// shutdown gracefully stops all tasks
func (a *Agent) shutdown() {
	log.Println("Agent shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	a.server.Shutdown(ctx)

	// Mark running tasks as stopping, then stop them
	tasks := query(a, func(s *agentState) []*types.Task {
		var running []*types.Task
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				task.State = types.TaskStopping
				running = append(running, task)
			}
		}
		return running
	})

	for _, task := range tasks {
		if err := a.runnerFor(task.Driver).Stop(task); err != nil {
			log.Printf("Failed to stop task %s: %v", task.ID, err)
		}
	}
}

// corsMiddleware adds CORS headers for browser access
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// GetJobByName finds a job by name (jobs are stored by ID)
func (a *Agent) GetJobByName(jobName string) *types.Job {
	return query(a, func(s *agentState) *types.Job {
		for _, job := range s.jobs {
			if job.Name == jobName {
				return job
			}
		}
		return nil
	})
}

// GetJobs returns all jobs this agent knows about (for JobStore interface)
func (a *Agent) GetJobs() []*types.Job {
	return query(a, func(s *agentState) []*types.Job {
		jobs := make([]*types.Job, 0, len(s.jobs))
		for _, j := range s.jobs {
			jobs = append(jobs, j)
		}
		return jobs
	})
}

// GetPlacedTaskCounts returns a map of jobID -> number of placed tasks on this agent.
// Counts ALL tasks (not just running) because placed = what the leader dispatched here.
func (a *Agent) GetPlacedTaskCounts() map[string]int {
	return query(a, func(s *agentState) map[string]int {
		counts := make(map[string]int)
		for _, task := range s.tasks {
			if task.JobID != "" {
				counts[task.JobID]++
			}
		}
		return counts
	})
}

// GetJob returns a specific job by ID
func (a *Agent) GetJob(id string) *types.Job {
	return query(a, func(s *agentState) *types.Job {
		return s.jobs[id]
	})
}

// StoreJob stores a job (used by leader when it learns about remote jobs)
func (a *Agent) StoreJob(job *types.Job) {
	a.do(func(s *agentState) {
		s.jobs[job.ID] = job
		s.stateTime = time.Now() // Track when state actually changed
	})
}

// DeleteJob removes a job from the store by ID (for JobStore interface)
func (a *Agent) DeleteJob(id string) {
	a.do(func(s *agentState) {
		delete(s.jobs, id)
		s.stateTime = time.Now() // Track when state actually changed
	})
	a.scheduleSave()
}

// GetStateTime returns when state was last updated
func (a *Agent) GetStateTime() time.Time {
	return query(a, func(s *agentState) time.Time {
		return s.stateTime
	})
}

// SyncJobs updates local jobs from leader and persists to disk
func (a *Agent) SyncJobs(jobs []*types.Job, updated time.Time) {
	a.do(func(s *agentState) {
		for _, job := range jobs {
			s.jobs[job.ID] = job
		}
		s.stateTime = updated
	})
	a.scheduleSave()
}

// scheduleSave signals that state should be persisted (debounced by monitor loop)
func (a *Agent) scheduleSave() {
	a.needsSave.Store(true)
}

// LoadState loads jobs from state.json on startup
func (a *Agent) LoadState() error {
	path := a.config.Paths.StateFile
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No state file found at %s, starting fresh", path)
			return nil
		}
		return err
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	a.do(func(s *agentState) {
		for _, job := range state.Jobs {
			s.jobs[job.ID] = job // Store by ID (consistent with StoreJob, SyncJobs)
		}
		s.stateTime = state.Updated
	})

	log.Printf("Loaded %d jobs from %s (updated %s)", len(state.Jobs), path, state.Updated.Format(time.RFC3339))
	return nil
}

// SaveState persists jobs to state.json
func (a *Agent) SaveState() {
	// Get state snapshot
	type snapshot struct {
		jobs    []*types.Job
		updated time.Time
	}
	snap := query(a, func(s *agentState) snapshot {
		jobs := make([]*types.Job, 0, len(s.jobs))
		for _, j := range s.jobs {
			jobs = append(jobs, j)
		}
		// Don't update stateTime here - it's only updated when state actually changes
		// (StoreJob, DeleteJob, SyncJobs), not when we persist to disk
		return snapshot{jobs, s.stateTime}
	})

	state := State{
		Jobs:    snap.jobs,
		Updated: snap.updated,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal state: %v", err)
		return
	}

	path := a.config.Paths.StateFile
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("Failed to create state dir: %v", err)
		return
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("Failed to save state: %v", err)
	}
}

// findJobByName is a helper to find job by name within state (jobs stored by ID)
func findJobByName(s *agentState, jobName string) *types.Job {
	for _, job := range s.jobs {
		if job.Name == jobName {
			return job
		}
	}
	return nil
}

// resourceUsage returns total CPU shares and memory used by running/stopping tasks + reservations.
// Stopping tasks count because they still consume resources until fully stopped.
func (s *agentState) resourceUsage() (cpu int, mem uint64) {
	cpu = s.reservedCPU
	mem = s.reservedMem
	for _, task := range s.tasks {
		if task.State == types.TaskRunning || task.State == types.TaskStopping {
			cpu += task.CPUShares
			mem += task.MemoryLimit
		}
	}
	return
}

// hasCapacity checks if the agent has capacity for a new job.
// Accounts for both running tasks AND pending reservations.
func (a *Agent) hasCapacity(job *types.Job) bool {
	return query(a, func(s *agentState) bool {
		usedCPU, usedMem := s.resourceUsage()

		if job.CPUShares > 0 {
			maxCPU := a.sysInfo.CPUCores * 1024
			if usedCPU+job.CPUShares > maxCPU {
				log.Printf("Insufficient CPU: used=%d requested=%d max=%d", usedCPU, job.CPUShares, maxCPU)
				return false
			}
		}

		if job.MemoryLimit > 0 {
			if usedMem+job.MemoryLimit > a.sysInfo.MemoryBytes {
				log.Printf("Insufficient memory: used=%d requested=%d max=%d", usedMem, job.MemoryLimit, a.sysInfo.MemoryBytes)
				return false
			}
		}

		return true
	})
}

// allocatePortsForJob allocates host ports appropriate for the job type.
// For Docker jobs, Ports values are container ports — host ports are always dynamic.
// For process jobs, uses the existing logic (0 = dynamic, >0 = fixed).
func (a *Agent) allocatePortsForJob(job *types.Job) (map[string]int, error) {
	if job.Driver == types.DriverDocker {
		ports := make(map[string]int)
		for name := range job.Ports {
			p, err := getFreePort()
			if err != nil {
				return nil, fmt.Errorf("failed to allocate host port for %s: %w", name, err)
			}
			ports[name] = p
		}
		return ports, nil
	}
	return allocatePorts(job.Ports)
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
