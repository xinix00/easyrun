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
}

// Agent runs jobs and reports status
type Agent struct {
	id       string
	endpoint string
	config   *config.Config
	runner   runner.Runner
	sysInfo  SystemInfo // detected once at startup

	ops chan func(*agentState) // all state access goes through here

	server     *http.Server
	getLeader  func() string // returns current leader address (for proxying)
	httpClient *http.Client

	needsSave atomic.Bool // flag for debounced persistence
}

// New creates a new agent with optional runner (nil uses default ProcessRunner)
func New(cfg *config.Config, id string, r runner.Runner) *Agent {
	if r == nil {
		runnerCfg := &runner.Config{
			RootfsBase:   cfg.Paths.RootfsBase,
			ArtifactsDir: cfg.Paths.Artifacts,
			MaxCPUShares: cfg.Capacity.CPUShares,
			Isolate:      cfg.Runner.Isolate,
		}
		r = runner.NewProcessRunner(runnerCfg)
	}

	endpoint := fmt.Sprintf("http://%s:%d", cfg.Node.IP, cfg.Node.Port)

	return &Agent{
		id:         id,
		endpoint:   endpoint,
		config:     cfg,
		runner:     r,
		sysInfo:    GetSystemInfo(), // detect once at startup
		ops:        make(chan func(*agentState), stateChannelBufferSize),
		httpClient: &http.Client{Timeout: proxyTimeout},
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

// Init performs startup cleanup (removes old task directories)
func (a *Agent) Init() error {
	return a.runner.Cleanup()
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

// StopAllTasks stops all running tasks (used when agent is isolated)
func (a *Agent) StopAllTasks() {
	// Get tasks to stop
	tasks := query(a, func(s *agentState) []*types.Task {
		var running []*types.Task
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				running = append(running, task)
			}
		}
		return running
	})

	// Stop them outside of state loop (runner.Stop can block)
	for _, task := range tasks {
		log.Printf("Stopping task %s (isolation mode)", task.ID)
		if err := a.runner.Stop(task); err != nil {
			log.Printf("Failed to stop task %s: %v", task.ID, err)
		} else {
			a.do(func(s *agentState) {
				if t := s.tasks[task.ID]; t != nil {
					t.State = types.TaskStopped
				}
			})
		}
	}
}

// shutdown gracefully stops all tasks
func (a *Agent) shutdown() {
	log.Println("Agent shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	a.server.Shutdown(ctx)

	// Get running tasks
	tasks := query(a, func(s *agentState) []*types.Task {
		var running []*types.Task
		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				running = append(running, task)
			}
		}
		return running
	})

	// Stop them
	for _, task := range tasks {
		if err := a.runner.Stop(task); err != nil {
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

// getJob returns the job for a task
func (a *Agent) getJob(jobID string) *types.Job {
	return query(a, func(s *agentState) *types.Job {
		return s.jobs[jobID]
	})
}

// GetJobs returns all jobs this agent knows about
func (a *Agent) GetJobs() []*types.Job {
	return query(a, func(s *agentState) []*types.Job {
		jobs := make([]*types.Job, 0, len(s.jobs))
		for _, j := range s.jobs {
			jobs = append(jobs, j)
		}
		return jobs
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
	})
}

// DeleteJob removes a job from the store by ID (for JobStore interface)
func (a *Agent) DeleteJob(id string) {
	a.do(func(s *agentState) {
		delete(s.jobs, id)
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
			s.jobs[job.Name] = job
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
			s.jobs[job.Name] = job
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
		s.stateTime = time.Now()
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

// hasCapacity checks if the agent has capacity for a new job
func (a *Agent) hasCapacity(job *types.Job) bool {
	return query(a, func(s *agentState) bool {
		usedCPU := 0
		usedMem := uint64(0)

		for _, task := range s.tasks {
			if task.State == types.TaskRunning {
				if j := s.jobs[task.JobName]; j != nil {
					usedCPU += j.CPUShares
					usedMem += j.MemoryLimit
				}
			}
		}

		if job.CPUShares > 0 {
			maxCPU := a.sysInfo.CPUCores * 1024 // 1024 shares per core
			if usedCPU+job.CPUShares > maxCPU {
				log.Printf("Insufficient CPU capacity: used=%d, requested=%d, max=%d", usedCPU, job.CPUShares, maxCPU)
				return false
			}
		}

		if job.MemoryLimit > 0 {
			maxMem := a.sysInfo.MemoryBytes
			if usedMem+job.MemoryLimit > maxMem {
				log.Printf("Insufficient memory capacity: used=%d, requested=%d, max=%d", usedMem, job.MemoryLimit, maxMem)
				return false
			}
		}

		return true
	})
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
