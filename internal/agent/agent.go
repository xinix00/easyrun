package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"easyrun/internal/runner"
	"easyrun/internal/types"
	"easyrun/pkg/config"
	"easyrun/pkg/httputil"
)

// State represents persisted state
type State struct {
	Jobs    []*types.Job `json:"jobs"`
	Updated time.Time    `json:"updated"`
}

const (
	defaultMaxRestarts     = 5
	defaultRestartWindow   = 5 * time.Minute
	shutdownTimeout        = 5 * time.Second
	proxyTimeout           = 10 * time.Second
	stateChannelBufferSize = 256
)

// agentState holds all mutable state (owned by single goroutine)
type agentState struct {
	jobs      map[string]*types.Job  // job name → job
	tasks     map[string]*types.Task // task ID → task
	stateTime time.Time
}

// Agent runs jobs and reports status
type Agent struct {
	id           string
	endpoint     string
	config       *config.Config
	execRunner   runner.Runner
	dockerRunner runner.Runner
	sysInfo      SystemInfo        // detected once at startup
	attributes   map[string]string // node attributes for affinity matching

	ops chan func(*agentState) // all state access goes through here

	server     *http.Server
	getLeader  func() string // returns current leader address (for proxying)
	httpClient *http.Client
	apiKey     string // API key for authenticating with leader and protecting local endpoints

	needsSave   atomic.Bool              // flag for debounced persistence
	checkStates map[string]*checkState   // health check state per task (monitor goroutine only)
}

// New creates a new agent with optional runner (nil uses default ExecRunner)
func New(cfg *config.Config, id string, r runner.Runner) *Agent {
	endpoint := fmt.Sprintf("http://%s:%d", cfg.Node.IP, cfg.Node.Port)

	// Build node attributes: auto-detected + user-configured (config overrides)
	hasDocker := "false"
	if _, err := exec.LookPath("docker"); err == nil {
		hasDocker = "true"
	}

	attrs := map[string]string{
		"node.id":     id,
		"node.arch":   runtime.GOARCH,
		"node.os":     runtime.GOOS,
		"node.docker": hasDocker,
	}
	for k, v := range cfg.Node.Attributes {
		attrs[k] = v
	}

	if r == nil {
		r = runner.NewExecRunner(&runner.Config{
			RootfsBase:   cfg.Paths.RootfsBase,
			ArtifactsDir: cfg.Paths.Artifacts,
			MaxCPUShares: cfg.Capacity.CPUShares,
			Isolate:      cfg.Runner.Isolate,
			NodeAttrs:    attrs,
		})
	}

	return &Agent{
		id:           id,
		endpoint:     endpoint,
		config:       cfg,
		execRunner:   r,
		dockerRunner: runner.NewDockerRunner(attrs),
		sysInfo:      GetSystemInfo(), // detect once at startup
		attributes:   attrs,
		ops:          make(chan func(*agentState), stateChannelBufferSize),
		httpClient:   &http.Client{Timeout: proxyTimeout},
		apiKey:       cfg.APIKey,
		checkStates:  make(map[string]*checkState),
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

// monitorInterval returns the task monitor interval from config (default 5s).
func (a *Agent) monitorInterval() time.Duration {
	if d := a.config.Timeouts.HealthCheckInterval; d > 0 {
		return d
	}
	return 5 * time.Second
}

// healthTimeout returns the health check timeout from config (default 5s).
func (a *Agent) healthTimeout() time.Duration {
	if d := a.config.Timeouts.HealthCheckTimeout; d > 0 {
		return d
	}
	return 5 * time.Second
}

// ID returns the agent ID
func (a *Agent) ID() string {
	return a.id
}

// Endpoint returns the agent's HTTP endpoint
func (a *Agent) Endpoint() string {
	return a.endpoint
}

// Attributes returns the agent's node attributes
func (a *Agent) Attributes() map[string]string {
	return a.attributes
}

// matchesAffinity checks if this agent's attributes satisfy all job affinity constraints.
func (a *Agent) matchesAffinity(affinity map[string]string) bool {
	for k, v := range affinity {
		if a.attributes[k] != v {
			return false
		}
	}
	return true
}

// resolveArtifact picks the first artifact whose Match constraints are satisfied
// by this agent's attributes. Empty Match = catch-all (always matches).
// Returns nil if no artifact matches.
func (a *Agent) resolveArtifact(artifacts []types.Artifact) *types.Artifact {
	for i := range artifacts {
		if a.matchesAffinity(artifacts[i].Match) {
			return &artifacts[i]
		}
	}
	return nil
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

	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return httputil.RequireAPIKey(a.apiKey, h)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/capacity", auth(a.handleCapacity))
	mux.HandleFunc("/tasks", auth(a.handleTasks))
	mux.HandleFunc("/run", auth(a.handleRun))
	mux.HandleFunc("/delete/", auth(a.handleDelete))
	mux.HandleFunc("/stop/", auth(a.handleStop))
	mux.HandleFunc("/stop-task/", auth(a.handleStopTask))
	mux.HandleFunc("/logs/", auth(a.handleLogs))
	mux.HandleFunc("/leader", a.handleLeader)

	// Proxy endpoints - forward to leader for cluster-wide operations
	mux.HandleFunc("/v1/agents", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/jobs", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/jobs/", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/status", auth(a.proxyToLeader))
	mux.HandleFunc("/v1/events", auth(a.proxySSEToLeader))

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

// stopTasks stops tasks in parallel, removes from state, and blocks until done.
func (a *Agent) stopTasks(tasks []*types.Task) {
	if len(tasks) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(t *types.Task) {
			defer wg.Done()
			if err := a.runnerFor(t.Driver).Stop(t); err != nil {
				log.Printf("Failed to stop task %s: %v", t.ID, err)
			}
			a.do(func(s *agentState) {
				delete(s.tasks, t.ID)
			})
		}(task)
	}
	wg.Wait()
	a.scheduleSave()
}

// markRunningAsStopping returns all running tasks after marking them as stopping.
func markRunningAsStopping(s *agentState) []*types.Task {
	var tasks []*types.Task
	for _, task := range s.tasks {
		if task.State == types.TaskRunning {
			task.State = types.TaskStopping
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// StopAllTasks stops all running tasks and removes them from state (used when agent is isolated)
func (a *Agent) StopAllTasks() {
	tasks := query(a, func(s *agentState) []*types.Task { return markRunningAsStopping(s) })
	a.stopTasks(tasks)
	if len(tasks) > 0 {
		log.Printf("Isolation mode: stopped and removed %d tasks", len(tasks))
	}
}

// shutdown gracefully stops all tasks
func (a *Agent) shutdown() {
	log.Println("Agent shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = a.server.Shutdown(ctx)

	tasks := query(a, func(s *agentState) []*types.Task { return markRunningAsStopping(s) })
	a.stopTasks(tasks)
}

// corsMiddleware adds CORS headers for browser access
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")

		// Handle preflight
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// GetJob returns a specific job by name
func (a *Agent) GetJob(name string) *types.Job {
	return query(a, func(s *agentState) *types.Job {
		return s.jobs[name]
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

// GetPlacedTaskCounts returns a map of jobName -> number of placed tasks on this agent.
// Counts ALL tasks (including failed) because failed tasks exhausted their restart
// counter and should NOT be re-dispatched by the leader.
func (a *Agent) GetPlacedTaskCounts() map[string]int {
	return query(a, func(s *agentState) map[string]int {
		counts := make(map[string]int)
		for _, task := range s.tasks {
			if task.JobName != "" {
				counts[task.JobName]++
			}
		}
		return counts
	})
}

// StoreJob stores a job (used by leader when it learns about remote jobs)
func (a *Agent) StoreJob(job *types.Job) {
	a.do(func(s *agentState) {
		s.jobs[job.Name] = job
		s.stateTime = time.Now()
	})
}

// DeleteJob removes a job from the store by name (for JobStore interface)
func (a *Agent) DeleteJob(name string) {
	a.do(func(s *agentState) {
		delete(s.jobs, name)
		s.stateTime = time.Now()
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
	type snapshot struct {
		jobs    []*types.Job
		updated time.Time
	}
	snap := query(a, func(s *agentState) snapshot {
		jobs := make([]*types.Job, 0, len(s.jobs))
		for _, j := range s.jobs {
			jobs = append(jobs, j)
		}
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

// resourceUsage returns total CPU shares and memory used by running/stopping tasks.
// Stopping tasks count because they still consume resources until fully stopped.
func (s *agentState) resourceUsage() (cpu int, mem uint64) {
	for _, task := range s.tasks {
		if task.State == types.TaskRunning || task.State == types.TaskStopping {
			cpu += task.CPUShares
			mem += task.MemoryLimit
		}
	}
	return
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
