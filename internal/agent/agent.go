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
	"sync"
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

const defaultMaxRestarts = 5

// Agent runs jobs and reports status
type Agent struct {
	id       string
	endpoint string
	config   *config.Config
	runner   runner.Runner

	jobs      map[string]*types.Job  // jobID -> job (for restarts)
	tasks     map[string]*types.Task // taskID -> task
	stateTime time.Time              // when state was last updated
	tasksMu   sync.RWMutex

	server *http.Server
}

// New creates a new agent
func New(cfg *config.Config, id string) *Agent {
	runnerCfg := &runner.Config{
		RootfsBase:   cfg.Paths.RootfsBase,
		ArtifactsDir: cfg.Paths.Artifacts,
		MaxCPUShares: cfg.Capacity.CPUShares,
		Chroot:       cfg.Runner.Chroot,
	}

	endpoint := fmt.Sprintf("http://%s:%d", cfg.Node.IP, cfg.Node.Port)

	return &Agent{
		id:       id,
		endpoint: endpoint,
		config:   cfg,
		runner:   runner.NewProcessRunner(runnerCfg),
		jobs:     make(map[string]*types.Job),
		tasks:    make(map[string]*types.Task),
	}
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
	// Clean all old task directories for fresh start
	if pr, ok := a.runner.(*runner.ProcessRunner); ok {
		return pr.CleanupAll()
	}
	return nil
}

// Run starts the agent HTTP server
func (a *Agent) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/tasks", a.handleTasks)
	mux.HandleFunc("/run", a.handleRun)
	mux.HandleFunc("/stop/", a.handleStop)

	addr := fmt.Sprintf("%s:%d", a.config.Node.IP, a.config.Node.Port)
	a.server = &http.Server{
		Addr:    addr,
		Handler: mux,
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
	a.tasksMu.Lock()
	defer a.tasksMu.Unlock()

	for id, task := range a.tasks {
		if task.State == types.TaskRunning {
			log.Printf("Stopping task %s (isolation mode)", id)
			if err := a.runner.Stop(task); err != nil {
				log.Printf("Failed to stop task %s: %v", id, err)
			} else {
				task.State = types.TaskStopped
			}
		}
	}
}

// shutdown gracefully stops all tasks
func (a *Agent) shutdown() {
	log.Println("Agent shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.server.Shutdown(ctx)

	a.tasksMu.Lock()
	defer a.tasksMu.Unlock()

	for _, task := range a.tasks {
		if task.State == types.TaskRunning {
			if err := a.runner.Stop(task); err != nil {
				log.Printf("Failed to stop task %s: %v", task.ID, err)
			}
		}
	}
}

// getJob returns the job for a task
func (a *Agent) getJob(jobID string) *types.Job {
	a.tasksMu.RLock()
	defer a.tasksMu.RUnlock()
	return a.jobs[jobID]
}

// GetJobs returns all jobs this agent knows about
func (a *Agent) GetJobs() []*types.Job {
	a.tasksMu.RLock()
	defer a.tasksMu.RUnlock()

	jobs := make([]*types.Job, 0, len(a.jobs))
	for _, j := range a.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

// GetJob returns a specific job by ID
func (a *Agent) GetJob(id string) *types.Job {
	a.tasksMu.RLock()
	defer a.tasksMu.RUnlock()
	return a.jobs[id]
}

// StoreJob stores a job (used by leader when it learns about remote jobs)
func (a *Agent) StoreJob(job *types.Job) {
	a.tasksMu.Lock()
	defer a.tasksMu.Unlock()
	a.jobs[job.ID] = job
}

// GetStateTime returns when state was last updated
func (a *Agent) GetStateTime() time.Time {
	a.tasksMu.RLock()
	defer a.tasksMu.RUnlock()
	return a.stateTime
}

// SyncJobs updates local jobs from leader and persists to disk
func (a *Agent) SyncJobs(jobs []*types.Job, updated time.Time) {
	a.tasksMu.Lock()
	for _, job := range jobs {
		a.jobs[job.ID] = job
	}
	a.stateTime = updated
	a.tasksMu.Unlock()

	a.SaveState()
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

	a.tasksMu.Lock()
	for _, job := range state.Jobs {
		a.jobs[job.ID] = job
	}
	a.stateTime = state.Updated
	a.tasksMu.Unlock()

	log.Printf("Loaded %d jobs from %s (updated %s)", len(state.Jobs), path, state.Updated.Format(time.RFC3339))
	return nil
}

// SaveState persists jobs to state.json
func (a *Agent) SaveState() {
	a.tasksMu.Lock()
	jobs := make([]*types.Job, 0, len(a.jobs))
	for _, j := range a.jobs {
		jobs = append(jobs, j)
	}
	// Update stateTime when saving
	a.stateTime = time.Now()
	updated := a.stateTime
	a.tasksMu.Unlock()

	state := State{
		Jobs:    jobs,
		Updated: updated,
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
		return
	}
}

// hasCapacity checks if the agent has capacity for a new job
func (a *Agent) hasCapacity(job *types.Job) bool {
	a.tasksMu.RLock()
	defer a.tasksMu.RUnlock()

	// Calculate current resource usage
	usedCPU := 0
	usedMem := uint64(0)

	for _, task := range a.tasks {
		if task.State == types.TaskRunning {
			if j := a.jobs[task.JobID]; j != nil {
				usedCPU += j.CPUShares
				usedMem += j.MemoryLimit
			}
		}
	}

	// Check CPU capacity
	if job.CPUShares > 0 {
		maxCPU := a.config.Capacity.CPUShares
		if maxCPU > 0 && usedCPU+job.CPUShares > maxCPU {
			log.Printf("Insufficient CPU capacity: used=%d, requested=%d, max=%d", usedCPU, job.CPUShares, maxCPU)
			return false
		}
	}

	// Check memory capacity
	if job.MemoryLimit > 0 {
		maxMem := a.config.Capacity.Memory
		if maxMem > 0 && usedMem+job.MemoryLimit > maxMem {
			log.Printf("Insufficient memory capacity: used=%d, requested=%d, max=%d", usedMem, job.MemoryLimit, maxMem)
			return false
		}
	}

	return true
}

func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
