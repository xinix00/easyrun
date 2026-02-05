package leader

import (
	"sync"
	"time"

	"easyrun/internal/types"
)

func init() {
	// Fast timeouts for tests - no waiting for fake agents
	HTTPClientTimeout = 10 * time.Millisecond
	VerifyInterval = 10 * time.Millisecond
}

// MockJobStore implements JobStore for testing
type MockJobStore struct {
	mu        sync.Mutex
	jobs      map[string]*types.Job
	stateTime time.Time
}

func NewMockJobStore() *MockJobStore {
	return &MockJobStore{
		jobs: make(map[string]*types.Job),
	}
}

func (m *MockJobStore) GetJobs() []*types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	jobs := make([]*types.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (m *MockJobStore) GetJob(id string) *types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[id]
}

// GetJobByName finds a job by name (for test convenience - jobs are stored by ID)
func (m *MockJobStore) GetJobByName(name string) *types.Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, job := range m.jobs {
		if job.Name == name {
			return job
		}
	}
	return nil
}

func (m *MockJobStore) StoreJob(job *types.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
}

func (m *MockJobStore) DeleteJob(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
}

func (m *MockJobStore) GetStateTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stateTime
}

func (m *MockJobStore) SyncJobs(jobs []*types.Job, updated time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, job := range jobs {
		m.jobs[job.ID] = job
	}
	m.stateTime = updated
}
