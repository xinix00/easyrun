package leader

import (
	"sync"
	"time"

	"easyrun/internal/types"
)

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

func (m *MockJobStore) StoreJob(job *types.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Name] = job
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
		m.jobs[job.Name] = job
	}
	m.stateTime = updated
}
