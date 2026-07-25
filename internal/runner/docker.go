package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"hop/internal/types"
)

const (
	containerPrefix      = "hop-"
	dockerStopTimeout    = 10 // seconds
	dockerCommandTimeout = 30 * time.Second
	defaultDockerSocket  = "/var/run/docker.sock"
)

// DockerRunner runs jobs as Docker containers via the Docker API directly.
type DockerRunner struct {
	nodeAttrs  map[string]string
	client     *http.Client
	stdoutLog  map[string]*LogBroadcaster
	stderrLog  map[string]*LogBroadcaster
	logCancel  map[string]context.CancelFunc
	mu         sync.RWMutex
}

// NewDockerRunner creates a new Docker runner
func NewDockerRunner(nodeAttrs map[string]string, socketPath string) *DockerRunner {
	if socketPath == "" {
		socketPath = defaultDockerSocket
	}
	return &DockerRunner{
		nodeAttrs: nodeAttrs,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		},
		stdoutLog: make(map[string]*LogBroadcaster),
		stderrLog: make(map[string]*LogBroadcaster),
		logCancel: make(map[string]context.CancelFunc),
	}
}

// dockerAPI performs a Docker API request
func (r *DockerRunner) dockerAPI(method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = strings.NewReader(string(data))
	}
	req, err := http.NewRequest(method, "http://docker"+path, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return r.client.Do(req)
}

// dockerAPIWithContext performs a Docker API request with context
func (r *DockerRunner) dockerAPIWithContext(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return r.client.Do(req)
}

// Run starts a Docker container for the job.
func (r *DockerRunner) Run(job *types.Job, task *types.Task) error {
	if job.Image == "" {
		return fmt.Errorf("image is required for docker runner")
	}

	containerName := containerPrefix + task.ID

	// Pull image first
	resp, err := r.dockerAPI("POST", fmt.Sprintf("/images/create?fromImage=%s", job.Image), nil)
	if err != nil {
		return fmt.Errorf("docker pull failed: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Build port bindings and exposed ports
	portBindings := map[string][]portBinding{}
	exposedPorts := map[string]struct{}{}
	for _, hostPort := range task.Ports {
		containerPort := hostPort // container port = host port (same convention)
		key := fmt.Sprintf("%d/tcp", containerPort)
		exposedPorts[key] = struct{}{}
		portBindings[key] = []portBinding{{HostPort: fmt.Sprintf("%d", hostPort)}}
	}

	// Build env
	var env []string
	for k, v := range job.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, PortEnvVars(task.Ports)...)
	env = append(env, AttrEnvVars(r.nodeAttrs)...)

	// Build volumes
	var binds []string
	for hostPath, containerPath := range job.Volumes {
		binds = append(binds, hostPath+":"+containerPath)
	}

	// Build command
	var cmd []string
	if job.Command != "" {
		cmd = []string{"/bin/sh", "-c", job.Command}
	}

	// Create container
	createBody := createRequest{
		Image:        job.Image,
		Env:          env,
		Cmd:          cmd,
		ExposedPorts: exposedPorts,
		HostConfig: hostConfig{
			PortBindings: portBindings,
			Binds:        binds,
			Memory:       int64(job.MemoryLimit),
			CPUShares:    int64(job.CPUShares),
		},
	}

	resp, err = r.dockerAPI("POST", "/containers/create?name="+containerName, createBody)
	if err != nil {
		return fmt.Errorf("docker create failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("docker create failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// Start container
	resp, err = r.dockerAPI("POST", "/containers/"+containerName+"/start", nil)
	if err != nil {
		return fmt.Errorf("docker start failed: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker start failed (%d)", resp.StatusCode)
	}

	// Start log streaming
	r.startLogStreaming(task.ID, containerName)

	return nil
}

// Stop stops and removes a Docker container
func (r *DockerRunner) Stop(task *types.Task) error {
	containerName := containerPrefix + task.ID
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()

	// Stop container
	resp, err := r.dockerAPIWithContext(ctx, "POST", fmt.Sprintf("/containers/%s/stop?t=%d", containerName, dockerStopTimeout), nil)
	if err != nil {
		log.Printf("docker stop %s: %v", containerName, err)
	} else {
		resp.Body.Close()
	}

	// Remove container
	resp, err = r.dockerAPIWithContext(ctx, "DELETE", "/containers/"+containerName+"?force=true", nil)
	if err != nil {
		log.Printf("docker rm %s: %v", containerName, err)
	} else {
		resp.Body.Close()
	}

	// Stop log streaming
	r.mu.Lock()
	if cancel := r.logCancel[task.ID]; cancel != nil {
		cancel()
	}
	delete(r.logCancel, task.ID)
	if b := r.stdoutLog[task.ID]; b != nil {
		b.Close()
	}
	if b := r.stderrLog[task.ID]; b != nil {
		b.Close()
	}
	delete(r.stdoutLog, task.ID)
	delete(r.stderrLog, task.ID)
	r.mu.Unlock()

	return nil
}

// Status checks if a Docker container is running
func (r *DockerRunner) Status(task *types.Task) (types.TaskState, error) {
	containerName := containerPrefix + task.ID

	resp, err := r.dockerAPI("GET", "/containers/"+containerName+"/json", nil)
	if err != nil {
		return types.TaskFailed, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return types.TaskFailed, nil
	}

	var info struct {
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return types.TaskFailed, nil
	}

	if info.State.Running {
		return types.TaskRunning, nil
	}
	return types.TaskFailed, nil
}

// GetStdout returns the stdout log broadcaster for a task
func (r *DockerRunner) GetStdout(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stdoutLog[taskID]
}

// GetStderr returns the stderr log broadcaster for a task
func (r *DockerRunner) GetStderr(taskID string) *LogBroadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stderrLog[taskID]
}

// Cleanup removes all hop containers
func (r *DockerRunner) Cleanup() error {
	resp, err := r.dockerAPI("GET", "/containers/json?all=true&filters="+`{"name":["hop-"]}`, nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var containers []struct {
		ID    string   `json:"Id"`
		Names []string `json:"Names"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return nil
	}

	for _, c := range containers {
		resp, err := r.dockerAPI("DELETE", "/containers/"+c.ID+"?force=true", nil)
		if err == nil {
			resp.Body.Close()
		}
	}
	return nil
}

// startLogStreaming streams container logs via Docker API
func (r *DockerRunner) startLogStreaming(taskID, containerName string) {
	stdoutB := NewLogBroadcaster()
	stderrB := NewLogBroadcaster()

	ctx, cancel := context.WithCancel(context.Background())

	r.mu.Lock()
	r.stdoutLog[taskID] = stdoutB
	r.stderrLog[taskID] = stderrB
	r.logCancel[taskID] = cancel
	r.mu.Unlock()

	go func() {
		resp, err := r.dockerAPIWithContext(ctx, "GET", "/containers/"+containerName+"/logs?follow=true&stdout=true&stderr=true", nil)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		// Docker multiplexed stream: 8-byte header per frame
		// [stream_type(1)][0][0][0][size(4)] then payload
		reader := bufio.NewReader(resp.Body)
		header := make([]byte, 8)
		for {
			if _, err := io.ReadFull(reader, header); err != nil {
				return
			}
			size := int(header[4])<<24 | int(header[5])<<16 | int(header[6])<<8 | int(header[7])
			payload := make([]byte, size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}
			if header[0] == 1 {
				_, _ = stdoutB.Write(payload)
			} else {
				_, _ = stderrB.Write(payload)
			}
		}
	}()
}

// Docker API types (minimal, only what we need)

type portBinding struct {
	HostPort string `json:"HostPort"`
}

type hostConfig struct {
	PortBindings map[string][]portBinding `json:"PortBindings,omitempty"`
	Binds        []string                 `json:"Binds,omitempty"`
	Memory       int64                    `json:"Memory,omitempty"`
	CPUShares    int64                    `json:"CpuShares,omitempty"`
}

type createRequest struct {
	Image        string                `json:"Image"`
	Env          []string              `json:"Env,omitempty"`
	Cmd          []string              `json:"Cmd,omitempty"`
	ExposedPorts map[string]struct{}   `json:"ExposedPorts,omitempty"`
	HostConfig   hostConfig            `json:"HostConfig"`
}
