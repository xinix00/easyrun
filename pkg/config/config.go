package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all configuration for the orchestrator
type Config struct {
	Node     NodeConfig     `yaml:"node"`
	Cluster  ClusterConfig  `yaml:"cluster"`
	Capacity CapacityConfig `yaml:"capacity"`
	Paths    PathsConfig    `yaml:"paths"`
	Runner   RunnerConfig   `yaml:"runner"`
	Timeouts TimeoutsConfig `yaml:"timeouts"`
	APIKey   string         `yaml:"api_key"`
}

// NodeConfig holds node-specific configuration
type NodeConfig struct {
	ID         string            `yaml:"id"`
	IP         string            `yaml:"ip"`
	Port       int               `yaml:"port"`
	Attributes map[string]string `yaml:"attributes"` // user-defined node attributes (merged with auto-detected)
}

// ClusterConfig holds cluster-wide configuration
type ClusterConfig struct {
	Name         string   `yaml:"name"`
	RaftEndpoints []string `yaml:"raft_endpoints"` // HopRaft endpoints (e.g., ["http://10.0.0.1:8080", "http://10.0.0.2:8080"])
}

// CapacityConfig holds resource capacity configuration
type CapacityConfig struct {
	CPUShares int    `yaml:"cpu_shares"`
	Memory    uint64 `yaml:"memory"`
}

// PathsConfig holds filesystem paths configuration
type PathsConfig struct {
	StateFile  string `yaml:"state_file"`
	RootfsBase string `yaml:"rootfs_base"`
}

// RunnerConfig holds runner configuration
type RunnerConfig struct {
	Isolate      bool   `yaml:"isolate"`       // Enable process isolation (chroot on Linux, sandbox on macOS). Default: true
	DockerSocket string `yaml:"docker_socket"` // Docker daemon socket path. Default: /tmp/hop/docker.sock
}

// TimeoutsConfig holds timeout configuration
type TimeoutsConfig struct {
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	HealthCheckTimeout  time.Duration `yaml:"health_check_timeout"`
	NodeDeadThreshold   time.Duration `yaml:"node_dead_threshold"`
	LeaderLease         time.Duration `yaml:"leader_lease"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:   "",
			IP:   "",
			Port: 8080,
		},
		Cluster: ClusterConfig{
			Name:          "default",
			RaftEndpoints: nil, // Empty = standalone mode
		},
		Capacity: CapacityConfig{
			CPUShares: 14000,
			Memory:    8 * 1024 * 1024 * 1024, // 8GB
		},
		Paths: PathsConfig{
			StateFile:  "./data/state.json",
			RootfsBase: "/tmp/hop",
		},
		Runner: RunnerConfig{
			Isolate:      true,
			DockerSocket: "/var/run/docker.sock",
		},
		Timeouts: TimeoutsConfig{
			HealthCheckInterval: 5 * time.Second,
			HealthCheckTimeout:  5 * time.Second,
			NodeDeadThreshold:   30 * time.Second,
			LeaderLease:         30 * time.Second,
		},
	}
}

// Load loads configuration from a YAML file
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
