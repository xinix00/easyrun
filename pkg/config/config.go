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
}

// NodeConfig holds node-specific configuration
type NodeConfig struct {
	ID   string `yaml:"id"`
	IP   string `yaml:"ip"`
	Port int    `yaml:"port"`
}

// ClusterConfig holds cluster-wide configuration
type ClusterConfig struct {
	Name         string   `yaml:"name"`
	RaftEndpoints []string `yaml:"raft_endpoints"` // EasyRaft endpoints (e.g., ["http://10.0.0.1:8080", "http://10.0.0.2:8080"])
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
	Artifacts  string `yaml:"artifacts"`
	Cache      string `yaml:"cache"`
}

// RunnerConfig holds runner configuration
type RunnerConfig struct {
	Chroot bool `yaml:"chroot"` // Enable chroot isolation (requires static binaries)
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
			RootfsBase: "./data/rootfs",
			Artifacts:  "./data/artifacts",
			Cache:      "./data/cache",
		},
		Timeouts: TimeoutsConfig{
			HealthCheckInterval: 10 * time.Second,
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
