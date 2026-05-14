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

// ClusterConfig holds cluster-wide configuration.
type ClusterConfig struct {
	Name string     `yaml:"name"`
	Lock LockConfig `yaml:"lock"`
}

// LockConfig configures the hoplock backend used for leader election. The
// zero value means "no backend" (standalone mode); an in-memory backend is
// substituted at startup.
type LockConfig struct {
	// Type selects the backend implementation. Supported values:
	//   - "" or "hoplockserver": talk to a hoplockserver over HTTP.
	//   - "s3": talk to an S3-compatible object store via sigv4.
	//   - "mem": in-process memory (standalone / test).
	Type string `yaml:"type"`

	// URL is the hoplockserver base URL (e.g. http://lock:8090). Used
	// when Type is "hoplockserver".
	URL string `yaml:"url"`

	// Key is the lease object key. Defaults to "clusters/<cluster_name>/lease.json".
	Key string `yaml:"key"`

	// APIKey is the X-API-Key header for hoplockserver. Optional.
	APIKey string `yaml:"api_key"`

	// S3 holds the S3-backend configuration when Type is "s3".
	S3 S3LockConfig `yaml:"s3"`
}

// S3LockConfig configures hoplock/s3 for callers who already operate an
// S3-compatible object store and prefer not to run hoplockserver.
type S3LockConfig struct {
	Endpoint        string `yaml:"endpoint"`
	Bucket          string `yaml:"bucket"`
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`
	UsePathStyle    bool   `yaml:"use_path_style"`
}

// CapacityConfig caps how much CPU/memory hop will commit on this node.
// Both fields are optional: 0 means "use auto-detected hardware". Setting them
// to a positive value lower than the hardware tells hop to schedule fewer/
// smaller jobs — useful when the node is shared with non-hop workloads.
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
			Name: "default",
			Lock: LockConfig{}, // empty = standalone
		},
		Capacity: CapacityConfig{
			// 0 = auto-detect; operators override to cap below hardware.
			CPUShares: 0,
			Memory:    0,
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
