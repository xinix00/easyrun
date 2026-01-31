package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds easyraft configuration
type Config struct {
	// This node's URL (how others reach us)
	Self string `yaml:"self"`

	// All peer URLs (including self)
	Peers []string `yaml:"peers"`

	// API key for internal raft communication
	APIKey string `yaml:"api_key"`

	// HTTP port to listen on
	Port int `yaml:"port"`

	// Timing
	HeartbeatInterval int `yaml:"heartbeat_interval_ms"` // default 3000
	ElectionTimeout   int `yaml:"election_timeout_ms"`   // default 10000
}

// Load loads config from a YAML file
func Load(path string) (*Config, error) {
	cfg := &Config{
		Port:              8080,
		HeartbeatInterval: 3000,
		ElectionTimeout:   10000,
	}

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
