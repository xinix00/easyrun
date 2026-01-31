package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"easyraft/internal/api"
	"easyraft/internal/config"
	"easyraft/internal/lease"
	"easyraft/internal/raft"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if cfg.Self == "" {
		log.Fatal("self is required in config")
	}
	if len(cfg.Peers) == 0 {
		log.Fatal("peers is required in config")
	}
	if cfg.APIKey == "" {
		log.Fatal("api_key is required in config")
	}

	log.Printf("Starting easyraft node %s", cfg.Self)
	log.Printf("Peers: %v", cfg.Peers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
	}()

	// Create components
	r := raft.New(cfg)
	l := lease.NewManager()
	s := api.New(cfg, r, l)

	// Start raft election loop
	go r.Run(ctx)

	// Start HTTP server (blocks)
	if err := s.Run(ctx); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
