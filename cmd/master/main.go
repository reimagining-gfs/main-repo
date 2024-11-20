package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
)

func main() {
	// Load server configuration
	config, err := master.LoadConfig("../../configs/general-config.yml")
	if err != nil {
		log.Fatalf("Failed to load configuration file: %v", err)
	}

	// Validate server address
	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	if config.Server.Host == "" || config.Server.Port <= 0 || config.Server.Port > 65535 {
		log.Fatalf("Invalid server configuration: Host=%s, Port=%d", config.Server.Host, config.Server.Port)
	}
	log.Printf("Server configuration validated. Starting master server at: %v", addr)

	// Initialize the master server
	server, err := master.NewMasterServer(addr, config)
	if err != nil {
		log.Fatalf("Failed to create master server: %v", err)
	}

	// Load metadata
	if err := server.Master.LoadMetadata(config.Metadata.Database.Path); err != nil {
		log.Printf("Warning: Failed to load metadata from %s: %v", config.Metadata.Database.Path, err)
	} else {
		log.Printf("Metadata loaded successfully from %s", config.Metadata.Database.Path)
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("Master server is running. Press Ctrl+C to stop.")

	// Wait for termination signal
	sig := <-sigChan
	log.Printf("Received termination signal: %v. Shutting down the server...", sig)

	// Stop the server gracefully
	server.Stop()
	log.Println("Server stopped successfully. Exiting.")
}
