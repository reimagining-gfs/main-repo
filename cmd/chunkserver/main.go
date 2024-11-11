package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/google/uuid"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/chunkserver"
)

func main() {
	port := flag.Int("port", 8080, "Port number to run the chunk server on")
	flag.Parse()

	serverID := uuid.NewString()

	address := fmt.Sprintf("localhost:%d", *port)

	config, err := chunkserver.LoadConfig("../../configs/chunkserver-config.yml")

	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Port %d is not available: %v\n", *port, err)
		return
	}
	listener.Close()

	cs, err := chunkserver.NewChunkServer(serverID, address, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create chunk server: %v\n", err)
		return
	}

	fmt.Printf("Starting chunk server with ID: %s\n", serverID)
	if err := cs.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start chunk server: %v\n", err)
		return
	}

	select {}
}