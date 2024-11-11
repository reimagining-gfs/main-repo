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
    config, err := master.LoadConfig("../../configs/general-config.yml")
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    addr := fmt.Sprintf("%s:%d", "localhost", 50051)
    server, err := master.NewMasterServer(addr, config)
    if err != nil {
        log.Fatalf("Failed to create master server: %v", err)
    }

    if err := server.Master.LoadMetadata(config.Metadata.Database.Path); err != nil {
        log.Printf("Error loading metadata: %v", err)
    }

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    server.Stop()
}