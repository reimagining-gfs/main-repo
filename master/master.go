package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
    "os"
    "os/signal"
	"sync"
    "syscall"
	"time"

	"google.golang.org/grpc"

	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"

	"gopkg.in/yaml.v3"
)

type Config struct {
    Server struct {
        Host            string `yaml:"host"`
        Port            int    `yaml:"port"`
        MaxConnections  int    `yaml:"max_connections"`
        ConnectionTimeout int  `yaml:"connection_timeout"`
        MaxRequestSize  int64  `yaml:"max_request_size"`
        ThreadPoolSize  int    `yaml:"thread_pool_size"`
    } `yaml:"server"`
    
    Chunk struct {
        Size             int64  `yaml:"size"`
        NamingPattern    string `yaml:"naming_pattern"`
        ChecksumAlgorithm string `yaml:"checksum_algorithm"`
        VerifyOnRead     bool   `yaml:"verify_on_read"`
    } `yaml:"chunk"`
    
    Replication struct {
        Factor      int   `yaml:"factor"`
        Timeout     int   `yaml:"timeout"`
        MinSuccess  int   `yaml:"min_success"`
    } `yaml:"replication"`
    
    Health struct {
        CheckInterval  int `yaml:"check_interval"`
        Timeout       int `yaml:"timeout"`
        MaxFailures   int `yaml:"max_failures"`
        AutoRecovery  bool `yaml:"auto_recovery"`
        RecoveryTimeout int `yaml:"recovery_timeout"`
    } `yaml:"health"`
    
    Metadata struct {
        Database struct {
            Type           string `yaml:"type"`
            Path           string `yaml:"path"`
            BackupInterval int    `yaml:"backup_interval"`
        } `yaml:"database"`
        MaxFilenameLength int `yaml:"max_filename_length"`
        MaxDirectoryDepth int `yaml:"max_directory_depth"`
        CacheSizeMB      int `yaml:"cache_size_mb"`
        CacheTTLSeconds  int `yaml:"cache_ttl_seconds"`
    } `yaml:"metadata"`

    Lease struct {
        LeaseTimeout     int `yaml:"lease_timeout"`      // Duration of leases in seconds
        HeartbeatTimeout int `yaml:"heartbeat_timeout"`  // Timeout for heartbeat signals in seconds
    } `yaml:"lease"`
}

type ChunkInfo struct {
    Size    int64
    Locations map[string]bool  // server_id -> exists
    Primary   string          // server_id of primary
    LeaseExpiration time.Time
    mu sync.RWMutex
}

type FileInfo struct {
    Chunks map[int64]string  // chunk index -> chunk handle
    mu sync.RWMutex
}

type ServerInfo struct {
    LastHeartbeat time.Time
    AvailableSpace int64
    CPUUsage float64
    ActiveOps int32
    Chunks map[string]bool  // chunk_handle -> exists
    mu sync.RWMutex
}

type Master struct {
    config *Config
    
    // File namespace
    files map[string]*FileInfo
    filesMu sync.RWMutex

    // Chunk management
    chunks map[string]*ChunkInfo  // chunk_handle -> chunk info
    chunksMu sync.RWMutex

    // Server management
    servers map[string]*ServerInfo  // server_id -> server info
    serversMu sync.RWMutex

    // For generating unique IDs
    nextChunkHandle int64
    handleMu sync.Mutex
}

type MasterServer struct {
    client_pb.UnimplementedClientMasterServiceServer
    chunk_pb.UnimplementedChunkMasterServiceServer
    master *Master
    grpcServer *grpc.Server
}

func LoadConfig(path string) (*Config, error) {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        return nil, fmt.Errorf("error reading config file: %v", err)
    }
    
    config := &Config{}
    if err := yaml.Unmarshal(data, config); err != nil {
        return nil, fmt.Errorf("error parsing config file: %v", err)
    }
    
    return config, nil
}

func (m *Master) LoadMetadata(path string) error {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        return err
    }

    if len(data) == 0 {
        log.Printf("Metadata file is empty, starting with empty state.")
        return nil
    }

    var metadata struct {
        Files           map[string]*FileInfo
        Chunks          map[string]*ChunkInfo
        NextChunkHandle int64
    }

    if err := json.Unmarshal(data, &metadata); err != nil {
        return err
    }

    m.filesMu.Lock()
    m.chunksMu.Lock()
    defer m.filesMu.Unlock()
    defer m.chunksMu.Unlock()

    m.files = metadata.Files
    m.chunks = metadata.Chunks
    m.nextChunkHandle = metadata.NextChunkHandle

    return nil
}

func (m *Master) SaveMetadata(path string) error {
    m.filesMu.RLock()
    m.chunksMu.RLock()
    defer m.filesMu.RUnlock()
    defer m.chunksMu.RUnlock()

    log.Printf("Check Pointing")

    metadata := struct {
        Files map[string]*FileInfo
        Chunks map[string]*ChunkInfo
        NextChunkHandle int64
    }{
        Files: m.files,
        Chunks: m.chunks,
        NextChunkHandle: m.nextChunkHandle,
    }

    data, err := json.Marshal(metadata)
    if err != nil {
        return err
    }

    return ioutil.WriteFile(path, data, 0644)
}

func (s *MasterServer) Stop() {
    s.grpcServer.GracefulStop()
}

func NewMaster(config *Config) *Master {
    m := &Master{
        config: config,
        files: make(map[string]*FileInfo),
        chunks: make(map[string]*ChunkInfo),
        servers: make(map[string]*ServerInfo),
    }
    
    // bg processes to monitor leases, check server health and maintenance of replication
    
    return m
}

func NewMasterServer(addr string, config *Config) (*MasterServer, error) {
    server := &MasterServer{
        master: NewMaster(config),
        grpcServer: grpc.NewServer(),
    }

    chunk_pb.RegisterChunkMasterServiceServer(server.grpcServer, server)
    client_pb.RegisterClientMasterServiceServer(server.grpcServer, server)

    lis, err := net.Listen("tcp", addr)
    if err != nil {
        return nil, fmt.Errorf("failed to listen: %v", err)
    }

    go func() {
        if err := server.grpcServer.Serve(lis); err != nil {
            log.Fatalf("failed to serve: %v", err)
        }
    }()

    return server, nil
}

func main() {
	config, err := LoadConfig("../configs/general-config.yml")

	if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
    server, err := NewMasterServer(addr, config)
    if err != nil {
        log.Fatalf("Failed to create master server: %v", err)
    }

	if err := server.master.LoadMetadata(config.Metadata.Database.Path); err != nil {
        log.Printf("Error loading metadata: %v", err)
    }

    // [TODO]: When adding operation - log, consider clearing it after every checkpoint.
    // Periodic checkpointing
    go func() {
        ticker := time.NewTicker(time.Duration(config.Metadata.Database.BackupInterval) * time.Second)
        for range ticker.C {
            if err := server.master.SaveMetadata(config.Metadata.Database.Path); err != nil {
                log.Printf("Failed to save metadata: %v", err)
            }
        }
    }()

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    server.Stop()
}