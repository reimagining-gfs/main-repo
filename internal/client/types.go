package client

import (
    "sync"
    "time"

    client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
    "google.golang.org/grpc"
)

type ClientConfig struct {
    MasterAddr     string        `yaml:"master_address"`
    RetryPolicy    RetryConfig   `yaml:"retry"`
    Cache          CacheConfig   `yaml:"cache"`
    Timeouts       TimeoutConfig `yaml:"timeouts"`
}

type RetryConfig struct {
    MaxAttempts int           `yaml:"max_attempts"`
    Interval    time.Duration `yaml:"interval"`
}

type CacheConfig struct {
    ChunkTTL time.Duration `yaml:"chunk_ttl"`
}

type TimeoutConfig struct {
    Operation time.Duration `yaml:"operation"`
    Connection time.Duration `yaml:"connection"`
}

type Client struct {
    config     *ClientConfig
    conn       *grpc.ClientConn
    client     client_pb.ClientMasterServiceClient

    chunkCache   map[string]*ChunkLocationCache
    chunkCacheMu sync.RWMutex

    activeOps    map[string]*Operation
    activeOpsMu  sync.RWMutex
}

type ChunkLocationCache struct {
    Info      *client_pb.ChunkInfo
    ExpiresAt time.Time
}

type Operation struct {
    Type      OperationType
    StartTime time.Time
    Chunks    []string
    Error     error
    Done      chan struct{}
}

type OperationType int

const (
    OpRead OperationType = iota
    OpWrite
    OpAppend
)

type FileHandle struct {
    client   *Client
    filename string
    readOnly bool
    position int64
    mu       sync.RWMutex
}
