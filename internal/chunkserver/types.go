package chunkserver

import (
    "sync"
    "time"
    "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"

    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
    chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
    chunkserver_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk"

    "google.golang.org/grpc"
)

type ChunkMetadata struct {
    Size         int64
    LastModified time.Time
    Checksum     uint32
}

type ChunkServer struct {
    mu sync.RWMutex

    // Server identification
    serverID string
    address  string

    config *Config

    dataDir     string
    serverDir   string // Complete path including serverID
    chunks      map[string]*ChunkMetadata

    // Operation coordination
    operationQueue *OperationQueue
    leases        map[string]time.Time

    // Master connection
    masterClient  chunk_pb.ChunkMasterServiceClient
    heartbeatStop chan struct{}

    // Server state
    availableSpace int64
    isRunning     bool

    chunkPrimary map[string]bool

    pendingData     map[string]map[string]*PendingData  // operationID -> chunkHandle -> data
    pendingDataLock sync.RWMutex

    grpcServer *grpc.Server

    chunk_ops.UnimplementedChunkOperationServiceServer
    chunkserver_pb.UnimplementedChunkServiceServer
}

type Operation struct {
    Type          OperationType
    ChunkHandle   string
    Offset        int64
    Data          []byte
    Checksum      uint32
    Secondaries   []string
    ResponseChan  chan OperationResult
}

type OperationType int

const (
    OpWrite OperationType = iota
    OpRead
)

type OperationResult struct {
    Status  common.Status
    Data    []byte
    Offset  int64
    Error   error
}

type OperationQueue struct {
    mu       sync.Mutex
    queue    []*Operation
    notEmpty chan struct{}
}

type PendingData struct {
    Data     []byte
    Checksum uint32
    Offset   int64
}