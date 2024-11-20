package master

import (
	"sync"
	"time"

	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
	"google.golang.org/grpc"
)

type ChunkInfo struct {
	Size            int64
	Version         int32
	Locations       map[string]bool
	ServerAddresses map[string]string
	Primary         string
	LeaseExpiration time.Time
	StaleReplicas   map[string]bool
	mu              sync.RWMutex
}

type FileInfo struct {
	Chunks map[int64]string
	mu     sync.RWMutex
}

type ServerInfo struct {
	Address        string
	LastHeartbeat  time.Time
	AvailableSpace int64
	CPUUsage       float64
	ActiveOps      int32
	Chunks         map[string]bool
	LastUpdated    time.Time
	Status         string
	FailureCount   int
	mu             sync.RWMutex
}

type Master struct {
	Config *Config

	// File namespace
	files   map[string]*FileInfo
	filesMu sync.RWMutex

	// Chunk management
	chunks   map[string]*ChunkInfo // chunk_handle -> chunk info
	chunksMu sync.RWMutex

	deletedChunks   map[string]bool
	deletedChunksMu sync.RWMutex

	// Server management
	servers   map[string]*ServerInfo // server_id -> server info
	serversMu sync.RWMutex

	// Chunk server manager
	chunkServerMgr *ChunkServerManager

	gcInProgress bool
	gcMu         sync.Mutex

	pendingOpsMu sync.RWMutex
	pendingOps   map[string][]*PendingOperation // serverId -> pending operations

	opLog *OperationLog
}

type ChunkServerManager struct {
	master        *Master
	mu            sync.RWMutex
	activeStreams map[string]chan *chunk_pb.HeartBeatResponse
}

type MasterServer struct {
	client_pb.UnimplementedClientMasterServiceServer
	chunk_pb.UnimplementedChunkMasterServiceServer
	Master     *Master
	grpcServer *grpc.Server
}

type PendingOperation struct {
	Type         chunk_pb.ChunkCommand_CommandType
	ChunkHandle  string
	Targets      []string
	Source       string
	AttemptCount int
	LastAttempt  time.Time
	CreatedAt    time.Time
}
