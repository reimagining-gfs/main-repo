package master

import (
    "sync"
    "time"

	"google.golang.org/grpc"
    client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
)

type ChunkInfo struct {
    Size    int64
    Locations map[string]bool  
    Primary   string          
    LeaseExpiration time.Time
    mu sync.RWMutex
}

type FileInfo struct {
    Chunks map[int64]string  
    mu sync.RWMutex
}

type ServerInfo struct {
    LastHeartbeat time.Time
    AvailableSpace int64
    CPUUsage float64
    ActiveOps int32
    Chunks map[string]bool  
    LastUpdated      time.Time
    Status           string 
    FailureCount     int
    mu sync.RWMutex
}

type DeletedFileInfo struct {
    OriginalPath string
    DeleteTime   time.Time
    FileInfo     *FileInfo
}


type Master struct {
    Config *Config
    
    // File namespace
    files map[string]*FileInfo
    filesMu sync.RWMutex

    // Chunk management
    chunks map[string]*ChunkInfo  // chunk_handle -> chunk info
    chunksMu sync.RWMutex

    // Server management
    servers map[string]*ServerInfo  // server_id -> server info
    serversMu sync.RWMutex

    // Chunk server manager
    chunkServerMgr *ChunkServerManager

    deletedFiles    map[string]*DeletedFileInfo  // map[deleted_path]*DeletedFileInfo
    deletedFilesMu  sync.RWMutex
    gcInProgress   bool
    gcMu           sync.Mutex
}

type ChunkServerManager struct {
    master         *Master
    mu             sync.RWMutex
    activeStreams  map[string]chan *chunk_pb.HeartBeatResponse
}

type MasterServer struct {
    client_pb.UnimplementedClientMasterServiceServer
    chunk_pb.UnimplementedChunkMasterServiceServer
    Master *Master
    grpcServer *grpc.Server
}