package master

import (
    "context"
    "errors"
	"fmt"
	"log"
    "net"
    "sync"
	"time"

    "github.com/google/uuid"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
    common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
)

func (s *MasterServer) Stop() {
    s.grpcServer.GracefulStop()
}

func NewChunkServerManager(master *Master) *ChunkServerManager {
    return &ChunkServerManager{
        master:        master,
        activeStreams: make(map[string]chan *chunk_pb.HeartBeatResponse),
    }
}

func NewMaster(config *Config) *Master {
    m := &Master{
        Config: config,
        deletedFiles: make(map[string]*DeletedFileInfo),
        files: make(map[string]*FileInfo),
        chunks: make(map[string]*ChunkInfo),
        servers: make(map[string]*ServerInfo),
    }
    
    // bg processes to monitor leases, check server health and maintenance of replication
    go m.monitorServerHealth()
    go m.monitorChunkReplication()
    go m.cleanupExpiredLeases()
    go m.runGarbageCollection()

    return m
}

func NewMasterServer(addr string, config *Config) (*MasterServer, error) {
    server := &MasterServer{
        Master: NewMaster(config),
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

func (s *MasterServer) ReportChunk(ctx context.Context, req *chunk_pb.ReportChunkRequest) (*chunk_pb.ReportChunkResponse, error) {
    if req.ServerId == "" {
        return nil, status.Error(codes.InvalidArgument, "server_id is required")
    }

    s.Master.serversMu.Lock()
    if _, exists := s.Master.servers[req.ServerId]; !exists {
        s.Master.servers[req.ServerId] = &ServerInfo{
            LastHeartbeat: time.Now(),
            Chunks:        make(map[string]bool),
            Status:        "ACTIVE",
        }
    }
    serverInfo := s.Master.servers[req.ServerId]
    s.Master.serversMu.Unlock()

    s.Master.chunksMu.Lock()
    defer s.Master.chunksMu.Unlock()

    for _, chunk := range req.Chunks {
        chunkHandle := chunk.Handle
        if _, exists := s.Master.chunks[chunkHandle]; !exists {
            s.Master.chunks[chunkHandle] = &ChunkInfo{
                Locations: make(map[string]bool),
            }
        }
        
        s.Master.chunks[chunkHandle].mu.Lock()
        s.Master.chunks[chunkHandle].Locations[req.ServerId] = true
        s.Master.chunks[chunkHandle].mu.Unlock()

        serverInfo.mu.Lock()
        serverInfo.Chunks[chunkHandle] = true
        serverInfo.mu.Unlock()
    }

    return &chunk_pb.ReportChunkResponse{
        Status: &common_pb.Status{Code: common_pb.Status_OK},
    }, nil
}

// Lease can only be requested by an active primary chunk-server
func (s *MasterServer) RequestLease(ctx context.Context, req *chunk_pb.RequestLeaseRequest) (*chunk_pb.RequestLeaseResponse, error) {
    if req.ChunkHandle.Handle == "" {
        return nil, status.Error(codes.InvalidArgument, "chunk_handle is required")
    }

    s.Master.chunksMu.RLock()
    chunkInfo, exists := s.Master.chunks[req.ChunkHandle.Handle]
    s.Master.chunksMu.RUnlock()

    if !exists {
        return &chunk_pb.RequestLeaseResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: "chunk not found",
            },
            Granted: false,
        }, nil
    }

    chunkInfo.mu.Lock()
    defer chunkInfo.mu.Unlock()

    // Check if the requesting server is the current primary
    if chunkInfo.Primary != req.ServerId {
        return &chunk_pb.RequestLeaseResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: "only the primary server can request lease extension",
            },
            Granted: false,
        }, nil
    }

    // Extend the lease
    chunkInfo.LeaseExpiration = time.Now().Add(time.Duration(s.Master.Config.Lease.LeaseTimeout) * time.Second)

    return &chunk_pb.RequestLeaseResponse{
        Status:          &common_pb.Status{Code: common_pb.Status_OK},
        Granted:         true,
        LeaseExpiration: chunkInfo.LeaseExpiration.Unix(),
    }, nil
}

func (s *MasterServer) HeartBeat(stream chunk_pb.ChunkMasterService_HeartBeatServer) error {
    var serverId string
    responseChannel := make(chan *chunk_pb.HeartBeatResponse, 10)
    defer close(responseChannel)

    // Start goroutine to send responses
    go func() {
        for response := range responseChannel {
            if err := stream.Send(response); err != nil {
                log.Printf("Error sending heartbeat response to %s: %v", serverId, err)
                return
            }
        }
    }()

    for {
        req, err := stream.Recv()
        if err != nil {
            s.Master.handleServerFailure(serverId)
            return err
        }

        if serverId == "" {
            serverId = req.ServerId
            s.Master.serversMu.Lock()
            s.Master.servers[serverId] = &ServerInfo{
                LastHeartbeat:   time.Now(),
                AvailableSpace:  req.AvailableSpace,
                CPUUsage:        req.CpuUsage,
                ActiveOps:       req.ActiveOperations,
                Chunks:          make(map[string]bool),
                Status:          "ACTIVE",
                LastUpdated:     time.Now(),
            }
            s.Master.serversMu.Unlock()
        }

        if err := s.updateServerStatus(serverId, req); err != nil {
            log.Printf("Error updating server status: %v", err)
            continue
        }

        commands := s.generateChunkCommands(serverId)
        
        response := &chunk_pb.HeartBeatResponse{
            Status:   &common_pb.Status{Code: common_pb.Status_OK},
            Commands: commands,
        }

        select {
        case responseChannel <- response:
        default:
            log.Printf("Warning: Response channel full for server %s", serverId)
        }
    }
}

// File-related errors
// [TODO]: Move them to error-config.yml
var (
    ErrFileNotFound = errors.New("file not found")
    ErrFileExists = errors.New("file already exists")
    ErrInvalidFileName = errors.New("invalid filename")
    ErrInvalidChunkRange = errors.New("invalid chunk range")
)

func (s *MasterServer) GetFileChunksInfo(ctx context.Context, req *client_pb.GetFileChunksInfoRequest) (*client_pb.GetFileChunksInfoResponse, error) {
    if err := s.validateFilename(req.Filename); err != nil {
        return &client_pb.GetFileChunksInfoResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: err.Error(),
            },
        }, nil
    }

    s.Master.filesMu.RLock()
    fileInfo, exists := s.Master.files[req.Filename]
    if !exists {
        s.Master.filesMu.RUnlock()
        return &client_pb.GetFileChunksInfoResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrFileNotFound.Error(),
            },
        }, nil
    }

    fileInfo.mu.RLock()
    defer fileInfo.mu.RUnlock()
    s.Master.filesMu.RUnlock()

    // Validate chunk range
    if req.StartChunk < 0 || req.StartChunk > req.EndChunk {
        return &client_pb.GetFileChunksInfoResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrInvalidChunkRange.Error(),
            },
        }, nil
    }

    s.Master.filesMu.Lock()
    for idx := req.StartChunk; idx <= req.EndChunk; idx++ {
        if idx >= int64(len(fileInfo.Chunks)) {
            // Generate new chunk handle using UUID
            chunkHandle := uuid.New().String()
            fileInfo.Chunks[idx] = chunkHandle 
            
            s.Master.chunksMu.Lock()
            // Create new chunk info
            s.Master.chunks[chunkHandle] = &ChunkInfo{
                mu:        sync.RWMutex{},
                Locations: make(map[string]bool),
            }
            
            // Select initial servers for this chunk
            selectedServers := s.selectInitialChunkServers()
            if len(selectedServers) > 0 {
                // Assign the first server as primary with a lease
                chunkInfo := s.Master.chunks[chunkHandle]
                chunkInfo.mu.Lock()
                chunkInfo.Primary = selectedServers[0]
                chunkInfo.LeaseExpiration = time.Now().Add(time.Minute * 5) // 5-minute lease
                
                // Add all selected servers to locations
                for _, serverId := range selectedServers {
                    chunkInfo.Locations[serverId] = true
                }
                chunkInfo.mu.Unlock()
            }
            s.Master.chunksMu.Unlock()
            
            replicationDone := make(chan struct{})
            go func() {
                s.Master.initiateReplication(chunkHandle)
                close(replicationDone)
            }()
            
            // Wait for replication to complete
            <-replicationDone
        }
    }
    s.Master.filesMu.Unlock()

    // Gather chunk information
    chunks := make(map[int64]*client_pb.ChunkInfo)
    s.Master.chunksMu.RLock()
    defer s.Master.chunksMu.RUnlock()

    for idx := req.StartChunk; idx <= req.EndChunk; idx++ {
        chunkHandle := fileInfo.Chunks[idx]
        chunkInfo := s.Master.chunks[chunkHandle]
        
        if chunkInfo == nil {
            continue
        }

        chunkInfo.mu.RLock()
        needsNewPrimary := chunkInfo.Primary == "" || time.Now().After(chunkInfo.LeaseExpiration)
        chunkInfo.mu.RUnlock()

        if needsNewPrimary {
            // Release the read lock before calling assignNewPrimary
            s.Master.chunksMu.RUnlock()
            s.Master.assignNewPrimary(chunkHandle)
            // Reacquire the read lock
            s.Master.chunksMu.RLock()
        }

        chunkInfo.mu.RLock()
        // Create chunk locations
        locations := make([]*common_pb.ChunkLocation, 0)
        var primaryLocation *common_pb.ChunkLocation

        // log.Print(chunkInfo.Primary)

        // Only set primary location if we have a valid primary with an active lease
        if chunkInfo.Primary != "" && time.Now().Before(chunkInfo.LeaseExpiration) {
            primaryLocation = &common_pb.ChunkLocation{
                ServerId: chunkInfo.Primary,
            }
        }

        // Add all non-primary locations as secondaries
        for serverId := range chunkInfo.Locations {
            if serverId != chunkInfo.Primary {
                locations = append(locations, &common_pb.ChunkLocation{
                    ServerId: serverId,
                })
            }
        }

        // If we still don't have a primary after assignment, skip this chunk
        if primaryLocation == nil {
            chunkInfo.mu.RUnlock()
            continue
        }

        chunks[idx] = &client_pb.ChunkInfo{
            ChunkHandle: &common_pb.ChunkHandle{
                Handle: chunkHandle,
            },
            PrimaryLocation:    primaryLocation,
            SecondaryLocations: locations,
        }
        chunkInfo.mu.RUnlock()
    }

    // If we couldn't get any valid chunks with primaries, return an error
    if len(chunks) == 0 {
        return &client_pb.GetFileChunksInfoResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: "No available chunk servers with valid primaries",
            },
        }, nil
    }

    return &client_pb.GetFileChunksInfoResponse{
        Status: &common_pb.Status{Code: common_pb.Status_OK},
        Chunks: chunks,
    }, nil
}

func (s *MasterServer) CreateFile(ctx context.Context, req *client_pb.CreateFileRequest) (*client_pb.CreateFileResponse, error) {
    if err := s.validateFilename(req.Filename); err != nil {
        return &client_pb.CreateFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: err.Error(),
            },
        }, nil
    }

    s.Master.filesMu.Lock()
    defer s.Master.filesMu.Unlock()

    // Check if file already exists
    if _, exists := s.Master.files[req.Filename]; exists {
        return &client_pb.CreateFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrFileExists.Error(),
            },
        }, nil
    }

    // Create new file entry
    s.Master.files[req.Filename] = &FileInfo{
        Chunks: make(map[int64]string),
    }

    return &client_pb.CreateFileResponse{
        Status: &common_pb.Status{Code: common_pb.Status_OK},
    }, nil
}

func (s *MasterServer) DeleteFile(ctx context.Context, req *client_pb.DeleteFileRequest) (*client_pb.DeleteFileResponse, error) {
    if err := s.validateFilename(req.Filename); err != nil {
        return &client_pb.DeleteFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: err.Error(),
            },
        }, nil
    }

    s.Master.filesMu.Lock()
    fileInfo, exists := s.Master.files[req.Filename]
    if !exists {
        s.Master.filesMu.Unlock()
        return &client_pb.DeleteFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrFileNotFound.Error(),
            },
        }, nil
    }

    // Generate unique trash path
    trashPath := fmt.Sprintf("%s%s", s.Master.Config.Deletion.TrashDirPrefix, req.Filename)
    
    // Move to deleted files
    s.Master.deletedFilesMu.Lock()
    s.Master.deletedFiles[trashPath] = &DeletedFileInfo{
        OriginalPath: req.Filename,
        DeleteTime:   time.Now(),
        FileInfo:     fileInfo,
    }
    s.Master.deletedFilesMu.Unlock()
    
    // Remove from active files
    delete(s.Master.files, req.Filename)
    s.Master.filesMu.Unlock()

    log.Printf("Soft deleted file %s (moved to %s)", req.Filename, trashPath)

    return &client_pb.DeleteFileResponse{
        Status: &common_pb.Status{Code: common_pb.Status_OK},
    }, nil
}