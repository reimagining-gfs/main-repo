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
    if s.Master.opLog != nil {
        s.Master.opLog.Close()
    }
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
        files: make(map[string]*FileInfo),
        chunks: make(map[string]*ChunkInfo),
        deletedChunks: make(map[string]bool),
        servers: make(map[string]*ServerInfo),
        pendingOps: make(map[string][]*PendingOperation),
        chunkServerMgr: &ChunkServerManager{
			activeStreams: make(map[string]chan *chunk_pb.HeartBeatResponse),
		},
    }
    
    // Initialize operation log

    opLog, err := NewOperationLog(config.OperationLog.Path, config.Metadata.Database.Path)
    if err != nil {
        log.Fatalf("Failed to initialize operation log: %v", err)
    }
    m.opLog = opLog

    // Replay operation log
    if err := m.replayOperationLog(); err != nil {
        log.Fatalf("Failed to replay operation log: %v", err)
    }

    // bg processes to monitor leases, check server health and maintenance of replication
    go m.monitorServerHealth()
    go m.monitorChunkReplication()
    go m.cleanupExpiredLeases()
    go m.runGarbageCollection()
    go m.runPendingOpsCleanup()

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

    go func() {
        ticker := time.NewTicker(time.Duration(server.Master.Config.Metadata.Database.BackupInterval) * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ticker.C:
                if err := server.Master.checkpointMetadata(); err != nil {
                    log.Printf("Failed to checkpoint metadata: %v", err)
                }
            }
        }
    }()

    return server, nil
}

func (s *MasterServer) ReportChunk(req *chunk_pb.ReportChunkRequest, stream chunk_pb.ChunkMasterService_ReportChunkServer) error {
    if req.ServerId == "" {
        return status.Error(codes.InvalidArgument, "server_id is required")
    }

    // Create response channel for this server
    responseChan := make(chan *chunk_pb.HeartBeatResponse, 10)
    defer close(responseChan)

    // Register stream in the existing ChunkServerManager
    s.Master.chunkServerMgr.mu.Lock()
    s.Master.chunkServerMgr.activeStreams[req.ServerId] = responseChan
    s.Master.chunkServerMgr.mu.Unlock()

    // Cleanup on exit
    defer func() {
        s.Master.chunkServerMgr.mu.Lock()
        delete(s.Master.chunkServerMgr.activeStreams, req.ServerId)
        s.Master.chunkServerMgr.mu.Unlock()
    }()

    // Process initial chunk report
    s.Master.serversMu.Lock()
    if _, exists := s.Master.servers[req.ServerId]; !exists {
        s.Master.servers[req.ServerId] = &ServerInfo{
            Address:       req.ServerAddress,
            LastHeartbeat: time.Now(),
            Chunks:        make(map[string]bool),
            Status:        "ACTIVE",
        }
    }
    serverInfo := s.Master.servers[req.ServerId]
    s.Master.serversMu.Unlock()

    s.Master.chunksMu.Lock()
    for _, chunk := range req.Chunks {
        chunkHandle := chunk.Handle
        if _, exists := s.Master.chunks[chunkHandle]; !exists {
            s.Master.chunks[chunkHandle] = &ChunkInfo{
                mu:        sync.RWMutex{},
                Locations: make(map[string]bool),
                ServerAddresses: make(map[string]string),
                Version:         0,
                StaleReplicas:   make(map[string]bool),
            }
        }

        chunkInfo := s.Master.chunks[chunkHandle]
        chunkInfo.mu.Lock()

        if chunk.Version < chunkInfo.Version {
            chunkInfo.StaleReplicas[req.ServerId] = true
            s.scheduleStaleReplicaDelete(chunkHandle, req.ServerId)
        } else {
            delete(chunkInfo.StaleReplicas, req.ServerId)
            chunkInfo.Locations[req.ServerId] = true
            chunkInfo.ServerAddresses[req.ServerId] = req.ServerAddress
        }
        chunkInfo.mu.Unlock()

        serverInfo.mu.Lock()
        serverInfo.Chunks[chunkHandle] = true
        serverInfo.mu.Unlock()
    }
    s.Master.chunksMu.Unlock()

    for {
        select {
        case response, ok := <-responseChan:
            if !ok {
                return nil
            }
            // Convert HeartBeatResponse to ReportChunkResponse for each command
            for _, command := range response.Commands {
                reportResponse := &chunk_pb.ReportChunkResponse{
                    Status:  response.Status,
                    Command: command,
                }
                if err := stream.Send(reportResponse); err != nil {
                    log.Printf("Error sending command to server %s: %v", req.ServerId, err)
                    return err
                }
            }
        case <-stream.Context().Done():
            return stream.Context().Err()
        }
    }
}

// Lease can only be requested by an active primary chunk-server
func (s *MasterServer) RequestLease(ctx context.Context, req *chunk_pb.RequestLeaseRequest) (*chunk_pb.RequestLeaseResponse, error) {
    if req.ChunkHandle.Handle == "" {
        return nil, status.Error(codes.InvalidArgument, "chunk_handle is required")
    }

    log.Print("Received Lease request: ", req.ServerId)

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

    // Check if the requesting server has the latest version
    if staleVersion, isStale := chunkInfo.StaleReplicas[req.ServerId]; isStale {
        return &chunk_pb.RequestLeaseResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: fmt.Sprintf("server has stale version %d (current: %d)", staleVersion, chunkInfo.Version),
            },
            Granted: false,
        }, nil
    }
    
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
    newVersion := chunkInfo.Version + 1

    updateCommand := &chunk_pb.ChunkCommand{
        Type: chunk_pb.ChunkCommand_UPDATE_VERSION,
        ChunkHandle: &common_pb.ChunkHandle{
            Handle: req.ChunkHandle.Handle,
        },
        Version: newVersion,
    }

    s.Master.chunkServerMgr.mu.RLock()
    for serverId := range chunkInfo.Locations {
        if responseChan, exists := s.Master.chunkServerMgr.activeStreams[serverId]; exists {
            response := &chunk_pb.HeartBeatResponse{
                Status:   &common_pb.Status{Code: common_pb.Status_OK},
                Commands: []*chunk_pb.ChunkCommand{updateCommand},
            }

            select {
            case responseChan <- response:
                log.Printf("Sent version update command to server %s for chunk %s (version %d)", 
                    serverId, req.ChunkHandle.Handle, newVersion)
                s.Master.incrementChunkVersion(chunkInfo)
            default:
                log.Printf("Warning: Failed to send version update to server %s (channel full)", serverId)
            }
        }
    }
    s.Master.chunkServerMgr.mu.RUnlock()

    log.Print("Extending: ", req.ServerId)

    return &chunk_pb.RequestLeaseResponse{
        Status:          &common_pb.Status{Code: common_pb.Status_OK},
        Granted:         true,
        LeaseExpiration: chunkInfo.LeaseExpiration.Unix(),
        Version:         chunkInfo.Version,
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

        log.Print("Received HeartBeat message: ", req.ServerId)

        if serverId == "" {
            serverId = req.ServerId
            s.Master.serversMu.Lock()
            s.Master.servers[serverId] = &ServerInfo{
                Address:         req.ServerAddress,
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

        log.Print("Commands: ", commands)
        
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
            chunkInfo := &ChunkInfo{
                mu:        sync.RWMutex{},
                Locations: make(map[string]bool),
                ServerAddresses: make(map[string]string),
                Version:         0,
                StaleReplicas:   make(map[string]bool),
            }
            s.Master.chunks[chunkHandle] = chunkInfo

            metadata := map[string]interface{}{
                "chunk_info": chunkInfo,
                "file_index": idx,
            }

            if err := s.Master.opLog.LogOperation(OpAddChunk, req.Filename, chunkHandle, metadata); err != nil {
                log.Printf("Failed to log chunk creation: %v", err)
            }
            
            // Select initial servers for this chunk
            selectedServers := s.selectInitialChunkServers()
            if len(selectedServers) == 0 {
                s.Master.chunksMu.Unlock()
                continue
            }

            // Prepare INIT_EMPTY command for all selected servers
            initCommand := &chunk_pb.ChunkCommand{
                Type: chunk_pb.ChunkCommand_INIT_EMPTY,
                ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
            }

            // Send INIT_EMPTY command to all selected servers
            var initWg sync.WaitGroup
            initErrors := make([]error, len(selectedServers))
            
            for i, serverId := range selectedServers {
                initWg.Add(1)
                go func(serverIdx int, srvId string) {
                    defer initWg.Done()
                    
                    s.Master.chunkServerMgr.mu.RLock()
                    responseChan, exists := s.Master.chunkServerMgr.activeStreams[srvId]
                    s.Master.chunkServerMgr.mu.RUnlock()
                    
                    if !exists {
                        initErrors[serverIdx] = fmt.Errorf("no active stream for server %s", srvId)
                        return
                    }

                    response := &chunk_pb.HeartBeatResponse{
                        Status:   &common_pb.Status{Code: common_pb.Status_OK},
                        Commands: []*chunk_pb.ChunkCommand{initCommand},
                    }

                    select {
                    case responseChan <- response:
                        // Command sent successfully
                    case <-time.After(5 * time.Second):
                        initErrors[serverIdx] = fmt.Errorf("timeout sending INIT_EMPTY to server %s", srvId)
                    }
                }(i, serverId)
            }

            // Wait for all initialization attempts to complete
            initWg.Wait()

            // Check for initialization errors
            var successfulServers []string
            for i, err := range initErrors {
                if err == nil {
                    successfulServers = append(successfulServers, selectedServers[i])
                } else {
                    log.Printf("Error initializing chunk %s on server %s: %v", 
                        chunkHandle, selectedServers[i], err)
                }
            }

            // Proceed only if we have enough successful initializations
            if len(successfulServers) > 0 {
                chunkInfo := s.Master.chunks[chunkHandle]
                chunkInfo.mu.Lock()
                
                // Add only successful servers to locations
                for _, serverId := range successfulServers {
                    chunkInfo.Locations[serverId] = true

                    s.Master.serversMu.Lock()
                    chunkInfo.ServerAddresses[serverId] = s.Master.servers[serverId].Address
                    s.Master.serversMu.Unlock()
                }

                if err := s.Master.opLog.LogOperation(OpUpdateChunk, req.Filename, chunkHandle, chunkInfo); err != nil {
                    log.Printf("Failed to log chunk update: %v", err)
                }
                chunkInfo.mu.Unlock()
            }
            s.Master.chunksMu.Unlock()
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
            s.Master.chunksMu.RUnlock()
            s.Master.assignNewPrimary(chunkHandle)
            s.Master.chunksMu.RLock()
        }

        chunkInfo.mu.RLock()
        locations := make([]*common_pb.ChunkLocation, 0)
        var primaryLocation *common_pb.ChunkLocation

        s.Master.serversMu.Lock()
        if chunkInfo.Primary != "" && time.Now().Before(chunkInfo.LeaseExpiration) {
            primaryLocation = &common_pb.ChunkLocation{
                ServerId: chunkInfo.Primary,
                ServerAddress: s.Master.servers[chunkInfo.Primary].Address,
            }
        }

        for serverId := range chunkInfo.Locations {
            if serverId != chunkInfo.Primary {
                locations = append(locations, &common_pb.ChunkLocation{
                    ServerId: serverId,
                    ServerAddress: s.Master.servers[serverId].Address,
                })
            }
        }
        s.Master.serversMu.Unlock()

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
    fileInfo := &FileInfo{
        Chunks: make(map[int64]string),
    }

    s.Master.files[req.Filename] = fileInfo

    if err := s.Master.opLog.LogOperation(OpCreateFile, req.Filename, "", fileInfo); err != nil {
        log.Printf("Failed to log file creation: %v", err)
    }

    return &client_pb.CreateFileResponse{
        Status: &common_pb.Status{Code: common_pb.Status_OK},
    }, nil
}

func (s *MasterServer) RenameFile(ctx context.Context, req *client_pb.RenameFileRequest) (*client_pb.RenameFileResponse, error) {
    if err := s.validateFilename(req.OldFilename); err != nil {
        return &client_pb.RenameFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: err.Error(),
            },
        }, nil
    }

    if err := s.validateFilename(req.NewFilename); err != nil {
        return &client_pb.RenameFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: err.Error(),
            },
        }, nil
    }

    s.Master.filesMu.Lock()
    defer s.Master.filesMu.Unlock()

    if _, exists := s.Master.files[req.NewFilename]; exists {
        return &client_pb.RenameFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrFileExists.Error(),
            },
        }, nil
    }

    if _, exists := s.Master.files[req.OldFilename]; !exists {
        return &client_pb.RenameFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrFileNotFound.Error(),
            },
        }, nil
    }

    s.Master.files[req.NewFilename] = s.Master.files[req.OldFilename]
    delete(s.Master.files, req.OldFilename)

    if err := s.Master.opLog.LogOperation(OpRenameFile, req.OldFilename, "", map[string]string{"new_filename": req.NewFilename}); err != nil {
        log.Printf("Failed to log file rename: %v", err)
    }

    return &client_pb.RenameFileResponse{
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

    if err := s.Master.opLog.LogOperation(OpDeleteFile, req.Filename, "", nil); err != nil {
        log.Printf("Failed to log file deletion: %v", err)
    }

    s.Master.filesMu.Lock()
    defer s.Master.filesMu.Unlock()

    _, exists := s.Master.files[req.Filename]
    if !exists {
        return &client_pb.DeleteFileResponse{
            Status: &common_pb.Status{
                Code:    common_pb.Status_ERROR,
                Message: ErrFileNotFound.Error(),
            },
        }, nil
    }

    now := time.Now().Format("2006-01-02T15:04:05")
    trashPath := fmt.Sprintf("%s_%s", req.Filename, now)
    trashPath = fmt.Sprintf("%s%s", s.Master.Config.Deletion.TrashDirPrefix, trashPath)
    
    s.Master.files[trashPath] = s.Master.files[req.Filename]
    delete(s.Master.files, req.Filename)

    log.Printf("Soft deleted file %s (moved to %s)", req.Filename, trashPath)

    return &client_pb.DeleteFileResponse{
        Status: &common_pb.Status{Code: common_pb.Status_OK},
    }, nil
}