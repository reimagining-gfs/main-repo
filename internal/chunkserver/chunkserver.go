package chunkserver

import (
    "context"
    "fmt"
    "io"
    "log"
    "net"
    "os"
    "path/filepath"
	"strings"
    "time"

    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
    "google.golang.org/grpc"
)

func NewChunkServer(serverID, address string, config *Config) (*ChunkServer, error) {
	serverDir := filepath.Join(config.Server.DataDir, serverID)
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create server directory: %v", err)
	}

	conn, err := grpc.Dial(config.Server.MasterAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	cs := &ChunkServer{
		serverID:      serverID,
		address:       address,
		config:        config,
		dataDir:       config.Server.DataDir,
		serverDir:     serverDir,
		chunks:        make(map[string]*ChunkMetadata),
		leases:        make(map[string]time.Time),
		masterClient:  chunk_pb.NewChunkMasterServiceClient(conn),
		heartbeatStop: make(chan struct{}),
		grpcServer:    grpcServer,
        operationQueue: NewOperationQueue(),
	}

	go func() {
		if err := cs.grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	return cs, nil
}

func (cs *ChunkServer) Start() error {
    cs.mu.Lock()
    if cs.isRunning {
        cs.mu.Unlock()
        return fmt.Errorf("chunk server already running")
    }
    cs.isRunning = true
    cs.mu.Unlock()

    go func() {
        if err := cs.reportChunks(); err != nil {
            log.Printf("Failed to report chunks: %v", err)
        }
    }()


	go cs.startHeartbeat()

    return nil
}

func (cs *ChunkServer) Stop() {
    cs.mu.Lock()
    if !cs.isRunning {
        cs.mu.Unlock()
        return
    }
    cs.isRunning = false
    close(cs.heartbeatStop)
    cs.mu.Unlock()
}

func (cs *ChunkServer) scanChunks() ([]*common_pb.ChunkHandle, error) {
    cs.mu.Lock()
    defer cs.mu.Unlock()

    cs.chunks = make(map[string]*ChunkMetadata)
    
    files, err := os.ReadDir(cs.serverDir)
    if err != nil {
        return nil, fmt.Errorf("failed to read server directory: %v", err)
    }

    var chunks []*common_pb.ChunkHandle
    
    for _, file := range files {
        if file.IsDir() || !strings.HasSuffix(file.Name(), ".chunk") {
            continue
        }

        // Extract chunk handle from filename
        handle := strings.TrimSuffix(file.Name(), ".chunk")
        
        // Get file info for metadata
        // info, err := file.Info()
        // if err != nil {
        //     log.Printf("Error getting info for chunk %s: %v", handle, err)
        //     continue
        // }

        // // Read chunk metadata if exists
        // metadata, err := cs.readChunkMetadata(handle)
        // if err != nil {
        //     log.Printf("Error reading metadata for chunk %s: %v", handle, err)
        //     // Create default metadata if none exists
        //     metadata = &ChunkMetadata{
        //         Size:    info.Size(),
        //         LastModified: info.ModTime(),
        //     }
        // }

        // cs.chunks[handle] = metadata
        chunks = append(chunks, &common_pb.ChunkHandle{Handle: handle})
    }

    return chunks, nil
}

func (cs *ChunkServer) reportChunks() error {
    ctx := context.Background()
    
    chunks, err := cs.scanChunks()
    if err != nil {
        return fmt.Errorf("failed to scan chunks: %v", err)
    }

    stream, err := cs.masterClient.ReportChunk(ctx, &chunk_pb.ReportChunkRequest{
        ServerId: cs.serverID,
        ServerAddress: cs.address,
        Chunks:   chunks,
    })
    if err != nil {
        return fmt.Errorf("failed to create report stream: %v", err)
    }

    for {
        resp, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return fmt.Errorf("error receiving report response: %v", err)
        }

        if err := cs.handleChunkCommand(resp.Command); err != nil {
            log.Printf("Error handling chunk command: %v", err)
        }
    }

    return nil
}

func (cs *ChunkServer) startHeartbeat() {
    ctx := context.Background()
    stream, err := cs.masterClient.HeartBeat(ctx)
    if err != nil {
        log.Printf("Failed to start heartbeat: %v", err)
        return
    }

    ticker := time.NewTicker(time.Duration(cs.config.Server.HeartbeatInterval) * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-cs.heartbeatStop:
            return
        case <-ticker.C:
            req := cs.buildHeartbeatRequest()
            log.Print("Sending Heartbeat")
            if err := stream.Send(req); err != nil {
                log.Printf("Failed to send heartbeat: %v", err)
                continue
            }

            resp, err := stream.Recv()
            if err != nil {
                log.Printf("Failed to receive heartbeat response: %v", err)
                continue
            }

            cs.handleHeartbeatResponse(resp)
        }
    }
}

func (cs *ChunkServer) buildHeartbeatRequest() *chunk_pb.HeartBeatRequest {
    cs.mu.RLock()
    defer cs.mu.RUnlock()

    chunks := make([]*chunk_pb.ChunkStatus, 0, len(cs.chunks))
    for handle, meta := range cs.chunks {
        chunks = append(chunks, &chunk_pb.ChunkStatus{
            ChunkHandle: &common_pb.ChunkHandle{Handle: handle},
            Size:       meta.Size,
        })
    }

    return &chunk_pb.HeartBeatRequest{
        ServerAddress:    cs.address,
        ServerId:         cs.serverID,
        Timestamp:        time.Now().Format(time.RFC3339),
        Chunks:           chunks,
        AvailableSpace:   cs.availableSpace,
        CpuUsage:        0.0, // TODO: Implement CPU usage monitoring
        ActiveOperations: int32(cs.operationQueue.Len()),
    }
}

func (cs *ChunkServer) handleHeartbeatResponse(resp *chunk_pb.HeartBeatResponse) {
    if resp.Status.Code != common_pb.Status_OK {
        log.Printf("Received error status in heartbeat: %v", resp.Status)
        return
    }

    for _, cmd := range resp.Commands {
        if err := cs.handleChunkCommand(cmd); err != nil {
            log.Printf("Failed to handle command %v for chunk %s: %v", 
                cmd.Type, cmd.ChunkHandle.Handle, err)
        }
    }
}

func (cs *ChunkServer) handleChunkCommand(cmd *chunk_pb.ChunkCommand) error {
    if cmd == nil {
        return fmt.Errorf("received nil command")
    }

    log.Printf("Handling command type %v for chunk %s", cmd.Type, cmd.ChunkHandle.Handle)

    switch cmd.Type {
	// TODO: Add other command types as well
    default:
        return fmt.Errorf("unknown command type: %v", cmd.Type)
    }
}