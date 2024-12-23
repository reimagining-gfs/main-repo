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

	chunkserver_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk"
	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
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
		serverID:               serverID,
		address:                address,
		config:                 config,
		dataDir:                config.Server.DataDir,
		serverDir:              serverDir,
		chunks:                 make(map[string]*ChunkMetadata),
		leases:                 make(map[string]time.Time),
		masterClient:           chunk_pb.NewChunkMasterServiceClient(conn),
		heartbeatStop:          make(chan struct{}),
		chunkPrimary:           make(map[string]bool),
		pendingData:            make(map[string]map[string]*PendingData),
		grpcServer:             grpcServer,
		idempotencyIdStatusMap: make(map[string]AppendStatus),
		operationQueue:         NewOperationQueue(),
	}

	chunk_ops.RegisterChunkOperationServiceServer(cs.grpcServer, cs)
	chunkserver_pb.RegisterChunkServiceServer(cs.grpcServer, cs)

	go func() {
		if err := cs.grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	if err := cs.RecoverMetadata(); err != nil {
		log.Printf("Warning: Failed to recover metadata: %v", err)
	}

	cs.StartMetadataCheckpointing(5 * time.Second)

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
	go cs.startLeaseRequester()
	go cs.processOperations()

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

	if err := cs.loadMetadata(); err != nil {
		log.Printf("Failed to load metadata from stable storage: %v", err)
		cs.chunks = make(map[string]*ChunkMetadata)
	}

	files, err := os.ReadDir(cs.serverDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read server directory: %v", err)
	}

	var chunks []*common_pb.ChunkHandle

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".chunk") {
			continue
		}

		handle := strings.TrimSuffix(file.Name(), ".chunk")

		// If metadata doesn't exist for this chunk, create it
		if _, exists := cs.chunks[handle]; !exists {
			cs.chunks[handle] = &ChunkMetadata{
				Size:         0,
				LastModified: time.Now(),
				Version:      1,
			}
		}

		chunks = append(chunks, &common_pb.ChunkHandle{
			Handle:  handle,
			Version: cs.chunks[handle].Version,
		})
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
		ServerId:      cs.serverID,
		ServerAddress: cs.address,
		Chunks:        chunks,
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
	for handle, chunkMetadata := range cs.chunks {
		chunks = append(chunks, &chunk_pb.ChunkStatus{
			ChunkHandle: &common_pb.ChunkHandle{Handle: handle},
			Size:        chunkMetadata.Size,
			Version:     chunkMetadata.Version,
		})
	}

	return &chunk_pb.HeartBeatRequest{
		ServerAddress:    cs.address,
		ServerId:         cs.serverID,
		Timestamp:        time.Now().Format(time.RFC3339),
		Chunks:           chunks,
		AvailableSpace:   cs.availableSpace,
		CpuUsage:         0.0,
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
	case chunk_pb.ChunkCommand_INIT_EMPTY:
		return cs.handleInitEmpty(cmd)

	case chunk_pb.ChunkCommand_BECOME_PRIMARY:
		return cs.handleBecomePrimary(cmd)

	case chunk_pb.ChunkCommand_REPLICATE:
		return cs.handleReplicate(cmd)

	case chunk_pb.ChunkCommand_DELETE:
		return cs.handleDelete(cmd)

	case chunk_pb.ChunkCommand_UPDATE_VERSION:
		return cs.handleUpdateVersion(cmd)

	case chunk_pb.ChunkCommand_NONE:
		return nil

	default:
		return fmt.Errorf("unknown command type: %v", cmd.Type)
	}
}

func (cs *ChunkServer) handleInitEmpty(cmd *chunk_pb.ChunkCommand) error {
	if cmd.ChunkHandle == nil {
		return fmt.Errorf("received init empty command with nil chunk handle")
	}

	chunkHandle := cmd.ChunkHandle.Handle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	_, err := os.Stat(chunkPath)
	if err == nil {
		return fmt.Errorf("chunk %s already exists", chunkHandle)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("error checking chunk file existence: %v", err)
	}

	file, err := os.Create(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to create chunk file %s: %v", chunkHandle, err)
	}
	defer file.Close()

	cs.mu.Lock()
	cs.chunks[chunkHandle] = &ChunkMetadata{
		Size:         0,
		LastModified: time.Now(),
		Version:      0,
	}
	cs.mu.Unlock()

	// Save metadata to stable storage
	if err := cs.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata after chunk creation: %v", err)
	}

	log.Printf("Created new empty chunk: %s with version 1", chunkHandle)
	return nil
}

func (cs *ChunkServer) handleUpdateVersion(cmd *chunk_pb.ChunkCommand) error {
	if cmd.ChunkHandle == nil {
		return fmt.Errorf("received update version command with nil chunk handle")
	}

	chunkHandle := cmd.ChunkHandle.Handle

	cs.mu.Lock()
	metadata, exists := cs.chunks[chunkHandle]
	cs.mu.Unlock()

	if !exists {
		return fmt.Errorf("chunk %s does not exist", chunkHandle)
	}

	metadata.Version = cmd.Version
	metadata.LastModified = time.Now()

	// Save metadata to stable storage
	if err := cs.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata after version update: %v", err)
	}

	log.Printf("Updated chunk %s version to %d", chunkHandle, cmd.Version)
	return nil
}

func (cs *ChunkServer) handleReplicate(cmd *chunk_pb.ChunkCommand) error {
	if cmd.ChunkHandle == nil {
		return fmt.Errorf("received replicate command with nil chunk handle")
	}

	chunkHandle := cmd.ChunkHandle.Handle

	operation := &Operation{
		OperationId:  fmt.Sprintf("replicate-%s-%d", chunkHandle, time.Now().UnixNano()),
		Type:         OpReplicate,
		ChunkHandle:  chunkHandle,
		Secondaries:  cmd.TargetLocations,
		ResponseChan: make(chan OperationResult),
	}

	cs.operationQueue.Push(operation)

	result := <-operation.ResponseChan
	if result.Error != nil {
		return fmt.Errorf("replication failed: %v", result.Error)
	}

	log.Printf("Successfully finished replication for chunk: %s", chunkHandle)
	return nil
}

func (cs *ChunkServer) handleDelete(cmd *chunk_pb.ChunkCommand) error {
	if cmd.ChunkHandle == nil {
		return fmt.Errorf("received delete command with nil chunk handle")
	}

	chunkHandle := cmd.ChunkHandle.Handle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	// Check if the chunk file exists
	_, err := os.Stat(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			// The chunk file doesn't exist, so we can return without doing anything
			return nil
		} else {
			// There was an error checking the file existence
			return fmt.Errorf("error checking chunk file existence: %v", err)
		}
	}

	// Attempt to delete the chunk file
	err = os.Remove(chunkPath)
	if err != nil {
		// If the deletion failed, log the error and retry after a delay
		log.Printf("Failed to delete chunk file %s: %v", chunkHandle, err)

		// Retry the deletion after a delay
		for i := 0; i < 3; i++ {
			time.Sleep(time.Second * 5)
			err = os.Remove(chunkPath)
			if err == nil {
				break
			}
			log.Printf("Retrying delete of chunk file %s (attempt %d): %v", chunkHandle, i+1, err)
		}

		// If the deletion still failed after retries, return the error
		if err != nil {
			return fmt.Errorf("failed to delete chunk file %s after retries: %v", chunkHandle, err)
		}
	}

	// Remove the chunk metadata from the map
	cs.mu.Lock()
	delete(cs.chunks, chunkHandle)
	delete(cs.leases, chunkHandle)
	delete(cs.chunkPrimary, chunkHandle)
	cs.mu.Unlock()

	log.Printf("Deleted chunk: %s", chunkHandle)
	return nil
}

func (cs *ChunkServer) handleBecomePrimary(cmd *chunk_pb.ChunkCommand) error {
	if cmd.ChunkHandle == nil {
		return fmt.Errorf("received become primary command with nil chunk handle")
	}

	chunkHandle := cmd.ChunkHandle.Handle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	_, err := os.Stat(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("chunk data does not exist: %v", chunkHandle)
		} else {
			return fmt.Errorf("error checking chunk data existence: %v", err)
		}
	}

	cs.mu.Lock()
	if _, ok := cs.chunks[chunkHandle]; !ok {
		info, err := os.Stat(chunkPath)
		if err != nil {
			cs.mu.Unlock()
			return fmt.Errorf("failed to get file info for chunk %s: %v", chunkHandle, err)
		}

		cs.chunks[chunkHandle] = &ChunkMetadata{
			Size:         info.Size(),
			LastModified: info.ModTime(),
		}
	}
	cs.mu.Unlock()

	cs.mu.Lock()
	cs.leases[chunkHandle] = time.Now().Add(time.Duration(cs.config.Server.LeaseTimeout) * time.Second)
	cs.chunkPrimary[chunkHandle] = true
	cs.mu.Unlock()

	log.Printf("Acquired lease for chunk %s", chunkHandle)
	return nil
}

func (cs *ChunkServer) startLeaseRequester() {
	ticker := time.NewTicker(time.Duration(cs.config.Server.LeaseRequestInterval) * time.Second)

	// TODO: Extend lease only in case you have an ongoing operation
	for range ticker.C {
		cs.mu.RLock()
		for handle, isPrimary := range cs.chunkPrimary {
			if isPrimary {
				if err := cs.requestLease(handle); err != nil {
					log.Printf("Failed to request lease for chunk %s: %v", handle, err)
				}
			}
		}
		cs.mu.RUnlock()
	}
}

func (cs *ChunkServer) requestLease(chunkHandle string) error {
	ctx := context.Background()
	req := &chunk_pb.RequestLeaseRequest{
		ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
		ServerId:    cs.serverID,
	}

	resp, err := cs.masterClient.RequestLease(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to request lease: %v", err)
	}

	if resp.Status.Code != common_pb.Status_OK {
		return fmt.Errorf("lease request failed: %v", resp.Status)
	}

	if resp.Granted {
		cs.leases[chunkHandle] = time.Now().Add(time.Duration(resp.LeaseExpiration) * time.Second)
		cs.chunkPrimary[chunkHandle] = true
		log.Printf("Acquired lease for chunk %s until %v", chunkHandle, cs.leases[chunkHandle])
	} else {
		delete(cs.leases, chunkHandle)
		delete(cs.chunkPrimary, chunkHandle)
		log.Printf("Failed to acquire lease for chunk %s", chunkHandle)
	}

	return nil
}

func (cs *ChunkServer) processOperations() {
	for {
		operation := cs.operationQueue.Pop()
		if operation == nil {
			continue
		}

		switch operation.Type {
		case OpWrite:
			log.Print("Handling Write")
			err := cs.handleWrite(operation)

			if err != nil {
				operation.ResponseChan <- OperationResult{
					Status: common_pb.Status{
						Code:    common_pb.Status_ERROR,
						Message: err.Error(),
					},
					Error: err,
				}
			} else {
				operation.ResponseChan <- OperationResult{
					Status: common_pb.Status{
						Code:    common_pb.Status_OK,
						Message: "Write operation succeeded",
					},
					Error: nil,
				}
			}

		case OpReplicate:
			log.Print("Handling Replication")
			err := cs.handleReplicateChunk(operation)

			if err != nil {
				operation.ResponseChan <- OperationResult{
					Status: common_pb.Status{
						Code:    common_pb.Status_ERROR,
						Message: err.Error(),
					},
					Error: err,
				}
			} else {
				operation.ResponseChan <- OperationResult{
					Status: common_pb.Status{
						Code:    common_pb.Status_OK,
						Message: "Replicate operation succeeded",
					},
					Error: nil,
				}
			}

		case OpAppend:
			log.Print("Handling Append")
			appendAtOffset, err := cs.handleAppend(operation)

			if err != nil {
				operation.ResponseChan <- OperationResult{
					Status: common_pb.Status{
						Code:    common_pb.Status_ERROR,
						Message: err.Error(),
					},
					Error: err,
				}
			} else {
				operation.ResponseChan <- OperationResult{
					Status: common_pb.Status{
						Code:    common_pb.Status_OK,
						Message: "Append operation succeeded",
					},
					Error:  nil,
					Offset: appendAtOffset,
				}
			}

		default:
			log.Println("Unknown operation type:", operation.Type)
		}
	}
}
