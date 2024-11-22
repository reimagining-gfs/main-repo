package chunkserver

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	chunkserver_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk"
	chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"

	"google.golang.org/grpc"
)

func (cs *ChunkServer) PushDataToPrimary(ctx context.Context, req *chunk_ops.PushDataToPrimaryRequest) (*chunk_ops.PushDataToPrimaryResponse, error) {
	if req.ChunkHandle == nil {
		return &chunk_ops.PushDataToPrimaryResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "chunk handle is nil",
			},
		}, nil
	}

	log.Print("Received Data: Primary")

	chunkHandle := req.ChunkHandle.Handle

	cs.mu.RLock()
	isPrimary, exists := cs.chunkPrimary[chunkHandle]
	hasValidLease := false
	if leaseExpiry, ok := cs.leases[chunkHandle]; ok {
		hasValidLease = leaseExpiry.After(time.Now())
	}
	cs.mu.RUnlock()

	if !exists || !isPrimary {
		return &chunk_ops.PushDataToPrimaryResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "server is not primary for chunk",
			},
		}, nil
	}

	if !hasValidLease {
		return &chunk_ops.PushDataToPrimaryResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "server's lease has expired",
			},
		}, nil
	}

	// TODO: check checksum with the given data
	cs.storePendingData(req.OperationId, chunkHandle, req.Data, req.Checksum, 0)

	var wg sync.WaitGroup
	errChan := make(chan error, len(req.SecondaryLocations))

	for _, secondary := range req.SecondaryLocations {
		wg.Add(1)
		go func(location *common_pb.ChunkLocation) {
			defer wg.Done()
			if err := cs.forwardDataToSecondary(ctx, location, req.OperationId, chunkHandle, req.Data, req.Checksum); err != nil {
				errChan <- fmt.Errorf("failed to forward to %s: %v", location.ServerAddress, err)
			}
		}(secondary)
	}

	wg.Wait()
	close(errChan)

	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return &chunk_ops.PushDataToPrimaryResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("errors forwarding to secondaries: %v", errors),
			},
		}, nil
	}

	return &chunk_ops.PushDataToPrimaryResponse{
		Status: &common_pb.Status{
			Code:    common_pb.Status_OK,
			Message: "data pushed successfully",
		},
	}, nil
}

func (cs *ChunkServer) updateChunkMetadata(chunkHandle string, data []byte, offset int64, version int32) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	metadata, exists := cs.chunks[chunkHandle]
	if !exists {
		metadata = &ChunkMetadata{
			Size:         offset + int64(len(data)),
			LastModified: time.Now(),
			Checksum:     crc32.ChecksumIEEE(data),
			Version:      version,
		}
	} else {
		// Update existing metadata
		if offset+int64(len(data)) > metadata.Size {
			metadata.Size = offset + int64(len(data))
		}
		metadata.LastModified = time.Now()
		metadata.Checksum = crc32.ChecksumIEEE(data)
		metadata.Version = version
	}
	cs.chunks[chunkHandle] = metadata
}

func (cs *ChunkServer) storePendingData(operationID, chunkHandle string, data []byte, checksum uint32, offset int64) {
	cs.pendingDataLock.Lock()
	defer cs.pendingDataLock.Unlock()

	if _, exists := cs.pendingData[operationID]; !exists {
		cs.pendingData[operationID] = make(map[string]*PendingData)
	}

	log.Print("Pending Data: ", data)

	cs.pendingData[operationID][chunkHandle] = &PendingData{
		Data:     data,
		Checksum: checksum,
		Offset:   offset,
	}
}

func (cs *ChunkServer) forwardDataToSecondary(ctx context.Context, location *common_pb.ChunkLocation, operationID, chunkHandle string, data []byte, checksum uint32) error {
	conn, err := grpc.Dial(location.ServerAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to secondary: %v", err)
	}
	defer conn.Close()

	client := chunkserver_pb.NewChunkServiceClient(conn)
	stream, err := client.PushData(ctx)
	if err != nil {
		return fmt.Errorf("failed to create stream: %v", err)
	}

	// TODO: Remove offset from Datachunk - verify whether it is needed. No.
	chunk := &chunkserver_pb.DataChunk{
		OperationId: operationID,
		ChunkHandle: chunkHandle,
		Data:        data,
		Checksum:    checksum,
		Offset:      0,
	}

	if err := stream.Send(chunk); err != nil {
		return fmt.Errorf("failed to send chunk: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %v", err)
	}

	if resp.Status.Code != common_pb.Status_OK {
		return fmt.Errorf("secondary returned error: %s", resp.Status.Message)
	}

	return nil
}

func (cs *ChunkServer) PushData(stream chunkserver_pb.ChunkService_PushDataServer) error {
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&chunkserver_pb.PushDataResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_OK,
					Message: "data received successfully",
				},
			})
		}
		if err != nil {
			return err
		}

		log.Print("Received Data: Secondary")

		// Verify checksum
		computedChecksum := crc32.ChecksumIEEE(chunk.Data)
		if computedChecksum != chunk.Checksum {
			return stream.SendAndClose(&chunkserver_pb.PushDataResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: "checksum mismatch",
				},
			})
		}

		cs.storePendingData(chunk.OperationId, chunk.ChunkHandle, chunk.Data, chunk.Checksum, chunk.Offset)
	}
}

func (cs *ChunkServer) getPendingData(operationID, chunkHandle string) []byte {
	cs.pendingDataLock.RLock()
	defer cs.pendingDataLock.RUnlock()

	if chunkData, exists := cs.pendingData[operationID][chunkHandle]; exists {
		return chunkData.Data
	}
	return nil
}

func (cs *ChunkServer) getPendingDataOffset(operationID, chunkHandle string) uint32 {
	cs.pendingDataLock.RLock()
	defer cs.pendingDataLock.RUnlock()

	if chunkData, exists := cs.pendingData[operationID][chunkHandle]; exists {
		return chunkData.Checksum
	}
	return 0
}

func (cs *ChunkServer) WriteChunk(ctx context.Context, req *chunk_ops.WriteChunkRequest) (*chunk_ops.WriteChunkResponse, error) {
	operation := &Operation{
		OperationId:  req.OperationId,
		Type:         OpWrite,
		ChunkHandle:  req.ChunkHandle.Handle,
		Offset:       req.Offset,
		Data:         cs.getPendingData(req.OperationId, req.ChunkHandle.Handle),
		Checksum:     cs.getPendingDataOffset(req.OperationId, req.ChunkHandle.Handle),
		Secondaries:  req.Secondaries,
		ResponseChan: make(chan OperationResult, 1),
	}

	cs.operationQueue.Push(operation)

	select {
	case result := <-operation.ResponseChan:
		if result.Error != nil {
			return &chunk_ops.WriteChunkResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: result.Error.Error(),
				},
			}, nil
		}

		return &chunk_ops.WriteChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_OK,
				Message: "Write operation completed successfully",
			},
		}, nil

	case <-ctx.Done():
		return &chunk_ops.WriteChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Write operation cancelled",
			},
		}, ctx.Err()
	}
}

func (cs *ChunkServer) RecordAppendChunk(ctx context.Context, req *chunk_ops.RecordAppendChunkRequest) (*chunk_ops.RecordAppendChunkResponse, error) {
	operation := &Operation{
		OperationId:  req.OperationId,
		Type:         OpAppend,
		ChunkHandle:  req.ChunkHandle.Handle,
		Offset:       0,
		Data:         cs.getPendingData(req.OperationId, req.ChunkHandle.Handle),
		Checksum:     cs.getPendingDataOffset(req.OperationId, req.ChunkHandle.Handle),
		Secondaries:  req.Secondaries,
		ResponseChan: make(chan OperationResult, 1),
	}

	cs.operationQueue.Push(operation)

	select {
	case result := <-operation.ResponseChan:
		if result.Error != nil {
			return &chunk_ops.RecordAppendChunkResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: result.Error.Error(),
				},
			}, nil
		}

		return &chunk_ops.RecordAppendChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_OK,
				Message: "Append operation completed successfully",
			},
			OffsetInChunk: result.Offset,
		}, nil

	case <-ctx.Done():
		return &chunk_ops.RecordAppendChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Append operation cancelled",
			},
		}, ctx.Err()
	}
}

func (cs *ChunkServer) ForwardWriteChunk(ctx context.Context, req *chunkserver_pb.ForwardWriteRequest) (*chunkserver_pb.ForwardWriteResponse, error) {
	data := cs.getPendingData(req.OperationId, req.ChunkHandle.Handle)
	if data == nil {
		return &chunkserver_pb.ForwardWriteResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Data not found in pending storage for operation",
			},
		}, nil
	}

	if req.Offset+int64(len(data)) > cs.config.Storage.MaxChunkSize {
		return &chunkserver_pb.ForwardWriteResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Write operation exceeds maximum chunk size",
			},
		}, nil
	}

	chunkHandle := req.ChunkHandle.Handle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	file, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return &chunkserver_pb.ForwardWriteResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Failed to open chunk file: " + err.Error(),
			},
		}, nil
	}
	defer file.Close()

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return &chunkserver_pb.ForwardWriteResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Failed to get file info: " + err.Error(),
			},
		}, nil
	}

	// If write offset is beyond current file size, pad with nulls
	if req.Offset > fileInfo.Size() {
		paddingSize := req.Offset - fileInfo.Size()
		padding := make([]byte, paddingSize)

		// Write padding at the end of current file
		if _, err := file.WriteAt(padding, fileInfo.Size()); err != nil {
			return &chunkserver_pb.ForwardWriteResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: "Failed to write padding: " + err.Error(),
				},
			}, nil
		}
	}

	if _, err := file.WriteAt(data, req.Offset); err != nil {
		return &chunkserver_pb.ForwardWriteResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Failed to write data to chunk file: " + err.Error(),
			},
		}, nil
	}

	// Update metadata using version from request
	cs.updateChunkMetadata(chunkHandle, data, req.Offset, req.Version)

	return &chunkserver_pb.ForwardWriteResponse{
		Status: &common_pb.Status{
			Code:    common_pb.Status_OK,
			Message: "Data written successfully to chunk",
		},
	}, nil
}

func (cs *ChunkServer) forwardWriteToSecondary(ctx context.Context, secondaryLocation *common_pb.ChunkLocation, operation *Operation) error {
	conn, err := grpc.Dial(secondaryLocation.ServerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}
	defer conn.Close()

	client := chunkserver_pb.NewChunkServiceClient(conn)

	forwardWriteReq := &chunkserver_pb.ForwardWriteRequest{
		OperationId: operation.OperationId,
		ChunkHandle: &common_pb.ChunkHandle{Handle: operation.ChunkHandle},
		Offset:      operation.Offset,
		Version:     cs.chunks[operation.ChunkHandle].Version,
	}

	_, err = client.ForwardWriteChunk(ctx, forwardWriteReq)
	if err != nil {
		return fmt.Errorf("failed to forward write to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}

	log.Printf("Successfully forwarded write to secondary %s", secondaryLocation.ServerAddress)
	return nil
}

func (cs *ChunkServer) handleWrite(operation *Operation) error {
	// TODO: Remove data from Operation Object
	data := cs.getPendingData(operation.OperationId, operation.ChunkHandle)
	if data == nil {
		return fmt.Errorf("data not found in pending storage for operation ID %s", operation.OperationId)
	}

	if operation.Offset+int64(len(data)) > cs.config.Storage.MaxChunkSize {
		return fmt.Errorf("write operation exceeds maximum chunk size")
	}

	chunkHandle := operation.ChunkHandle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	file, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// If write offset is beyond current file size, pad with nulls
	if operation.Offset > fileInfo.Size() {
		paddingSize := operation.Offset - fileInfo.Size()
		padding := make([]byte, paddingSize)

		// Write padding at the end of current file
		if _, err := file.WriteAt(padding, fileInfo.Size()); err != nil {
			return fmt.Errorf("failed to write padding: %w", err)
		}
	}

	// Write actual data at specified offset
	if _, err := file.WriteAt(data, operation.Offset); err != nil {
		return fmt.Errorf("failed to write data to chunk file: %w", err)
	}

	log.Println("Data written successfully to primary")

	cs.mu.Lock()
	if metadata, exists := cs.chunks[operation.ChunkHandle]; exists {
		metadata.LastModified = time.Now()
		metadata.Checksum = operation.Checksum
		newSize := operation.Offset + int64(len(operation.Data))
		if newSize > metadata.Size {
			metadata.Size = newSize
		}
	} else {
		cs.chunks[operation.ChunkHandle] = &ChunkMetadata{
			Size:         operation.Offset + int64(len(operation.Data)),
			LastModified: time.Now(),
			Checksum:     operation.Checksum,
			Version:      0,
		}
	}
	cs.mu.Unlock()

	// Forward to secondaries
	var wg sync.WaitGroup
	errorChan := make(chan error, len(operation.Secondaries))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, secondaryLocation := range operation.Secondaries {
		wg.Add(1)
		go func(location *common_pb.ChunkLocation) {
			defer wg.Done()
			err := cs.forwardWriteToSecondary(ctx, location, operation)
			if err != nil {
				errorChan <- fmt.Errorf("secondary %s failed: %w", secondaryLocation.ServerAddress, err)
			}
		}(secondaryLocation)
	}

	wg.Wait()
	close(errorChan)

	var errors []string
	for err := range errorChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("one or more secondaries failed to process the write")
	}

	return nil
}

func (cs *ChunkServer) handleAppend(operation *Operation) (int64, error) {
	data := cs.getPendingData(operation.OperationId, operation.ChunkHandle)
	if data == nil {
		return -1, fmt.Errorf("data not found in pending storage for operation ID %s", operation.OperationId)
	}

	chunkHandle := operation.ChunkHandle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	file, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return -1, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get file info: %w", err)
	}

	appendOffset := fileInfo.Size()

	if appendOffset+int64(len(data)) > cs.config.Storage.MaxChunkSize {
		return -1, fmt.Errorf("append operation exceeds maximum chunk size")
	}

	// Write actual data at specified offset
	if _, err := file.WriteAt(data, appendOffset); err != nil {
		return -1, fmt.Errorf("failed to write data to chunk file: %w", err)
	}

	log.Println("Data written successfully to primary")

	return appendOffset, nil
}

func (cs *ChunkServer) ReadChunk(ctx context.Context, req *chunk_ops.ReadChunkRequest) (*chunk_ops.ReadChunkResponse, error) {
	if req.ChunkHandle == nil {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "chunk handle is nil",
			},
		}, nil
	}

	// Validate read parameters
	if req.Offset < 0 {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "negative offset not allowed",
			},
		}, nil
	}

	if req.Length <= 0 {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "length must be positive",
			},
		}, nil
	}

	chunkHandle := req.ChunkHandle.Handle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	// Check if chunk file exists
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("chunk %s does not exist", chunkHandle),
			},
		}, nil
	}

	// Open the chunk file
	file, err := os.OpenFile(chunkPath, os.O_RDONLY, 0644)
	if err != nil {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("failed to open chunk file: %v", err),
			},
		}, nil
	}
	defer file.Close()

	// Get file size to validate read boundaries
	fileInfo, err := file.Stat()
	if err != nil {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("failed to get chunk file info: %v", err),
			},
		}, nil
	}

	// Check if offset is beyond file size
	if req.Offset >= fileInfo.Size() {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "offset is beyond chunk size",
			},
		}, nil
	}

	// Adjust length if it would read beyond file size
	readLength := req.Length
	if req.Offset+readLength > fileInfo.Size() {
		readLength = fileInfo.Size() - req.Offset
	}

	// Read the data
	data := make([]byte, readLength)
	n, err := file.ReadAt(data, req.Offset)
	if err != nil && err != io.EOF {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("failed to read chunk data: %v", err),
			},
		}, nil
	}

	// Verify the amount of data read
	if int64(n) < readLength {
		data = data[:n] // Truncate the buffer to actual bytes read
	}

	return &chunk_ops.ReadChunkResponse{
		Status: &common_pb.Status{
			Code:    common_pb.Status_OK,
			Message: "chunk read successful",
		},
		Data: data,
	}, nil
}

func (cs *ChunkServer) ForwardReplicateChunk(ctx context.Context, req *chunkserver_pb.ForwardReplicateChunkRequest) (*chunkserver_pb.ForwardReplicateChunkResponse, error) {
	chunkHandle := req.ChunkHandle.Handle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		file, err := os.Create(chunkPath)
		if err != nil {
			return &chunkserver_pb.ForwardReplicateChunkResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: fmt.Sprintf("failed to create chunk file: %v", err),
				},
			}, nil
		}
		file.Close()
	}

	computedChecksum := crc32.ChecksumIEEE(req.Data)
	if computedChecksum != req.Checksum {
		return &chunkserver_pb.ForwardReplicateChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "checksum mismatch",
			},
		}, nil
	}

	file, err := os.OpenFile(chunkPath, os.O_RDWR, 0644)
	if err != nil {
		return &chunkserver_pb.ForwardReplicateChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("failed to open chunk file: %v", err),
			},
		}, nil
	}
	defer file.Close()

	_, err = file.Write(req.Data)
	if err != nil {
		return &chunkserver_pb.ForwardReplicateChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("failed to write data to chunk file: %v", err),
			},
		}, nil
	}

	cs.mu.Lock()
	cs.chunks[chunkHandle] = &ChunkMetadata{
		Size:         int64(len(req.Data)),
		LastModified: time.Now(),
		Checksum:     computedChecksum,
		Version:      req.Version,
	}
	cs.mu.Unlock()

	log.Printf("Data from %v successfully replicated here.", chunkHandle)

	return &chunkserver_pb.ForwardReplicateChunkResponse{
		Status: &common_pb.Status{
			Code:    common_pb.Status_OK,
			Message: "chunk replicated successfully",
		},
	}, nil
}

func (cs *ChunkServer) forwardReplicateToSecondary(ctx context.Context, secondaryLocation *common_pb.ChunkLocation, chunkHandle string, data []byte, checksum uint32) error {
	log.Print("Replicating to Secondary: ", secondaryLocation.ServerAddress)
	conn, err := grpc.Dial(secondaryLocation.ServerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}
	defer conn.Close()

	client := chunkserver_pb.NewChunkServiceClient(conn)

	forwardReplicateReq := &chunkserver_pb.ForwardReplicateChunkRequest{
		ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
		Data:        data,
		Checksum:    checksum,
		Version:     cs.chunks[chunkHandle].Version,
	}

	_, err = client.ForwardReplicateChunk(ctx, forwardReplicateReq)
	if err != nil {
		return fmt.Errorf("failed to forward replicate to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}

	log.Printf("Successfully forwarded replicate to secondary %s", secondaryLocation.ServerAddress)
	return nil
}

func (cs *ChunkServer) handleReplicateChunk(operation *Operation) error {
	chunkHandle := operation.ChunkHandle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	file, err := os.Open(chunkPath)
	if err != nil {
		return fmt.Errorf("failed to open chunk file %s: %v", chunkHandle, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get chunk file info for %s: %v", chunkHandle, err)
	}

	data := make([]byte, fileInfo.Size())
	_, err = io.ReadFull(file, data)
	if err != nil {
		return fmt.Errorf("failed to read chunk data for %s: %v", chunkHandle, err)
	}

	checksum := crc32.ChecksumIEEE(data)

	var wg sync.WaitGroup
	errChan := make(chan error, len(operation.Secondaries))

	for _, secondary := range operation.Secondaries {
		wg.Add(1)
		go func(location *common_pb.ChunkLocation) {
			defer wg.Done()
			if err := cs.forwardReplicateToSecondary(context.Background(), location, chunkHandle, data, checksum); err != nil {
				errChan <- fmt.Errorf("failed to forward replicate to %s: %v", location.ServerAddress, err)
			}
		}(secondary)
	}

	wg.Wait()
	close(errChan)

	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("one or more secondaries failed to process the replicate operation: %v", errors)
	}

	return nil
}
