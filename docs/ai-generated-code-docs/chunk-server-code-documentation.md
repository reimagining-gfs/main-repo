---
title: Chunk Server Code Documentation
---
# Introduction

This document will walk you through the implementation of the GFS chunk server with exactly-once record-append semantics.

We will cover:

1. Initialization and configuration of the chunk server.
2. Communication between the chunk server and the master server.
3. Communication between the client and the chunk server.
4. Communication between chunk servers.
5. Explanation of the Two-Phase Commit Protocol used to ensure exactly-once semantics for appends.

# Initialization and configuration

<SwmSnippet path="/cmd/chunkserver/main.go" line="12">

---

The chunk server starts by parsing <SwmToken path="/cmd/chunkserver/main.go" pos="13:5:7" line-data="	// Parse command-line arguments">`command-line`</SwmToken> arguments to determine the port number and validating it.

```
func main() {
	// Parse command-line arguments
	port := flag.Int("port", 8080, "Port number to run the chunk server on")
	flag.Parse()

	// Validate the provided port number
	if *port <= 0 || *port > 65535 {
		log.Fatalf("Invalid port number: %d. Please provide a port between 1 and 65535.\n", *port)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/chunkserver/main.go" line="22">

---

Next, the server ID and address are generated based on the port number.

```
	// Generate server ID and address
	serverID := fmt.Sprintf("server-%d", *port)
	address := fmt.Sprintf("localhost:%d", *port)
	log.Printf("Initializing chunk server with ID: %s at address: %s\n", serverID, address)
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/chunkserver/main.go" line="27">

---

The server then loads its configuration from a YAML file.

```
	// Load server configuration
	config, err := chunkserver.LoadConfig("../../configs/chunkserver-config.yml")
	if err != nil {
		log.Fatalf("Failed to load configuration file: %v\n", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/chunkserver/main.go" line="33">

---

The server checks if the specified port is available by attempting to listen on it.

```
	// Verify if the port is available
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Port %d is not available: %v\n", *port, err)
	}
	// The listener is not used further; defer closing to clean up resources
	listener.Close()
	log.Printf("Port %d is available. Proceeding to initialize the chunk server.\n", *port)
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/chunkserver/main.go" line="42">

---

A new chunk server instance is created using the loaded configuration.

```
	// Create the chunk server instance
	cs, err := chunkserver.NewChunkServer(serverID, address, config)
	if err != nil {
		log.Fatalf("Failed to create chunk server: %v\n", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/chunkserver/main.go" line="48">

---

Finally, the chunk server is started and kept running indefinitely.

```
	// Start the chunk server
	log.Printf("Starting chunk server with ID: %s\n", serverID)
	if err := cs.Start(); err != nil {
		log.Fatalf("Failed to start chunk server: %v\n", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/chunkserver/main.go" line="54">

---

```
	// Keep the program running indefinitely
	log.Printf("Chunk server with ID %s is now running.\n", serverID)
	select {}
}
```

---

</SwmSnippet>

# Communication between chunk server and master

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="21">

---

The chunk server connects to the master server and registers itself.

```
func NewChunkServer(serverID, address string, config *Config) (*ChunkServer, error) {
	serverDir := filepath.Join(config.Server.DataDir, serverID)
	if err := os.MkdirAll(serverDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create server directory: %v", err)
	}

	conn, err := grpc.Dial(config.Server.MasterAddress, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="37">

---

The chunk server starts a gRPC server to handle incoming requests.

```
	grpcServer := grpc.NewServer()

	cs := &ChunkServer{
		serverID:       serverID,
		address:        address,
		config:         config,
		dataDir:        config.Server.DataDir,
		serverDir:      serverDir,
		chunks:         make(map[string]*ChunkMetadata),
		leases:         make(map[string]time.Time),
		masterClient:   chunk_pb.NewChunkMasterServiceClient(conn),
		heartbeatStop:  make(chan struct{}),
		chunkPrimary:   make(map[string]bool),
		pendingData:    make(map[string]map[string]*PendingData),
		grpcServer:     grpcServer,
		operationQueue: NewOperationQueue(),
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="147">

---

The chunk server reports its chunks to the master server.

```
func (cs *ChunkServer) reportChunks() error {
	ctx := context.Background()

	chunks, err := cs.scanChunks()
	if err != nil {
		return fmt.Errorf("failed to scan chunks: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="155">

---

```
	stream, err := cs.masterClient.ReportChunk(ctx, &chunk_pb.ReportChunkRequest{
		ServerId:      cs.serverID,
		ServerAddress: cs.address,
		Chunks:        chunks,
	})
	if err != nil {
		return fmt.Errorf("failed to create report stream: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="164">

---

```
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving report response: %v", err)
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="173">

---

```
		if err := cs.handleChunkCommand(resp.Command); err != nil {
			log.Printf("Error handling chunk command: %v", err)
		}
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="178">

---

```
	return nil
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="181">

---

The chunk server sends periodic heartbeats to the master server.

```
func (cs *ChunkServer) startHeartbeat() {
	ctx := context.Background()
	stream, err := cs.masterClient.HeartBeat(ctx)
	if err != nil {
		log.Printf("Failed to start heartbeat: %v", err)
		return
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="189">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="204">

---

```
			resp, err := stream.Recv()
			if err != nil {
				log.Printf("Failed to receive heartbeat response: %v", err)
				continue
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="210">

---

```
			cs.handleHeartbeatResponse(resp)
		}
	}
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="215">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="228">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="239">

---

```
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
```

---

</SwmSnippet>

# Communication between client and chunk server

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="233">

---

The chunk server handles write requests from clients.

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="245">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="258">

---

```
		return &chunk_ops.WriteChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_OK,
				Message: "Write operation completed successfully",
			},
		}, nil
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="265">

---

```
	case <-ctx.Done():
		return &chunk_ops.WriteChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Write operation cancelled",
			},
		}, ctx.Err()
	}
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="275">

---

The chunk server handles append requests from clients.

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="287">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="300">

---

```
		return &chunk_ops.RecordAppendChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_OK,
				Message: "Append operation completed successfully",
			},
			OffsetInChunk: result.Offset,
		}, nil
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="308">

---

```
	case <-ctx.Done():
		return &chunk_ops.RecordAppendChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "Append operation cancelled",
			},
		}, ctx.Err()
	}
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="726">

---

The chunk server handles read requests from clients.

```
func (cs *ChunkServer) ReadChunk(ctx context.Context, req *chunk_ops.ReadChunkRequest) (*chunk_ops.ReadChunkResponse, error) {
	if req.ChunkHandle == nil {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "chunk handle is nil",
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="736">

---

```
	// Validate read parameters
	if req.Offset < 0 {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "negative offset not allowed",
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="746">

---

```
	if req.Length <= 0 {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "length must be positive",
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="755">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="768">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="780">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="791">

---

```
	// Check if offset is beyond file size
	if req.Offset >= fileInfo.Size() {
		return &chunk_ops.ReadChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "offset is beyond chunk size",
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="801">

---

```
	// Adjust length if it would read beyond file size
	readLength := req.Length
	if req.Offset+readLength > fileInfo.Size() {
		readLength = fileInfo.Size() - req.Offset
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="807">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="819">

---

```
	// Verify the amount of data read
	if int64(n) < readLength {
		data = data[:n] // Truncate the buffer to actual bytes read
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="824">

---

```
	return &chunk_ops.ReadChunkResponse{
		Status: &common_pb.Status{
			Code:    common_pb.Status_OK,
			Message: "chunk read successful",
		},
		Data: data,
	}, nil
}
```

---

</SwmSnippet>

# Communication between chunk servers

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="143">

---

The chunk server forwards data to secondary chunk servers.

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="156">

---

```
	// TODO: Remove offset from Datachunk - verify whether it is needed. No.
	chunk := &chunkserver_pb.DataChunk{
		OperationId: operationID,
		ChunkHandle: chunkHandle,
		Data:        data,
		Checksum:    checksum,
		Offset:      0,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="165">

---

```
	if err := stream.Send(chunk); err != nil {
		return fmt.Errorf("failed to send chunk: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="169">

---

```
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="174">

---

```
	if resp.Status.Code != common_pb.Status_OK {
		return fmt.Errorf("secondary returned error: %s", resp.Status.Message)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="833">

---

The chunk server handles replication requests from other chunk servers.

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="850">

---

```
	computedChecksum := crc32.ChecksumIEEE(req.Data)
	if computedChecksum != req.Checksum {
		return &chunkserver_pb.ForwardReplicateChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "checksum mismatch",
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="860">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="871">

---

```
	_, err = file.Write(req.Data)
	if err != nil {
		return &chunkserver_pb.ForwardReplicateChunkResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("failed to write data to chunk file: %v", err),
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="881">

---

```
	cs.mu.Lock()
	cs.chunks[chunkHandle] = &ChunkMetadata{
		Size:         int64(len(req.Data)),
		LastModified: time.Now(),
		Checksum:     computedChecksum,
		Version:      req.Version,
	}
	cs.mu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="890">

---

```
	log.Printf("Data from %v successfully replicated here.", chunkHandle)

	return &chunkserver_pb.ForwardReplicateChunkResponse{
		Status: &common_pb.Status{
			Code:    common_pb.Status_OK,
			Message: "chunk replicated successfully",
		},
	}, nil
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="900">

---

The chunk server forwards replication requests to secondary chunk servers.

```
func (cs *ChunkServer) forwardReplicateToSecondary(ctx context.Context, secondaryLocation *common_pb.ChunkLocation, chunkHandle string, data []byte, checksum uint32) error {
	log.Print("Replicating to Secondary: ", secondaryLocation.ServerAddress)
	conn, err := grpc.Dial(secondaryLocation.ServerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}
	defer conn.Close()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="908">

---

```
	client := chunkserver_pb.NewChunkServiceClient(conn)

	forwardReplicateReq := &chunkserver_pb.ForwardReplicateChunkRequest{
		ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
		Data:        data,
		Checksum:    checksum,
		Version:     cs.chunks[chunkHandle].Version,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="917">

---

```
	_, err = client.ForwardReplicateChunk(ctx, forwardReplicateReq)
	if err != nil {
		return fmt.Errorf("failed to forward replicate to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="922">

---

```
	log.Printf("Successfully forwarded replicate to secondary %s", secondaryLocation.ServerAddress)
	return nil
}
```

---

</SwmSnippet>

# Two-phase commit protocol for exactly-once appends

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="571">

---

The chunk server initiates the first phase of the two-phase commit protocol by writing null data to the chunk and verifying if secondaries can write the data.

```
func (cs *ChunkServer) handleAppend(operation *Operation) (int64, error) {
	// Atomic Operation
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	data := cs.getPendingData(operation.OperationId, operation.ChunkHandle)
	if data == nil {
		return -1, fmt.Errorf("data not found in pending storage for operation ID %s", operation.OperationId)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="581">

---

```
	chunkHandle := operation.ChunkHandle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	file, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return -1, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="590">

---

```
	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get file info: %w", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="596">

---

```
	appendOffset := fileInfo.Size()

	if appendOffset+int64(len(data)) >= cs.config.Storage.MaxChunkSize {
		// TODO: pad with the data
		return -1, fmt.Errorf("append operation exceeds maximum chunk size")
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="603">

---

```
	nullBytes := make([]byte, len(data))

	// 1st Phase - Ask the secondaries whether they can write this data
	// Verify whether primary can write this null data
	if _, err := file.WriteAt(nullBytes, appendOffset); err != nil {
		return -1, fmt.Errorf("failed to write data to chunk file: %w", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="611">

---

```
	// Update Metadata
	if metadata, exists := cs.chunks[operation.ChunkHandle]; exists {
		metadata.LastModified = time.Now()
		metadata.Checksum = operation.Checksum
		newSize := appendOffset + int64(len(operation.Data))
		if newSize > metadata.Size {
			metadata.Size = newSize
		}
	} else {
		cs.chunks[operation.ChunkHandle] = &ChunkMetadata{
			Size:         appendOffset + int64(len(operation.Data)),
			LastModified: time.Now(),
			Checksum:     operation.Checksum,
			Version:      0,
		}
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="628">

---

```
	// Verify whether secondaries can write the data
	var wgPhaseOne sync.WaitGroup
	errorChanPhaseOne := make(chan error, len(operation.Secondaries))
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="632">

---

```
	ctxPhaseOne, cancelPhaseOne := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPhaseOne()

	for _, secondaryLocation := range operation.Secondaries {
		wgPhaseOne.Add(1)
		go func(location *common_pb.ChunkLocation) {
			defer wgPhaseOne.Done()
			err := cs.forwardAppendToSecondary(ctxPhaseOne, location, operation, appendOffset, AppendPhaseOne)
			if err != nil {
				errorChanPhaseOne <- fmt.Errorf("secondary %s failed: %w", secondaryLocation.ServerAddress, err)
			}
		}(secondaryLocation)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="646">

---

```
	wgPhaseOne.Wait()
	close(errorChanPhaseOne)

	var errorsInPhaseOne []string
	for err := range errorChanPhaseOne {
		errorsInPhaseOne = append(errorsInPhaseOne, err.Error())
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="654">

---

```
	if len(errorsInPhaseOne) > 0 {
		return -1, fmt.Errorf("1st Append Phase failed -- one or more secondaries failed to process the write")
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="658">

---

```
	log.Println("1st Phase of Exactly-Once Append Completed")

	// Start 2nd Phase of 2PC
	// Send actual data to secondaries
	var wgPhaseTwo sync.WaitGroup
	errorChanPhaseTwo := make(chan error, len(operation.Secondaries))
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="665">

---

In the second phase, the chunk server writes the actual data to the chunk and verifies if secondaries can write the data.

```
	ctxPhaseTwo, cancelPhaseTwo := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelPhaseTwo()

	for _, secondaryLocation := range operation.Secondaries {
		wgPhaseTwo.Add(1)
		go func(location *common_pb.ChunkLocation) {
			defer wgPhaseTwo.Done()
			err := cs.forwardAppendToSecondary(ctxPhaseTwo, location, operation, appendOffset, AppendPhaseTwo)
			if err != nil {
				errorChanPhaseTwo <- fmt.Errorf("secondary %s failed: %w", secondaryLocation.ServerAddress, err)
			}
		}(secondaryLocation)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="679">

---

```
	wgPhaseTwo.Wait()
	close(errorChanPhaseTwo)

	var errorsPhaseTwo []string
	for err := range errorChanPhaseTwo {
		errorsPhaseTwo = append(errorsPhaseTwo, err.Error())
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="687">

---

```
	if len(errorsPhaseTwo) > 0 {
		log.Println("Could not complete 2nd Phase of Exactly-Once Append Completed")

		// Attempt to nullify data at secondaries
		var wgNullify sync.WaitGroup
		errorChanNullify := make(chan error, len(operation.Secondaries))
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="694">

---

```
		ctxNullify, cancelNullify := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelNullify()

		for _, secondaryLocation := range operation.Secondaries {
			wgNullify.Add(1)
			go func(location *common_pb.ChunkLocation) {
				defer wgNullify.Done()
				err := cs.forwardAppendToSecondary(ctxNullify, location, operation, appendOffset, AppendNullify)
				if err != nil {
					errorChanNullify <- fmt.Errorf("secondary %s failed: %w", secondaryLocation.ServerAddress, err)
				}
			}(secondaryLocation)
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="708">

---

```
		wgNullify.Wait()
		close(errorChanNullify)

		return -1, fmt.Errorf("2nd Append Phase failed -- one or more secondaries failed to process the write")
	}
	log.Println("Appended data to secondaries")

	// Write data to primary
	if _, err := file.WriteAt(data, appendOffset); err != nil {
		return -1, fmt.Errorf("failed to write data to chunk file: %w", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="720">

---

```
	log.Println("Appended data to primary")
	log.Println("2nd Phase of Exactly-Once Append Completed")

	return appendOffset, nil
}
```

---

</SwmSnippet>

This ensures that appends are performed exactly once, even in the presence of failures.

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBR0ZTLURpc3RyaWJ1dGVkLVN5c3RlbXMlM0ElM0FNaXRhbnNoazAx" repo-name="GFS-Distributed-Systems"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
