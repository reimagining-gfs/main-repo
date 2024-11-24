---
title: Workflow Documentation of Chunk Server Interations
---
# Introduction

This document will walk you through the implementation of the chunk server interactions in a distributed file system. The feature focuses on the initialization, metadata management, data mutation, and exactly-once append operations.

We will cover:

1. Initialization of chunk server and processes done within it.
2. Metadata being stored.
3. How data is pushed to all the chunk servers for a mutation.
4. How writes are done.
5. How appends are done using the 2 Phase Commit Protocol.

# Initialization of chunk server and processes done within it

<SwmSnippet path="/cmd/chunkserver/main.go" line="12">

---

The chunk server initialization begins in <SwmPath>[cmd/chunkserver/main.go](/cmd/chunkserver/main.go)</SwmPath> where the server ID and address are generated based on the provided port number. This ensures each chunk server has a unique identifier and address. The server configuration is then loaded from a YAML file, which includes settings for the master address, data directory, heartbeat interval, and lease timeout.

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

Next, the chunk server verifies if the specified port is available by attempting to listen on it. If successful, the port is closed, and the chunk server instance is created using the <SwmToken path="/cmd/chunkserver/main.go" pos="43:10:10" line-data="	cs, err := chunkserver.NewChunkServer(serverID, address, config)">`NewChunkServer`</SwmToken> function in <SwmPath>[internal/chunkserver/chunkserver.go](/internal/chunkserver/chunkserver.go)</SwmPath>. This function sets up the server directory, connects to the master server, and initializes various internal structures such as the chunk metadata map, leases map, and operation queue.

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

Finally, the chunk server starts by reporting its chunks to the master, initiating the heartbeat process, and starting the lease requester and operation processor. These processes ensure the chunk server remains in sync with the master and other chunk servers, and can handle incoming operations efficiently.

```
	// Keep the program running indefinitely
	log.Printf("Chunk server with ID %s is now running.\n", serverID)
	select {}
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="73">

---

```
func (cs *ChunkServer) Start() error {
	cs.mu.Lock()
	if cs.isRunning {
		cs.mu.Unlock()
		return fmt.Errorf("chunk server already running")
	}
	cs.isRunning = true
	cs.mu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunkserver.go" line="88">

---

```
	go cs.startHeartbeat()
	go cs.startLeaseRequester()
	go cs.processOperations()
```

---

</SwmSnippet>

# Metadata being stored

Metadata management is crucial for maintaining the state and consistency of the chunk server. The metadata includes information about each chunk, such as its size, last modified time, checksum, and version. This metadata is stored in a map within the chunk server and is periodically checkpointed to stable storage.

<SwmSnippet path="/internal/chunkserver/config.go" line="19">

---

The <SwmToken path="/internal/chunkserver/types.go" pos="15:2:2" line-data="type ChunkMetadata struct {">`ChunkMetadata`</SwmToken> struct in <SwmPath>[internal/chunkserver/types.go](/internal/chunkserver/types.go)</SwmPath> defines the metadata fields. The <SwmToken path="/internal/chunkserver/metadata.go" pos="17:9:9" line-data="func (cs *ChunkServer) saveMetadata() error {">`saveMetadata`</SwmToken> function in <SwmPath>[internal/chunkserver/metadata.go](/internal/chunkserver/metadata.go)</SwmPath> serializes this metadata to a JSON file, ensuring it is saved atomically by writing to a temporary file first and then renaming it. This prevents partial writes from corrupting the metadata.

```
	Storage struct {
		MaxChunkSize  int64 `yaml:"max_chunk_size"`
		BufferSize    int   `yaml:"buffer_size"`
		FlushInterval int   `yaml:"flush_interval"`
	} `yaml:"storage"`
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/config.go" line="25">

---

```
	Operation struct {
		ReadTimeout   int `yaml:"read_timeout"`
		WriteTimeout  int `yaml:"write_timeout"`
		RetryAttempts int `yaml:"retry_attempts"`
		RetryDelay    int `yaml:"retry_delay"`
	} `yaml:"operation"`
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/config.go" line="33">

---

```
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/config.go" line="44">

---

During initialization, the chunk server attempts to recover metadata from the JSON file using the <SwmToken path="/internal/chunkserver/metadata.go" pos="44:9:9" line-data="func (cs *ChunkServer) loadMetadata() error {">`loadMetadata`</SwmToken> function. If the file does not exist, an empty metadata map is initialized. The <SwmToken path="/internal/chunkserver/metadata.go" pos="80:9:9" line-data="func (cs *ChunkServer) RecoverMetadata() error {">`RecoverMetadata`</SwmToken> function also verifies the existence of chunk files and removes metadata for any missing chunks. This ensures the metadata accurately reflects the current state of the chunk server.

```
	return config, nil
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="1">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="14">

---

```
	chunkserver_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk"
	chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
```

---

</SwmSnippet>

# How data is pushed to all the chunk servers for a mutation

Data mutations, such as writes and appends, require pushing data to all relevant chunk servers. This process begins with the client sending a <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="21:23:23" line-data="func (cs *ChunkServer) PushDataToPrimary(ctx context.Context, req *chunk_ops.PushDataToPrimaryRequest) (*chunk_ops.PushDataToPrimaryResponse, error) {">`PushDataToPrimaryRequest`</SwmToken> to the primary chunk server. The primary chunk server validates the request, stores the pending data, and forwards it to the secondary chunk servers.

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="21">

---

The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="21:9:9" line-data="func (cs *ChunkServer) PushDataToPrimary(ctx context.Context, req *chunk_ops.PushDataToPrimaryRequest) (*chunk_ops.PushDataToPrimaryResponse, error) {">`PushDataToPrimary`</SwmToken> function in <SwmPath>[internal/chunkserver/chunk_operations.go](/internal/chunkserver/chunk_operations.go)</SwmPath> handles the initial request from the client. It checks if the server is the primary for the chunk and if it has a valid lease. If both conditions are met, the data is stored in the pending data map, and a goroutine is spawned for each secondary server to forward the data.

```
func (cs *ChunkServer) PushDataToPrimary(ctx context.Context, req *chunk_ops.PushDataToPrimaryRequest) (*chunk_ops.PushDataToPrimaryResponse, error) {
	if req.ChunkHandle == nil {
		return &chunk_ops.PushDataToPrimaryResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "chunk handle is nil",
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="31">

---

```
	log.Print("Received Data: Primary")

	chunkHandle := req.ChunkHandle.Handle
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="35">

---

```
	cs.mu.RLock()
	isPrimary, exists := cs.chunkPrimary[chunkHandle]
	hasValidLease := false
	if leaseExpiry, ok := cs.leases[chunkHandle]; ok {
		hasValidLease = leaseExpiry.After(time.Now())
	}
	cs.mu.RUnlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="67">

---

The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="71:9:9" line-data="			if err := cs.forwardDataToSecondary(ctx, location, req.OperationId, chunkHandle, req.Data, req.Checksum); err != nil {">`forwardDataToSecondary`</SwmToken> function establishes a gRPC connection to the secondary server and sends the data using the <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="151:10:10" line-data="	stream, err := client.PushData(ctx)">`PushData`</SwmToken> RPC. The secondary server receives the data, verifies the checksum, and stores it in the pending data map. This ensures all chunk servers involved in the mutation have the data before the actual write or append operation is performed.

```
	for _, secondary := range req.SecondaryLocations {
		wg.Add(1)
		go func(location *common_pb.ChunkLocation) {
			defer wg.Done()
			if err := cs.forwardDataToSecondary(ctx, location, req.OperationId, chunkHandle, req.Data, req.Checksum); err != nil {
				errChan <- fmt.Errorf("failed to forward to %s: %v", location.ServerAddress, err)
			}
		}(secondary)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="77">

---

```
	wg.Wait()
	close(errChan)

	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="85">

---

```
	if len(errors) > 0 {
		return &chunk_ops.PushDataToPrimaryResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("errors forwarding to secondaries: %v", errors),
			},
		}, nil
	}
```

---

</SwmSnippet>

# How writes are done

Write operations involve writing data to a specific offset within a chunk. The process begins with the client sending a <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="233:23:23" line-data="func (cs *ChunkServer) WriteChunk(ctx context.Context, req *chunk_ops.WriteChunkRequest) (*chunk_ops.WriteChunkResponse, error) {">`WriteChunkRequest`</SwmToken> to the primary chunk server. The primary server retrieves the pending data and pushes a write operation to the operation queue.

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="233">

---

The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="233:9:9" line-data="func (cs *ChunkServer) WriteChunk(ctx context.Context, req *chunk_ops.WriteChunkRequest) (*chunk_ops.WriteChunkResponse, error) {">`WriteChunk`</SwmToken> function in <SwmPath>[internal/chunkserver/chunk_operations.go](/internal/chunkserver/chunk_operations.go)</SwmPath> creates an <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="234:6:6" line-data="	operation := &amp;Operation{">`Operation`</SwmToken> struct representing the write operation. This struct includes the operation ID, chunk handle, offset, data, checksum, and secondary locations. The operation is then pushed to the operation queue for processing.

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

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="476">

---

The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="476:9:9" line-data="func (cs *ChunkServer) handleWrite(operation *Operation) error {">`handleWrite`</SwmToken> function processes the write operation by writing the data to the chunk file at the specified offset. If the offset is beyond the current file size, the file is padded with null bytes. The chunk metadata is updated with the new size, last modified time, and checksum. The write operation is then forwarded to the secondary servers using the <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="403:9:9" line-data="func (cs *ChunkServer) forwardWriteToSecondary(ctx context.Context, secondaryLocation *common_pb.ChunkLocation, operation *Operation) error {">`forwardWriteToSecondary`</SwmToken> function.

```
func (cs *ChunkServer) handleWrite(operation *Operation) error {
	// TODO: Remove data from Operation Object
	data := cs.getPendingData(operation.OperationId, operation.ChunkHandle)
	if data == nil {
		return fmt.Errorf("data not found in pending storage for operation ID %s", operation.OperationId)
	}

	if operation.Offset+int64(len(data)) > cs.config.Storage.MaxChunkSize {
		return fmt.Errorf("write operation exceeds maximum chunk size")
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="487">

---

```
	chunkHandle := operation.ChunkHandle
	chunkPath := filepath.Join(cs.serverDir, chunkHandle+".chunk")

	file, err := os.OpenFile(chunkPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="496">

---

```
	// Get current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
```

---

</SwmSnippet>

# How appends are done using the 2 Phase Commit Protocol

Appends in this implementation use the 2 Phase Commit Protocol to ensure exactly-once semantics. The process begins with the client sending a <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="275:23:23" line-data="func (cs *ChunkServer) RecordAppendChunk(ctx context.Context, req *chunk_ops.RecordAppendChunkRequest) (*chunk_ops.RecordAppendChunkResponse, error) {">`RecordAppendChunkRequest`</SwmToken> to the primary chunk server. The primary server retrieves the pending data and initiates the first phase of the <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="660:11:11" line-data="	// Start 2nd Phase of 2PC">`2PC`</SwmToken> protocol.

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="275">

---

The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="275:9:9" line-data="func (cs *ChunkServer) RecordAppendChunk(ctx context.Context, req *chunk_ops.RecordAppendChunkRequest) (*chunk_ops.RecordAppendChunkResponse, error) {">`RecordAppendChunk`</SwmToken> function in <SwmPath>[internal/chunkserver/chunk_operations.go](/internal/chunkserver/chunk_operations.go)</SwmPath> creates an <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="276:6:6" line-data="	operation := &amp;Operation{">`Operation`</SwmToken> struct representing the append operation. The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="571:9:9" line-data="func (cs *ChunkServer) handleAppend(operation *Operation) (int64, error) {">`handleAppend`</SwmToken> function processes the operation by first writing null bytes to the chunk file at the append offset. This is the first phase, where the primary and secondary servers verify they can write the data.

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

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="428">

---

If the first phase is successful, the second phase begins, where the actual data is written to the chunk file. The <SwmToken path="/internal/chunkserver/chunk_operations.go" pos="439:9:9" line-data="func (cs *ChunkServer) forwardAppendToSecondary(ctx context.Context, secondaryLocation *common_pb.ChunkLocation, operation *Operation, appendOffset int64, phase AppendPhaseType) error {">`forwardAppendToSecondary`</SwmToken> function is used to forward the append operation to the secondary servers. If any secondary server fails during the second phase, the primary server attempts to nullify the data at the secondaries to maintain consistency.

```
func (cs *ChunkServer) ForwardAppendChunkPhaseOne(ctx context.Context, req *chunkserver_pb.ForwardWriteRequest) (*chunkserver_pb.ForwardWriteResponse, error) {
	actualData := cs.getPendingData(req.OperationId, req.ChunkHandle.Handle)
	nullData := make([]byte, len(actualData))
	return cs.ForwardWriteChunkImpl(ctx, req, nullData)
}

func (cs *ChunkServer) ForwardAppendChunkPhaseTwo(ctx context.Context, req *chunkserver_pb.ForwardWriteRequest) (*chunkserver_pb.ForwardWriteResponse, error) {
	data := cs.getPendingData(req.OperationId, req.ChunkHandle.Handle)
	return cs.ForwardWriteChunkImpl(ctx, req, data)
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="439">

---

```
func (cs *ChunkServer) forwardAppendToSecondary(ctx context.Context, secondaryLocation *common_pb.ChunkLocation, operation *Operation, appendOffset int64, phase AppendPhaseType) error {
	conn, err := grpc.Dial(secondaryLocation.ServerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("failed to connect to secondary %s: %w", secondaryLocation.ServerAddress, err)
	}
	defer conn.Close()

	client := chunkserver_pb.NewChunkServiceClient(conn)
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="448">

---

```
	forwardAppendRequest := &chunkserver_pb.ForwardWriteRequest{
		OperationId: operation.OperationId,
		ChunkHandle: &common_pb.ChunkHandle{Handle: operation.ChunkHandle},
		Offset:      appendOffset,
		Version:     cs.chunks[operation.ChunkHandle].Version,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/chunkserver/chunk_operations.go" line="571">

---

The append operation is completed when the primary server successfully writes the data and receives acknowledgments from all secondary servers. The chunk metadata is updated, and the operation result is sent back to the client. This ensures the append operation is performed exactly once, even in the presence of failures.

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

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBR0ZTLURpc3RyaWJ1dGVkLVN5c3RlbXMlM0ElM0FNaXRhbnNoazAx" repo-name="GFS-Distributed-Systems"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
