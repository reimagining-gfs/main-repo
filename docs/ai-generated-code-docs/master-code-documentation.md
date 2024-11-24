---
title: Master Code Documentation
---
# Introduction

This document will walk you through the implementation of the master in our GFS with exactly-once record-append-semantics.

We will cover:

1. Initialization of the master server.
2. Loading and checkpointing metadata.
3. Handling server health and chunk replication.
4. Managing file and chunk operations.
5. Operation logging and replay.

# Initialization of the master server

The master server is initialized in <SwmPath>[cmd/master/main.go](/cmd/master/main.go)</SwmPath>. The server is created and started, and signal handling for graceful shutdown is set up.

<SwmSnippet path="/cmd/master/main.go" line="28">

---

We create the master server and handle errors if the server creation fails.

```
	server, err := master.NewMasterServer(addr, config)
	if err != nil {
		log.Fatalf("Failed to create master server: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/master/main.go" line="33">

---

We load metadata from the specified path in the configuration.

```
	// Load metadata
	if err := server.Master.LoadMetadata(config.Metadata.Database.Path); err != nil {
		log.Printf("Warning: Failed to load metadata from %s: %v", config.Metadata.Database.Path, err)
	} else {
		log.Printf("Metadata loaded successfully from %s", config.Metadata.Database.Path)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/master/main.go" line="40">

---

Signal handling is set up to ensure the server can be gracefully shut down.

```
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("Master server is running. Press Ctrl+C to stop.")
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/master/main.go" line="45">

---

We wait for a termination signal and then shut down the server gracefully.

```
	// Wait for termination signal
	sig := <-sigChan
	log.Printf("Received termination signal: %v. Shutting down the server...", sig)
```

---

</SwmSnippet>

<SwmSnippet path="/cmd/master/main.go" line="49">

---

```
	// Stop the server gracefully
	server.Stop()
	log.Println("Server stopped successfully. Exiting.")
}
```

---

</SwmSnippet>

# Loading and checkpointing metadata

Metadata is loaded from a file during initialization and periodically checkpointed to ensure consistency.

<SwmSnippet path="/internal/master/metadata.go" line="11">

---

The <SwmToken path="/internal/master/metadata.go" pos="11:9:9" line-data="func (m *Master) LoadMetadata(path string) error {">`LoadMetadata`</SwmToken> function in <SwmPath>[internal/master/metadata.go](/internal/master/metadata.go)</SwmPath> loads metadata from a file or initializes empty maps if the file does not exist.

```
func (m *Master) LoadMetadata(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		log.Printf("Metadata file doesn't exist, starting with empty state.")
		// Initialize empty maps if metadata file doesn't exist
		m.filesMu.Lock()
		m.chunksMu.Lock()
		if m.files == nil {
			m.files = make(map[string]*FileInfo)
		}
		if m.chunks == nil {
			m.chunks = make(map[string]*ChunkInfo)
		}
		m.filesMu.Unlock()
		m.chunksMu.Unlock()
	} else if len(data) > 0 {
		var metadata struct {
			Files  map[string]*FileInfo  `json:"files"`
			Chunks map[string]*ChunkInfo `json:"chunks"`
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="35">

---

```
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="39">

---

```
		m.filesMu.Lock()
		m.chunksMu.Lock()

		// Initialize maps if they're nil
		if m.files == nil {
			m.files = make(map[string]*FileInfo)
		}
		if m.chunks == nil {
			m.chunks = make(map[string]*ChunkInfo)
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="50">

---

```
		// Copy files data directly
		m.files = metadata.Files

		// Convert stored chunk info to full chunk info
		for chunkID, storedInfo := range metadata.Chunks {
			m.chunks[chunkID] = &ChunkInfo{
				Size:            storedInfo.Size,
				Version:         storedInfo.Version,
				Locations:       make(map[string]bool),
				ServerAddresses: make(map[string]string),
				StaleReplicas:   make(map[string]bool),
			}
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="64">

---

```
		m.filesMu.Unlock()
		m.chunksMu.Unlock()
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="68">

---

```
	return m.replayOperationLog()
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="71">

---

The <SwmToken path="/internal/master/metadata.go" pos="71:9:9" line-data="func (m *Master) checkpointMetadata() error {">`checkpointMetadata`</SwmToken> function periodically saves the current state of metadata to a file.

```
func (m *Master) checkpointMetadata() error {
	m.filesMu.RLock()
	m.chunksMu.RLock()

	storedChunks := make(map[string]*ChunkInfo)
	for chunkID, chunk := range m.chunks {
		storedChunks[chunkID] = &ChunkInfo{
			Size:    chunk.Size,
			Version: chunk.Version,
		}
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="83">

---

```
	metadata := struct {
		Files  map[string]*FileInfo  `json:"files"`
		Chunks map[string]*ChunkInfo `json:"chunks"`
	}{
		Files:  m.files,
		Chunks: storedChunks,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="91">

---

```
	data, err := json.MarshalIndent(metadata, "", "  ")
	m.filesMu.RUnlock()
	m.chunksMu.RUnlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="95">

---

```
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="99">

---

```
	tempFile := m.Config.Metadata.Database.Path + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary metadata file: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="104">

---

```
	if err := os.Rename(tempFile, m.Config.Metadata.Database.Path); err != nil {
		return fmt.Errorf("failed to rename metadata file: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="108">

---

```
	m.opLog.mu.Lock()
	defer m.opLog.mu.Unlock()

	if err := m.opLog.logFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate operation log: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="115">

---

```
	if _, err := m.opLog.logFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to reset operation log position: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/metadata.go" line="119">

---

```
	// Reset the writer
	m.opLog.writer = bufio.NewWriter(m.opLog.logFile)

	return nil
}
```

---

</SwmSnippet>

# Handling server health and chunk replication

The master monitors server health, manages chunk replication, and cleans up expired leases.

<SwmSnippet path="/internal/master/monitor.go" line="1">

---

The <SwmToken path="/internal/master/monitor.go" pos="7:9:9" line-data="func (m *Master) monitorServerHealth() {">`monitorServerHealth`</SwmToken> function checks the health of servers and updates their status.

```
package master

import (
	"time"
)

func (m *Master) monitorServerHealth() {
	ticker := time.NewTicker(time.Duration(m.Config.Health.CheckInterval) * time.Second)
	for range ticker.C {
		m.serversMu.Lock()
		for serverId, server := range m.servers {
			server.mu.Lock()
			timeSinceLastHeartbeat := time.Since(server.LastHeartbeat)
			if timeSinceLastHeartbeat > time.Duration(m.Config.Health.Timeout)*time.Second {
				if server.Status == "ACTIVE" {
					server.Status = "INACTIVE"
					server.FailureCount++
				} else if server.Status == "INACTIVE" {
					server.FailureCount++
					if server.FailureCount >= m.Config.Health.MaxFailures {
						server.Status = "DEAD"
						go m.handleServerFailure(serverId)
					}
				}
			}
			server.mu.Unlock()
		}
		m.serversMu.Unlock()
	}
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/monitor.go" line="32">

---

The <SwmToken path="/internal/master/monitor.go" pos="32:9:9" line-data="func (m *Master) monitorChunkReplication() {">`monitorChunkReplication`</SwmToken> function ensures that chunks are replicated to meet the desired replication factor.

```
func (m *Master) monitorChunkReplication() {
	ticker := time.NewTicker(120 * time.Second)
	for range ticker.C {
		m.chunksMu.RLock()
		for chunkHandle, chunkInfo := range m.chunks {
			chunkInfo.mu.RLock()
			if len(chunkInfo.Locations) < m.Config.Replication.Factor {
				go m.initiateReplication(chunkHandle)
			}
			chunkInfo.mu.RUnlock()
		}
		m.chunksMu.RUnlock()
	}
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/monitor.go" line="47">

---

The <SwmToken path="/internal/master/monitor.go" pos="47:9:9" line-data="func (m *Master) cleanupExpiredLeases() {">`cleanupExpiredLeases`</SwmToken> function clears expired leases to allow new primaries to be assigned.

```
func (m *Master) cleanupExpiredLeases() {
	ticker := time.NewTicker(time.Duration(m.Config.Health.CheckInterval) * time.Second)
	for range ticker.C {
		m.chunksMu.Lock()
		now := time.Now()
		for _, chunkInfo := range m.chunks {
			chunkInfo.mu.Lock()
			if !chunkInfo.LeaseExpiration.IsZero() && now.After(chunkInfo.LeaseExpiration) {
				chunkInfo.Primary = ""
				chunkInfo.LeaseExpiration = time.Time{}
			}
			chunkInfo.mu.Unlock()
		}
		m.chunksMu.Unlock()
	}
}
```

---

</SwmSnippet>

# Managing file and chunk operations

The master handles various file and chunk operations, including creation, deletion, and lease management.

<SwmSnippet path="/internal/master/master.go" line="658">

---

The <SwmToken path="/internal/master/master.go" pos="658:9:9" line-data="func (s *MasterServer) CreateFile(ctx context.Context, req *client_pb.CreateFileRequest) (*client_pb.CreateFileResponse, error) {">`CreateFile`</SwmToken> function creates a new file entry and logs the operation.

```
func (s *MasterServer) CreateFile(ctx context.Context, req *client_pb.CreateFileRequest) (*client_pb.CreateFileResponse, error) {
	if err := s.validateFilename(req.Filename); err != nil {
		return &client_pb.CreateFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="668">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="681">

---

```
	// Create new file entry
	fileInfo := &FileInfo{
		Chunks: make(map[int64]string),
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="686">

---

```
	s.Master.files[req.Filename] = fileInfo

	if err := s.Master.opLog.LogOperation(OpCreateFile, req.Filename, "", fileInfo); err != nil {
		log.Printf("Failed to log file creation: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="692">

---

```
	return &client_pb.CreateFileResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
	}, nil
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="351">

---

The <SwmToken path="/internal/master/master.go" pos="351:9:9" line-data="func (s *MasterServer) CreateChunk(fileInfo *FileInfo, filename string, chunkIndex int64) error {">`CreateChunk`</SwmToken> function generates a new chunk handle, logs the creation, and sends initialization commands to selected servers.

```
func (s *MasterServer) CreateChunk(fileInfo *FileInfo, filename string, chunkIndex int64) error {
	// Generate new chunk handle using UUID
	chunkHandle := uuid.New().String()
	log.Printf("Creating for index %d", chunkIndex)
	fileInfo.Chunks[chunkIndex] = chunkHandle

	s.Master.chunksMu.Lock()
	// Create new chunk info
	chunkInfo := &ChunkInfo{
		mu:              sync.RWMutex{},
		Locations:       make(map[string]bool),
		ServerAddresses: make(map[string]string),
		Version:         0,
		StaleReplicas:   make(map[string]bool),
	}
	s.Master.chunks[chunkHandle] = chunkInfo
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="368">

---

```
	metadata := map[string]interface{}{
		"chunk_info": chunkInfo,
		"file_index": chunkIndex,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="373">

---

```
	if err := s.Master.opLog.LogOperation(OpAddChunk, filename, chunkHandle, metadata); err != nil {
		log.Printf("Failed to log chunk creation: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="377">

---

```
	// Select initial servers for this chunk
	selectedServers := s.selectInitialChunkServers()
	if len(selectedServers) == 0 {
		s.Master.chunksMu.Unlock()
		return nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="384">

---

```
	// Prepare INIT_EMPTY command for all selected servers
	initCommand := &chunk_pb.ChunkCommand{
		Type:        chunk_pb.ChunkCommand_INIT_EMPTY,
		ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="390">

---

```
	// Send INIT_EMPTY command to all selected servers
	var initWg sync.WaitGroup
	initErrors := make([]error, len(selectedServers))
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="394">

---

```
	for i, serverId := range selectedServers {
		initWg.Add(1)
		go func(serverIdx int, srvId string) {
			defer initWg.Done()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="399">

---

```
			s.Master.chunkServerMgr.mu.RLock()
			responseChan, exists := s.Master.chunkServerMgr.activeStreams[srvId]
			s.Master.chunkServerMgr.mu.RUnlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="403">

---

```
			if !exists {
				initErrors[serverIdx] = fmt.Errorf("no active stream for server %s", srvId)
				return
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="408">

---

```
			response := &chunk_pb.HeartBeatResponse{
				Status:   &common_pb.Status{Code: common_pb.Status_OK},
				Commands: []*chunk_pb.ChunkCommand{initCommand},
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="413">

---

```
			select {
			case responseChan <- response:
				// Command sent successfully
			case <-time.After(5 * time.Second):
				initErrors[serverIdx] = fmt.Errorf("timeout sending INIT_EMPTY to server %s", srvId)
			}
		}(i, serverId)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="422">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="436">

---

```
	// Proceed only if we have enough successful initializations
	if len(successfulServers) > 0 {
		chunkInfo := s.Master.chunks[chunkHandle]
		chunkInfo.mu.Lock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="441">

---

```
		// Add only successful servers to locations
		for _, serverId := range successfulServers {
			chunkInfo.Locations[serverId] = true
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="445">

---

```
			s.Master.serversMu.Lock()
			chunkInfo.ServerAddresses[serverId] = s.Master.servers[serverId].Address
			s.Master.serversMu.Unlock()
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="450">

---

```
		if err := s.Master.opLog.LogOperation(OpUpdateChunk, filename, chunkHandle, chunkInfo); err != nil {
			log.Printf("Failed to log chunk update: %v", err)
		}
		chunkInfo.mu.Unlock()
	}
	s.Master.chunksMu.Unlock()

	return nil
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="187">

---

The <SwmToken path="/internal/master/master.go" pos="188:9:9" line-data="func (s *MasterServer) RequestLease(ctx context.Context, req *chunk_pb.RequestLeaseRequest) (*chunk_pb.RequestLeaseResponse, error) {">`RequestLease`</SwmToken> function handles lease requests from chunk servers.

```
// Lease can only be requested by an active primary chunk-server
func (s *MasterServer) RequestLease(ctx context.Context, req *chunk_pb.RequestLeaseRequest) (*chunk_pb.RequestLeaseResponse, error) {
	if req.ChunkHandle.Handle == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_handle is required")
	}

	log.Print("Received Lease request: ", req.ServerId)
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="195">

---

```
	s.Master.chunksMu.RLock()
	chunkInfo, exists := s.Master.chunks[req.ChunkHandle.Handle]
	s.Master.chunksMu.RUnlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="199">

---

```
	if !exists {
		return &chunk_pb.RequestLeaseResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "chunk not found",
			},
			Granted: false,
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="209">

---

```
	chunkInfo.mu.Lock()
	defer chunkInfo.mu.Unlock()

	// Check if the requesting server has the latest version
	if staleVersion, isStale := chunkInfo.StaleReplicas[req.ServerId]; isStale {
		return &chunk_pb.RequestLeaseResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("server has stale version %t (current: %d)", staleVersion, chunkInfo.Version),
			},
			Granted: false,
		}, nil
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="223">

---

```
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
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="234">

---

```
	// Extend the lease
	chunkInfo.LeaseExpiration = time.Now().Add(time.Duration(s.Master.Config.Lease.LeaseTimeout) * time.Second)
	newVersion := chunkInfo.Version + 1
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="238">

---

```
	updateCommand := &chunk_pb.ChunkCommand{
		Type: chunk_pb.ChunkCommand_UPDATE_VERSION,
		ChunkHandle: &common_pb.ChunkHandle{
			Handle: req.ChunkHandle.Handle,
		},
		Version: newVersion,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="246">

---

```
	numVersionUpdates := 0
	s.Master.chunkServerMgr.mu.RLock()
	for serverId := range chunkInfo.Locations {
		if responseChan, exists := s.Master.chunkServerMgr.activeStreams[serverId]; exists {
			response := &chunk_pb.HeartBeatResponse{
				Status:   &common_pb.Status{Code: common_pb.Status_OK},
				Commands: []*chunk_pb.ChunkCommand{updateCommand},
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="255">

---

```
			select {
			case responseChan <- response:
				numVersionUpdates = numVersionUpdates + 1
				log.Printf("Sent version update command to server %s for chunk %s (version %d)",
					serverId, req.ChunkHandle.Handle, newVersion)
			default:
				log.Printf("Warning: Failed to send version update to server %s (channel full)", serverId)
			}
		}
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="266">

---

```
	s.Master.chunkServerMgr.mu.RUnlock()

	if numVersionUpdates > 0 {
		s.Master.incrementChunkVersion(req.ChunkHandle.Handle, chunkInfo)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/master.go" line="272">

---

```
	log.Printf("Extending Lease for server %v for chunkhandle %v", req.ServerId, req.ChunkHandle.Handle)

	return &chunk_pb.RequestLeaseResponse{
		Status:          &common_pb.Status{Code: common_pb.Status_OK},
		Granted:         true,
		LeaseExpiration: chunkInfo.LeaseExpiration.Unix(),
		Version:         chunkInfo.Version,
	}, nil
}
```

---

</SwmSnippet>

# Operation logging and replay

The master logs operations to ensure consistency and replays the log during initialization.

<SwmSnippet path="/internal/master/operation-log.go" line="59">

---

The <SwmToken path="/internal/master/operation-log.go" pos="59:9:9" line-data="func (ol *OperationLog) LogOperation(operation string, filename string, chunkHandle string, metadata interface{}) error {">`LogOperation`</SwmToken> function logs various operations to the operation log.

```
func (ol *OperationLog) LogOperation(operation string, filename string, chunkHandle string, metadata interface{}) error {
	ol.mu.Lock()
	defer ol.mu.Unlock()

	entry := LogEntry{
		Timestamp:   time.Now(),
		Operation:   operation,
		Filename:    filename,
		ChunkHandle: chunkHandle,
		Metadata:    metadata,
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="71">

---

```
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="76">

---

```
	if _, err := ol.writer.WriteString(string(data) + "\n"); err != nil {
		return fmt.Errorf("failed to write log entry: %v", err)
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="80">

---

```
	return ol.writer.Flush()
}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="93">

---

The <SwmToken path="/internal/master/operation-log.go" pos="93:9:9" line-data="func (m *Master) replayOperationLog() error {">`replayOperationLog`</SwmToken> function replays the operation log to restore the state during initialization.

```
func (m *Master) replayOperationLog() error {
	file, err := os.Open(m.Config.OperationLog.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="103">

---

```
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("failed to unmarshal log entry: %v", err)
		}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="110">

---

```
		switch entry.Operation {
		case OpCreateFile:
			m.filesMu.Lock()
			m.files[entry.Filename] = &FileInfo{
				Chunks: make(map[int64]string),
			}
			m.filesMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="118">

---

```
		case OpDeleteFile:
			m.filesMu.Lock()
			delete(m.files, entry.Filename)
			m.filesMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="123">

---

```
		case OpRenameFile:
			m.filesMu.Lock()
			if fileInfo, exists := m.files[entry.Filename]; exists {
				m.files[entry.NewFilename] = fileInfo
				delete(m.files, entry.Filename)
			}
			m.filesMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="131">

---

```
		case OpAddChunk:
			metadata, ok := entry.Metadata.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid metadata format for chunk creation")
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="137">

---

```
			// Extract chunk info
			chunkInfoData, ok := metadata["chunk_info"]
			if !ok {
				return fmt.Errorf("chunk_info missing from metadata")
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="143">

---

```
			var chunkInfo ChunkInfo
			chunkInfoBytes, _ := json.Marshal(chunkInfoData)
			if err := json.Unmarshal(chunkInfoBytes, &chunkInfo); err != nil {
				return fmt.Errorf("failed to unmarshal chunk info: %v", err)
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="149">

---

```
			// Extract file index
			fileIndexFloat, ok := metadata["file_index"].(float64)
			if !ok {
				return fmt.Errorf("file_index missing or invalid in metadata")
			}
			fileIndex := int64(fileIndexFloat)
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="156">

---

```
			m.chunksMu.Lock()
			m.chunks[entry.ChunkHandle] = &chunkInfo
			m.chunksMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="160">

---

```
			// Update file's chunk mapping
			m.filesMu.Lock()
			if fileInfo, exists := m.files[entry.Filename]; exists {
				fileInfo.Chunks[fileIndex] = entry.ChunkHandle
			}
			m.filesMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="167">

---

```
		case OpUpdateChunk:
			var chunkInfo ChunkInfo
			metadataBytes, _ := json.Marshal(entry.Metadata)
			if err := json.Unmarshal(metadataBytes, &chunkInfo); err != nil {
				return fmt.Errorf("failed to unmarshal chunk info: %v", err)
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="174">

---

```
			m.chunksMu.Lock()
			if chunk, exists := m.chunks[entry.ChunkHandle]; exists {
				chunk.Locations = chunkInfo.Locations
				chunk.Primary = chunkInfo.Primary
				chunk.LeaseExpiration = chunkInfo.LeaseExpiration
			}
			m.chunksMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="182">

---

```
		case OpDeleteChunk:
			m.chunksMu.Lock()
			delete(m.chunks, entry.ChunkHandle)
			m.chunksMu.Unlock()
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="187">

---

```
		case OpUpdateChunkVersion:
			metadata, ok := entry.Metadata.(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid metadata format for chunk version update")
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="193">

---

```
			version, ok := metadata["version"].(float64)
			if !ok {
				return fmt.Errorf("version missing or invalid in metadata")
			}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="198">

---

```
			m.chunksMu.Lock()
			if chunk, exists := m.chunks[entry.ChunkHandle]; exists {
				chunk.Version = int32(version)
				// Update primary if it was included in the log
				if primary, ok := metadata["primary"].(string); ok {
					chunk.Primary = primary
				}
			}
			m.chunksMu.Unlock()
		}
	}
```

---

</SwmSnippet>

<SwmSnippet path="/internal/master/operation-log.go" line="210">

---

```
	return scanner.Err()
}
```

---

</SwmSnippet>

# Conclusion

This document has covered the initialization, metadata management, server health monitoring, file and chunk operations, and operation logging for the master in our GFS implementation. The design choices ensure robust and consistent management of the file system.

<SwmMeta version="3.0.0" repo-id="Z2l0aHViJTNBJTNBR0ZTLURpc3RyaWJ1dGVkLVN5c3RlbXMlM0ElM0FNaXRhbnNoazAx" repo-name="GFS-Distributed-Systems"><sup>Powered by [Swimm](https://app.swimm.io/)</sup></SwmMeta>
