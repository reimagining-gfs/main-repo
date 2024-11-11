package master

import (
    "context"
    "os"
    "fmt"
    "io"
    "io/ioutil"
    "net"
    "sync"
    "strings"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"
    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
    "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
    "google.golang.org/grpc/metadata"
)

func TestLoadConfig(t *testing.T) {
    configPath := "../../configs/general-config.yml"
    config, err := LoadConfig(configPath)
    if err != nil {
        t.Errorf("LoadConfig(%s) returned an error: %v", configPath, err)
    }

    // Test that all required fields are present and of correct type
    if config.Server.Host == "" {
        t.Error("Server.Host is empty")
    }
    if config.Server.Port == 0 {
        t.Error("Server.Port is not set")
    }
    if config.Server.MaxConnections == 0 {
        t.Error("Server.MaxConnections is not set")
    }
    if config.Server.ConnectionTimeout == 0 {
        t.Error("Server.ConnectionTimeout is not set")
    }
    if config.Server.MaxRequestSize == 0 {
        t.Error("Server.MaxRequestSize is not set")
    }
    if config.Server.ThreadPoolSize == 0 {
        t.Error("Server.ThreadPoolSize is not set")
    }

    // Chunk configuration
    if config.Chunk.Size == 0 {
        t.Error("Chunk.Size is not set")
    }
    if config.Chunk.NamingPattern == "" {
        t.Error("Chunk.NamingPattern is empty")
    }
    if config.Chunk.ChecksumAlgorithm == "" {
        t.Error("Chunk.ChecksumAlgorithm is empty")
    }

    // Deletion configuration
    if config.Deletion.GCInterval == 0 {
        t.Error("Deletion.GCInterval is not set")
    }
    if config.Deletion.RetentionPeriod == 0 {
        t.Error("Deletion.RetentionPeriod is not set")
    }
    if config.Deletion.GCDeleteBatchSize == 0 {
        t.Error("Deletion.GCDeleteBatchSize is not set")
    }
    if config.Deletion.TrashDirPrefix == "" {
        t.Error("Deletion.TrashDirPrefix is empty")
    }

    // Replication configuration
    if config.Replication.Factor == 0 {
        t.Error("Replication.Factor is not set")
    }
    if config.Replication.Timeout == 0 {
        t.Error("Replication.Timeout is not set")
    }

    // Health configuration
    if config.Health.CheckInterval == 0 {
        t.Error("Health.CheckInterval is not set")
    }
    if config.Health.Timeout == 0 {
        t.Error("Health.Timeout is not set")
    }
    if config.Health.MaxFailures == 0 {
        t.Error("Health.MaxFailures is not set")
    }

    // Metadata configuration
    if config.Metadata.Database.Type == "" {
        t.Error("Metadata.Database.Type is empty")
    }
    if config.Metadata.Database.Path == "" {
        t.Error("Metadata.Database.Path is empty")
    }
    if config.Metadata.Database.BackupInterval == 0 {
        t.Error("Metadata.Database.BackupInterval is not set")
    }
    if config.Metadata.MaxFilenameLength == 0 {
        t.Error("Metadata.MaxFilenameLength is not set")
    }
    if config.Metadata.MaxDirectoryDepth == 0 {
        t.Error("Metadata.MaxDirectoryDepth is not set")
    }

    // Lease configuration
    if config.Lease.LeaseTimeout == 0 {
        t.Error("Lease.LeaseTimeout is not set")
    }
}

func TestLoadConfigInvalidPath(t *testing.T) {
    invalidPath := "/this/path/does/not/exist/config.yaml"
    _, err := LoadConfig(invalidPath)
    if err == nil {
        t.Errorf("LoadConfig(%s) did not return an error", invalidPath)
    }
}

func TestLoadConfigEmptyPath(t *testing.T) {
    _, err := LoadConfig("")
    if err == nil {
        t.Errorf("LoadConfig('') did not return an error")
    }
}

func TestLoadConfigPermissionDenied(t *testing.T) {
    tmpDir := t.TempDir()
    permDeniedPath := fmt.Sprintf("%s/config.yaml", tmpDir)
    err := ioutil.WriteFile(permDeniedPath, []byte{}, 0000)
    if err != nil {
        t.Errorf("Failed to create file with permission denied: %v", err)
    }

    _, err = LoadConfig(permDeniedPath)
    if err == nil {
        t.Errorf("LoadConfig(%s) did not return an error", permDeniedPath)
    }

    // Cleanup
    os.Remove(permDeniedPath)
}

type MockReportChunkStream struct {
    grpc.ServerStream
    ctx     context.Context
    sent    []*chunk_pb.ReportChunkResponse
    recvErr error
    mu      sync.Mutex
}

func (m *MockReportChunkStream) Context() context.Context {
    if m.ctx != nil {
        return m.ctx
    }
    return context.Background()
}

func (m *MockReportChunkStream) Send(resp *chunk_pb.ReportChunkResponse) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.sent = append(m.sent, resp)
    return nil
}

func (m *MockReportChunkStream) RecvMsg(msg interface{}) error {
    if m.recvErr != nil {
        return m.recvErr
    }
    return nil
}

func (m *MockReportChunkStream) SendMsg(msg interface{}) error {
    if resp, ok := msg.(*chunk_pb.ReportChunkResponse); ok {
        return m.Send(resp)
    }
    return nil
}

func (m *MockReportChunkStream) SetHeader(metadata.MD) error {
    return nil
}

func (m *MockReportChunkStream) SendHeader(metadata.MD) error {
    return nil
}

func (m *MockReportChunkStream) SetTrailer(metadata.MD) {
}

func TestMasterServer_ReportChunk(t *testing.T) {
    masterServer := setupMasterServer(t)
    defer masterServer.Stop()
    
    t.Run("Valid chunk report", func(t *testing.T) {
        req := &chunk_pb.ReportChunkRequest{
            ServerId: "server1",
            Chunks: []*common.ChunkHandle{
                {
                    Handle: "chunk1",
                },
            },
        }

        ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
        defer cancel()
        
        mockStream := &MockReportChunkStream{
            ctx: ctx,
        }

        // Start ReportChunk in a goroutine
        errChan := make(chan error, 1)
        go func() {
            err := masterServer.ReportChunk(req, mockStream)
            errChan <- err
        }()

        // Wait for either context cancellation or error
        select {
        case err := <-errChan:
            if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
                t.Errorf("ReportChunk() unexpected error = %v", err)
            }
        case <-time.After(200 * time.Millisecond):
            t.Error("Test timed out")
        }

        // Verify server state
        masterServer.Master.serversMu.RLock()
        serverInfo, exists := masterServer.Master.servers[req.ServerId]
        masterServer.Master.serversMu.RUnlock()

        if !exists {
            t.Error("Server was not registered")
            return
        }

        if serverInfo != nil {
            serverInfo.mu.RLock()
            if len(serverInfo.Chunks) != 1 {
                t.Errorf("Expected 1 chunk, got %d", len(serverInfo.Chunks))
            }
            if !serverInfo.Chunks["chunk1"] {
                t.Error("Chunk1 not registered with server")
            }
            serverInfo.mu.RUnlock()
        }
    })

    t.Run("Empty server ID", func(t *testing.T) {
        req := &chunk_pb.ReportChunkRequest{
            ServerId: "",
            Chunks: []*common.ChunkHandle{
                {
                    Handle: "chunk1",
                },
            },
        }

        mockStream := &MockReportChunkStream{}
        
        err := masterServer.ReportChunk(req, mockStream)

        st, ok := status.FromError(err)
        if !ok {
            t.Error("Expected gRPC status error")
        }

        if st.Code() != codes.InvalidArgument {
            t.Errorf("Expected InvalidArgument error code, got %v", st.Code())
        }
    })

    // t.Run("Stream error handling", func(t *testiC
}

func TestMasterServer_RequestLease(t *testing.T) {
	// Setup
	masterServer := setupMasterServer(t)
	defer masterServer.Stop()

	t.Run("Empty chunk handle", func(t *testing.T) {
		req := &chunk_pb.RequestLeaseRequest{
			ChunkHandle: &common.ChunkHandle{Handle: ""},
			ServerId:    "server1",
		}

		_, err := masterServer.RequestLease(context.Background(), req)
		if err == nil {
			t.Error("RequestLease() expected error for empty chunk handle")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("RequestLease() expected InvalidArgument error, got: %v", err)
		}
	})

	t.Run("Chunk not found", func(t *testing.T) {
		req := &chunk_pb.RequestLeaseRequest{
			ChunkHandle: &common.ChunkHandle{Handle: "nonexistent-chunk"},
			ServerId:    "server1",
		}

		resp, err := masterServer.RequestLease(context.Background(), req)
		if err != nil {
			t.Errorf("RequestLease() error = %v", err)
		}

		if resp.Status.Code != common.Status_ERROR {
			t.Errorf("RequestLease() expected error status, got: %v", resp.Status)
		}
		if resp.Status.Message != "chunk not found" {
			t.Errorf("RequestLease() expected 'chunk not found' error, got: %s", resp.Status.Message)
		}
		if resp.Granted {
			t.Error("RequestLease() expected lease to be denied")
		}
	})

	t.Run("Non-primary server request", func(t *testing.T) {
		chunkHandle := "chunk1"
		addChunkToMaster(masterServer.Master, chunkHandle)
		masterServer.Master.chunks[chunkHandle].Primary = "server1"
		masterServer.Master.chunks[chunkHandle].Locations = map[string]bool{"server2": true}

		req := &chunk_pb.RequestLeaseRequest{
			ChunkHandle: &common.ChunkHandle{Handle: chunkHandle},
			ServerId:    "server2",
		}

		resp, err := masterServer.RequestLease(context.Background(), req)
		if err != nil {
			t.Errorf("RequestLease() error = %v", err)
		}

		if resp.Status.Code != common.Status_ERROR {
			t.Errorf("RequestLease() expected error status, got: %v", resp.Status)
		}
		if resp.Status.Message != "only the primary server can request lease extension" {
			t.Errorf("RequestLease() unexpected error message: %s", resp.Status.Message)
		}
		if resp.Granted {
			t.Error("RequestLease() expected lease to be denied")
		}
	})

	t.Run("Successful lease extension", func(t *testing.T) {
		chunkHandle := "chunk4"
		serverId := "server4"
		addChunkToMaster(masterServer.Master, chunkHandle)
		masterServer.Master.chunks[chunkHandle].Primary = serverId
		masterServer.Master.chunks[chunkHandle].Locations = map[string]bool{serverId: true}
		
		// Add active server
		masterServer.Master.servers[serverId] = &ServerInfo{
			Status: "ACTIVE",
		}

		req := &chunk_pb.RequestLeaseRequest{
			ChunkHandle: &common.ChunkHandle{Handle: chunkHandle},
			ServerId:    serverId,
		}

		resp, err := masterServer.RequestLease(context.Background(), req)
		if err != nil {
			t.Errorf("RequestLease() error = %v", err)
		}

		if resp.Status.Code != common.Status_OK {
			t.Errorf("RequestLease() expected OK status, got: %v", resp.Status)
		}
		if !resp.Granted {
			t.Error("RequestLease() expected lease to be granted")
		}
		if resp.LeaseExpiration <= time.Now().Unix() {
			t.Error("RequestLease() lease expiration should be in the future")
		}
	})
}

func TestMasterServer_GetFileChunksInfo(t *testing.T) {
    // Setup
    masterServer := setupMasterServer(t)
    defer masterServer.Stop()

    // Add some active servers first
    serverID1 := "server1"
    serverID2 := "server2"
    serverID3 := "server3"
    
    // Create response channels for each server
    responseChannels := make(map[string]chan *chunk_pb.HeartBeatResponse)
    for _, serverID := range []string{serverID1, serverID2, serverID3} {
        responseChannels[serverID] = make(chan *chunk_pb.HeartBeatResponse, 10)
    }

    // Setup servers with active streams
    masterServer.Master.serversMu.Lock()
    masterServer.Master.servers = map[string]*ServerInfo{
        serverID1: {Status: "ACTIVE"},
        serverID2: {Status: "ACTIVE"},
        serverID3: {Status: "ACTIVE"},
    }
    masterServer.Master.serversMu.Unlock()

    // Register active streams for each server
    masterServer.Master.chunkServerMgr.mu.Lock()
    for serverID, ch := range responseChannels {
        masterServer.Master.chunkServerMgr.activeStreams[serverID] = ch
    }
    masterServer.Master.chunkServerMgr.mu.Unlock()

    t.Run("Valid file chunks info request with valid primaries", func(t *testing.T) {
        filename := "test.txt"
        chunks := []string{"chunk1", "chunk2", "chunk3"}
        addFileToMaster(masterServer.Master, filename, chunks)

        // Explicitly set primaries and locations for each chunk
        masterServer.Master.chunksMu.Lock()
        for _, chunk := range chunks {
            masterServer.Master.chunks[chunk] = &ChunkInfo{
                mu:              sync.RWMutex{},
                Primary:         serverID1,
                LeaseExpiration: time.Now().Add(time.Hour),
                Locations: map[string]bool{
                    serverID1: true,
                    serverID2: true,
                },
            }
        }
        masterServer.Master.chunksMu.Unlock()

        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: 0,
            EndChunk:   2,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        assert.NoError(t, err)
        assert.Equal(t, common.Status_OK, resp.Status.Code)
        assert.Equal(t, 3, len(resp.Chunks))

        for _, chunk := range resp.Chunks {
            assert.NotNil(t, chunk.PrimaryLocation)
            assert.Equal(t, serverID1, chunk.PrimaryLocation.ServerId)
            assert.Equal(t, 1, len(chunk.SecondaryLocations))
            assert.Equal(t, serverID2, chunk.SecondaryLocations[0].ServerId)
        }
    })

    t.Run("New chunk handle creation and replication verification", func(t *testing.T) {
        filename := "new_chunks.txt"
        
        // Reset server statuses to active
        masterServer.Master.serversMu.Lock()
        for _, server := range masterServer.Master.servers {
            server.Status = "ACTIVE"
        }
        masterServer.Master.serversMu.Unlock()

        // Add file to master without chunks
        addFileToMaster(masterServer.Master, filename, []string{})

        // Create a done channel to track INIT_EMPTY commands
        done := make(chan struct{})

        // Start goroutines to handle response channels
        for serverID, ch := range responseChannels {
            go func(sID string, respCh chan *chunk_pb.HeartBeatResponse) {
                select {
                case response := <-respCh:
                    // Verify INIT_EMPTY command
                    if len(response.Commands) > 0 && response.Commands[0].Type == chunk_pb.ChunkCommand_INIT_EMPTY {
                        // Simulate successful initialization
                        masterServer.Master.chunksMu.Lock()
                        chunkHandle := response.Commands[0].ChunkHandle.Handle
                        if chunk, exists := masterServer.Master.chunks[chunkHandle]; exists {
                            chunk.mu.Lock()
                            chunk.Locations[sID] = true
                            chunk.mu.Unlock()
                        }
                        masterServer.Master.chunksMu.Unlock()
                        done <- struct{}{}
                    }
                case <-time.After(time.Second):
                    // Timeout, do nothing
                }
            }(serverID, ch)
        }

        // Request chunk info which should trigger replication
        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: 0,
            EndChunk:   0,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        assert.NoError(t, err)
        assert.Equal(t, common.Status_OK, resp.Status.Code)

        // Wait for initialization to complete
        select {
        case <-done:
            // Success
        case <-time.After(2 * time.Second):
            t.Fatal("Timeout waiting for chunk initialization")
        }

        // Verify the response
        assert.NotEmpty(t, resp.Chunks)
        chunk, exists := resp.Chunks[0]
        assert.True(t, exists, "Chunk at index 0 should exist")
        assert.NotNil(t, chunk.PrimaryLocation)
        assert.NotEmpty(t, chunk.SecondaryLocations)

        // Verify chunk info in master
        masterServer.Master.filesMu.RLock()
        fileInfo := masterServer.Master.files[filename]
        fileInfo.mu.RLock()
        chunkHandle := fileInfo.Chunks[0]
        fileInfo.mu.RUnlock()
        masterServer.Master.filesMu.RUnlock()

        masterServer.Master.chunksMu.Lock()
        updatedChunk := masterServer.Master.chunks[chunkHandle]
        assert.NotEmpty(t, updatedChunk.Primary)
        assert.NotEmpty(t, updatedChunk.Locations)
        assert.Greater(t, updatedChunk.LeaseExpiration.Unix(), time.Now().Unix())
        masterServer.Master.chunksMu.Unlock()
    })

    // Clean up
    for _, ch := range responseChannels {
        close(ch)
    }
}

func TestMasterServer_CreateFile(t *testing.T) {
	// Setup
	masterServer := setupMasterServer(t)
	defer masterServer.Stop()

	t.Run("File already exists", func(t *testing.T) {
		// Add a file to the master
		filename := "test.txt"
		addFileToMaster(masterServer.Master, filename, []string{})

		req := &client_pb.CreateFileRequest{
			Filename: filename,
		}

		resp, err := masterServer.CreateFile(context.Background(), req)
		if err != nil {
			t.Errorf("CreateFile() error = %v", err)
		}

		if resp.Status.Code != common.Status_ERROR {
			t.Errorf("CreateFile() expected error, got status: %v", resp.Status)
		}

		if resp.Status.Message != ErrFileExists.Error() {
			t.Errorf("CreateFile() expected '%v' error, got: %s", ErrFileExists, resp.Status.Message)
		}
	})

	t.Run("Invalid filename", func(t *testing.T) {
		req := &client_pb.CreateFileRequest{
			Filename: "/invalid/path/test.txt",
		}

		resp, err := masterServer.CreateFile(context.Background(), req)
		if err != nil {
			t.Errorf("CreateFile() error = %v", err)
		}

		if resp.Status.Code != common.Status_ERROR {
			t.Errorf("CreateFile() expected error, got status: %v", resp.Status)
		}

		if resp.Status.Message != ErrInvalidFileName.Error() {
			t.Errorf("CreateFile() expected '%v' error, got: %s", ErrInvalidFileName, resp.Status.Message)
		}
	})
}

func TestMasterServer_DeleteFile(t *testing.T) {
    // Setup
    masterServer := setupMasterServer(t)
    defer masterServer.Stop()

    t.Run("Delete non-existent file", func(t *testing.T) {
        req := &client_pb.DeleteFileRequest{
            Filename: "nonexistent.txt",
        }

        resp, err := masterServer.DeleteFile(context.Background(), req)
        if err != nil {
            t.Errorf("DeleteFile() error = %v", err)
            return
        }

        if resp.Status.Code != common.Status_ERROR {
            t.Errorf("DeleteFile() expected error status, got: %v", resp.Status)
        }

        if !strings.Contains(resp.Status.Message, "file not found") {
            t.Errorf("DeleteFile() expected 'file not found' error, got: %s", 
                resp.Status.Message)
        }
    })

    t.Run("Delete file with invalid filename", func(t *testing.T) {
        req := &client_pb.DeleteFileRequest{
            Filename: "",  // Invalid empty filename
        }

        resp, err := masterServer.DeleteFile(context.Background(), req)
        if err != nil {
            t.Errorf("DeleteFile() error = %v", err)
            return
        }

        if resp.Status.Code != common.Status_ERROR {
            t.Errorf("DeleteFile() expected error status, got: %v", resp.Status)
        }

        if !strings.Contains(resp.Status.Message, "invalid filename") {
            t.Errorf("DeleteFile() expected 'invalid filename' error, got: %s", 
                resp.Status.Message)
        }
    })
}

func setupMasterServer(t *testing.T) *MasterServer {
	config, err := LoadConfig("../../configs/general-config.yml")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	masterServer, err := NewMasterServer("localhost:12345", config)
	if err != nil {
		t.Fatalf("Failed to create master server: %v", err)
	}

	return masterServer
}

func addChunkToMaster(master *Master, chunkHandle string) {
	master.chunksMu.Lock()
	defer master.chunksMu.Unlock()

	master.chunks[chunkHandle] = &ChunkInfo{
		Locations: make(map[string]bool),
	}
}

func addFileToMaster(master *Master, filename string, chunkHandles []string) {
    // Initialize maps if nil
    if master.files == nil {
        master.files = make(map[string]*FileInfo)
    }
    if master.chunks == nil {
        master.chunks = make(map[string]*ChunkInfo)
    }
    if master.deletedFiles == nil {
        master.deletedFiles = make(map[string]*DeletedFileInfo)
    }

    fileInfo := &FileInfo{
        Chunks: make(map[int64]string),
        mu:     sync.RWMutex{},
    }

    for i, handle := range chunkHandles {
        fileInfo.Chunks[int64(i)] = handle
    }

    master.chunksMu.Lock()
    for _, chunkHandle := range chunkHandles {
        master.chunks[chunkHandle] = &ChunkInfo{
            Locations: make(map[string]bool),
            Size:     0, // Set appropriate size if needed
            mu:       sync.RWMutex{},
        }
    }
    master.chunksMu.Unlock()

    master.filesMu.Lock()
    master.files[filename] = fileInfo
    master.filesMu.Unlock()
}

// MockHeartBeatServer implements chunk_pb.ChunkMasterService_HeartBeatServer
type MockHeartBeatServer struct {
    ctx context.Context
    recvChan chan *chunk_pb.HeartBeatRequest
    sendChan chan *chunk_pb.HeartBeatResponse
    grpcError error
}

func NewMockHeartBeatServer() *MockHeartBeatServer {
    return &MockHeartBeatServer{
        ctx:      context.Background(),
        recvChan: make(chan *chunk_pb.HeartBeatRequest, 10),
        sendChan: make(chan *chunk_pb.HeartBeatResponse, 10),
    }
}

func (m *MockHeartBeatServer) Send(resp *chunk_pb.HeartBeatResponse) error {
    if m.grpcError != nil {
        return m.grpcError
    }
    m.sendChan <- resp
    return nil
}

func (m *MockHeartBeatServer) Recv() (*chunk_pb.HeartBeatRequest, error) {
    if m.grpcError != nil {
        return nil, m.grpcError
    }
    req, ok := <-m.recvChan
    if !ok {
        return nil, io.EOF
    }
    return req, nil
}

func (m *MockHeartBeatServer) Context() context.Context {
    return m.ctx
}

// Add the missing RecvMsg method
func (m *MockHeartBeatServer) RecvMsg(msg interface{}) error {
    if m.grpcError != nil {
        return m.grpcError
    }
    req, ok := <-m.recvChan
    if !ok {
        return io.EOF
    }
    // Type assert msg as a pointer to HeartBeatRequest
    heartbeatMsg, ok := msg.(*chunk_pb.HeartBeatRequest)
    if !ok {
        return status.Error(codes.Internal, "failed to cast message to HeartBeatRequest")
    }
    // Copy the received request to the provided message
    *heartbeatMsg = *req
    return nil
}

func (m *MockHeartBeatServer) SendMsg(msg interface{}) error {
    if m.grpcError != nil {
        return m.grpcError
    }
    resp, ok := msg.(*chunk_pb.HeartBeatResponse)
    if !ok {
        return status.Error(codes.Internal, "failed to cast message to HeartBeatResponse")
    }
    m.sendChan <- resp
    return nil
}

func (m *MockHeartBeatServer) SetHeader(metadata.MD) error {
    return nil
}

func (m *MockHeartBeatServer) SendHeader(metadata.MD) error {
    return nil
}

func (m *MockHeartBeatServer) SetTrailer(metadata.MD) {
    // No-op implementation
}

// Add method to handle gRPC header information
func (m *MockHeartBeatServer) Header() (metadata.MD, error) {
    return metadata.MD{}, nil
}

// Add method to handle gRPC trailer information
func (m *MockHeartBeatServer) Trailer() metadata.MD {
    return metadata.MD{}
}

func TestMasterServer_HeartBeat(t *testing.T) {
    // Helper function to get a unique port for each test
    getUniquePort := func() int {
        listener, err := net.Listen("tcp", "localhost:0")
        if err != nil {
            t.Fatalf("Failed to find available port: %v", err)
        }
        defer listener.Close()
        return listener.Addr().(*net.TCPAddr).Port
    }

    // Helper function to create master server with unique port
    setupTestMasterServer := func() *MasterServer {
        config, err := LoadConfig("../../configs/general-config.yml")
        if err != nil {
            t.Fatalf("Failed to load config: %v", err)
        }
        
        port := getUniquePort()
        masterServer, err := NewMasterServer(fmt.Sprintf("localhost:%d", port), config)
        if err != nil {
            t.Fatalf("Failed to create master server: %v", err)
        }
        
        return masterServer
    }

    t.Run("Successful heartbeat stream", func(t *testing.T) {
        masterServer := setupTestMasterServer()
        defer masterServer.Stop()
        mockStream := NewMockHeartBeatServer()
        
        errChan := make(chan error, 1)
        go func() {
            err := masterServer.HeartBeat(mockStream)
            errChan <- err
        }()

        // Send initial heartbeat
        mockStream.recvChan <- &chunk_pb.HeartBeatRequest{
            ServerId:         "server1",
            AvailableSpace:   1024,
            CpuUsage:         50.0,
            ActiveOperations: 5,
        }

        // Wait for response
        select {
        case resp := <-mockStream.sendChan:
            if resp.Status.Code != common.Status_OK {
                t.Errorf("Expected OK status, got %v", resp.Status.Code)
            }
        case <-time.After(time.Second):
            t.Fatal("Timeout waiting for heartbeat response")
        }

        // Verify server registration
        masterServer.Master.serversMu.RLock()
        serverInfo, exists := masterServer.Master.servers["server1"]
        masterServer.Master.serversMu.RUnlock()

        if !exists {
            t.Error("Server was not registered")
        }
        if serverInfo.Status != "ACTIVE" {
            t.Errorf("Expected server status ACTIVE, got %s", serverInfo.Status)
        }
        if serverInfo.AvailableSpace != 1024 {
            t.Errorf("Expected available space 1024, got %d", serverInfo.AvailableSpace)
        }

        // Clean up
        close(mockStream.recvChan)
        select {
        case err := <-errChan:
            if err != io.EOF {
                t.Errorf("Expected EOF, got %v", err)
            }
        case <-time.After(time.Second):
            t.Fatal("Timeout waiting for stream to close")
        }
    })
}

func TestMasterServer_InitiateReplication(t *testing.T) {
    // Create master server
    config, err := LoadConfig("../../configs/general-config.yml")
    if err != nil {
        t.Fatalf("Failed to load config: %v", err)
    }

    // Create a test server without starting gRPC
    masterServer, err := NewMasterServer("localhost:0", config)
    if err != nil {
        t.Fatalf("Failed to create master server: %v", err)
    }

    // Add some active servers first
    serverID1 := "server1"
    serverID2 := "server2"
    serverID3 := "server3"
    serverID4 := "server4"
    serverID5 := "server5"
    
    masterServer.Master.serversMu.Lock()
    masterServer.Master.servers = map[string]*ServerInfo{
        serverID1: {
            Status: "ACTIVE",
            AvailableSpace: 2048,  // Add available space
            CPUUsage: 50.0,        // Add reasonable CPU usage
            ActiveOps: 5,    // Add reasonable operation count
        },
        serverID2: {
            Status: "ACTIVE",
            AvailableSpace: 2048,
            CPUUsage: 50.0,
            ActiveOps: 5,
        },
        serverID3: {
            Status: "ACTIVE",
            AvailableSpace: 2048,
            CPUUsage: 50.0,
            ActiveOps: 5,
        },
        serverID4: {
            Status: "ACTIVE",
            AvailableSpace: 2048,
            CPUUsage: 50.0,
            ActiveOps: 5,
        },
        serverID5: {
            Status: "ACTIVE",
            AvailableSpace: 2048,
            CPUUsage: 50.0,
            ActiveOps: 5,
        },
    }
    masterServer.Master.serversMu.Unlock()
    
    chunkHandle := "test-chunk"
    masterServer.Master.chunks[chunkHandle] = &ChunkInfo{
        Size:      1024,
        Locations: make(map[string]bool),
        mu:        sync.RWMutex{},
    }

    // Add existing replica - use serverID1 to match the map key
    masterServer.Master.chunks[chunkHandle].Locations[serverID1] = true

    // Test initiateReplication
    masterServer.Master.initiateReplication(chunkHandle)

    // Create mock stream to verify replication commands
    mockStream := NewMockHeartBeatServer()
    
    errChan := make(chan error, 1)
    go func() {
        err := masterServer.HeartBeat(mockStream)
        errChan <- err
    }()

    // Send heartbeat from server that should receive replication command
    mockStream.recvChan <- &chunk_pb.HeartBeatRequest{
        ServerId:         serverID1,  // Use consistent server ID
        AvailableSpace:   2048,       // Match the server info
        CpuUsage:         50.0,
        ActiveOperations: 5,
    }

    // Verify replication command in response
    select {
    case resp := <-mockStream.sendChan:
        if resp.Status.Code != common.Status_OK {
            t.Errorf("Expected OK status, got %v", resp.Status.Code)
        }
        found := false
        for _, cmd := range resp.Commands {
            if cmd.Type == chunk_pb.ChunkCommand_REPLICATE && cmd.ChunkHandle.Handle == chunkHandle {
                found = true
                if len(cmd.TargetLocations) != 2 {
                    t.Errorf("Expected 2 target locations, got %d", len(cmd.TargetLocations))
                }
                // Verify the target locations don't include the existing replica
                for _, target := range cmd.TargetLocations {
                    if target.ServerId == serverID1 {
                        t.Errorf("Target locations should not include existing replica server")
                    }
                }
            }
        }
        if !found {
            t.Error("Expected to find replication command in response")
        }
    case <-time.After(time.Second):
        t.Fatal("Timeout waiting for heartbeat response")
    }

    // Clean up
    close(mockStream.recvChan)
    select {
    case err := <-errChan:
        if err != io.EOF {
            t.Errorf("Expected EOF, got %v", err)
        }
    case <-time.After(time.Second):
        t.Fatal("Timeout waiting for stream to close")
    }
}

func TestMasterServer_CleanupExpiredOperations(t *testing.T) {
    // Create master server without starting gRPC
    config, err := LoadConfig("../../configs/general-config.yml")
    if err != nil {
        t.Fatalf("Failed to load config: %v", err)
    }

    masterServer, err := NewMasterServer("localhost:0", config)
    if err != nil {
        t.Fatalf("Failed to create master server: %v", err)
    }

    serverId := "test-server"
    chunkHandle := "test-chunk"

    // Add operations with different ages and attempt counts
    masterServer.Master.pendingOpsMu.Lock()
    masterServer.Master.pendingOps[serverId] = []*PendingOperation{
        {
            Type:         chunk_pb.ChunkCommand_REPLICATE,
            ChunkHandle:  chunkHandle,
            AttemptCount: 6, // Should be cleaned up
            CreatedAt:    time.Now(),
        },
        {
            Type:         chunk_pb.ChunkCommand_REPLICATE,
            ChunkHandle:  chunkHandle + "-2",
            AttemptCount: 2,
            CreatedAt:    time.Now().Add(-2 * time.Hour), // Should be cleaned up
        },
        {
            Type:         chunk_pb.ChunkCommand_REPLICATE,
            ChunkHandle:  chunkHandle + "-3",
            AttemptCount: 1,
            CreatedAt:    time.Now(), // Should remain
        },
    }
    masterServer.Master.pendingOpsMu.Unlock()

    // Create mock stream
    mockStream := NewMockHeartBeatServer()
    
    errChan := make(chan error, 1)
    go func() {
        err := masterServer.HeartBeat(mockStream)
        errChan <- err
    }()

    // Run cleanup
    masterServer.Master.cleanupExpiredOperations()

    // Send heartbeat to check remaining operations
    mockStream.recvChan <- &chunk_pb.HeartBeatRequest{
        ServerId:         serverId,
        AvailableSpace:   1024,
        CpuUsage:         50.0,
        ActiveOperations: 5,
    }

    // Verify only non-expired operations in response
    select {
    case resp := <-mockStream.sendChan:
        if resp.Status.Code != common.Status_OK {
            t.Errorf("Expected OK status, got %v", resp.Status.Code)
        }
        if len(resp.Commands) != 1 {
            t.Errorf("Expected 1 command, got %d", len(resp.Commands))
        }
        if len(resp.Commands) > 0 && resp.Commands[0].ChunkHandle.Handle != chunkHandle+"-3" {
            t.Errorf("Expected remaining operation for chunk %s, got %s", 
                chunkHandle+"-3", resp.Commands[0].ChunkHandle.Handle)
        }
    case <-time.After(time.Second):
        t.Fatal("Timeout waiting for heartbeat response")
    }

    // Clean up
    close(mockStream.recvChan)
    select {
    case err := <-errChan:
        if err != io.EOF {
            t.Errorf("Expected EOF, got %v", err)
        }
    case <-time.After(time.Second):
        t.Fatal("Timeout waiting for stream to close")
    }
}

func NewMockMaster(config *Config) *Master {
    return &Master{
        Config: config,
        deletedFiles: make(map[string]*DeletedFileInfo),
        files: make(map[string]*FileInfo),
        chunks: make(map[string]*ChunkInfo),
        servers: make(map[string]*ServerInfo),
        pendingOps: make(map[string][]*PendingOperation),
        chunkServerMgr: &ChunkServerManager{
            activeStreams: make(map[string]chan *chunk_pb.HeartBeatResponse),
        },
    }
}

// NewTestMasterServer creates a MasterServer instance for testing
func NewTestMasterServer(addr string, config *Config) (*MasterServer, error) {
    server := &MasterServer{
        Master: NewMockMaster(config),
        grpcServer: grpc.NewServer(),
    }

    chunk_pb.RegisterChunkMasterServiceServer(server.grpcServer, server)
    return server, nil
}
