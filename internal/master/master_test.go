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

		resp, err := masterServer.ReportChunk(context.Background(), req)
		if err != nil {
			t.Errorf("ReportChunk() error = %v", err)
		}

		if resp.Status.Code != common.Status_OK {
			t.Errorf("ReportChunk() returned non-OK status: %v", resp.Status)
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

		resp, err := masterServer.ReportChunk(context.Background(), req)
		
		// Check if the error is returned
		if err == nil {
			t.Error("ReportChunk() expected error, got nil")
		}

		// Verify it's the correct gRPC error
		status, ok := status.FromError(err)
		if !ok {
			t.Error("Expected gRPC status error")
		}

		// Check error code and message
		if status.Code() != codes.InvalidArgument {
			t.Errorf("Expected InvalidArgument error code, got %v", status.Code())
		}

		if status.Message() != "server_id is required" {
			t.Errorf("Expected 'server_id is required' message, got %s", status.Message())
		}

		// Response should be nil since we got an error
		if resp != nil {
			t.Error("Expected nil response when error is returned")
		}
	})
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

	t.Run("Inactive server", func(t *testing.T) {
		chunkHandle := "chunk2"
		serverId := "inactive-server"
		addChunkToMaster(masterServer.Master, chunkHandle)
		masterServer.Master.chunks[chunkHandle].Primary = serverId
		masterServer.Master.chunks[chunkHandle].Locations = map[string]bool{serverId: true}
		
		// Add inactive server
		masterServer.Master.servers[serverId] = &ServerInfo{
			Status: "INACTIVE",
		}

		req := &chunk_pb.RequestLeaseRequest{
			ChunkHandle: &common.ChunkHandle{Handle: chunkHandle},
			ServerId:    serverId,
		}

		resp, err := masterServer.RequestLease(context.Background(), req)
		if err != nil {
			t.Errorf("RequestLease() error = %v", err)
		}

		if resp.Status.Code != common.Status_ERROR {
			t.Errorf("RequestLease() expected error status, got: %v", resp.Status)
		}
		if resp.Status.Message != "server is not active" {
			t.Errorf("RequestLease() unexpected error message: %s", resp.Status.Message)
		}
		if resp.Granted {
			t.Error("RequestLease() expected lease to be denied")
		}
	})

	t.Run("Server no longer has chunk", func(t *testing.T) {
		chunkHandle := "chunk3"
		serverId := "server3"
		addChunkToMaster(masterServer.Master, chunkHandle)
		masterServer.Master.chunks[chunkHandle].Primary = serverId
		masterServer.Master.chunks[chunkHandle].Locations = map[string]bool{} // Empty locations
		
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

		if resp.Status.Code != common.Status_ERROR {
			t.Errorf("RequestLease() expected error status, got: %v", resp.Status)
		}
		if resp.Status.Message != "server no longer holds chunk data" {
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
    masterServer.Master.serversMu.Lock()
    masterServer.Master.servers = map[string]*ServerInfo{
        serverID1: {
            Status: "ACTIVE",
        },
        serverID2: {
            Status: "ACTIVE",
        },
        serverID3: {
            Status: "ACTIVE",
        },
    }
    masterServer.Master.serversMu.Unlock()

    t.Run("Valid file chunks info request with valid primaries", func(t *testing.T) {
        filename := "test.txt"
        chunks := []string{"chunk1", "chunk2", "chunk3"}
        addFileToMaster(masterServer.Master, filename, chunks)

        // Explicitly set primaries and locations for each chunk
        masterServer.Master.chunksMu.Lock()
        for _, chunk := range chunks {
            masterServer.Master.chunks[chunk] = &ChunkInfo{
                mu:              sync.RWMutex{},
                Primary:        serverID1,
                LeaseExpiration: time.Now().Add(time.Hour),
                Locations:      map[string]bool{
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

        // Verify each chunk has a primary and secondary locations
        for _, chunk := range resp.Chunks {
            assert.NotNil(t, chunk.PrimaryLocation)
            assert.Equal(t, serverID1, chunk.PrimaryLocation.ServerId)
            assert.Equal(t, 1, len(chunk.SecondaryLocations))
            assert.Equal(t, serverID2, chunk.SecondaryLocations[0].ServerId)
        }
    })

    t.Run("Chunks with expired lease trigger primary reassignment", func(t *testing.T) {
        filename := "expired_lease.txt"
        chunkHandle := "chunk4"
        addFileToMaster(masterServer.Master, filename, []string{chunkHandle})

        // Set up chunk with expired lease and locations
        masterServer.Master.chunksMu.Lock()
        masterServer.Master.chunks[chunkHandle] = &ChunkInfo{
            mu:              sync.RWMutex{},
            Primary:        serverID1,
            LeaseExpiration: time.Now().Add(-time.Hour),
            Locations:      map[string]bool{
                serverID1: true,
                serverID2: true,
            },
        }
        masterServer.Master.chunksMu.Unlock()

        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: 0,
            EndChunk:   0,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        assert.NoError(t, err)
        assert.Equal(t, common.Status_OK, resp.Status.Code)
        assert.Equal(t, 1, len(resp.Chunks))

        // Verify new primary was assigned
        chunk := resp.Chunks[0]
        assert.NotNil(t, chunk.PrimaryLocation)
        assert.NotEmpty(t, chunk.PrimaryLocation.ServerId)
        assert.NotEmpty(t, chunk.SecondaryLocations)
    })

    t.Run("Chunks with no primary trigger primary assignment", func(t *testing.T) {
        filename := "no_primary.txt"
        chunkHandle := "chunk5"

        addFileToMaster(masterServer.Master, filename, []string{chunkHandle})
        
        // Properly initialize the chunk with locations but no primary
        masterServer.Master.chunksMu.Lock()
        masterServer.Master.chunks[chunkHandle] = &ChunkInfo{
            mu:              sync.RWMutex{},
            Primary:        "",  // No primary initially
            LeaseExpiration: time.Time{},
            Locations:      map[string]bool{
                serverID1: true,
                serverID2: true,
            },
        }
        masterServer.Master.chunksMu.Unlock()

        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: 0,
            EndChunk:   0,
        }


        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        
        assert.NoError(t, err)
        assert.Equal(t, common.Status_OK, resp.Status.Code)
        
        // Check that we got chunks back
        assert.NotEmpty(t, resp.Chunks, "Response should contain chunks")
        
        // Get the chunk at index 0
        chunk, exists := resp.Chunks[0]
        assert.True(t, exists, "Chunk at index 0 should exist")
        assert.NotNil(t, chunk, "Chunk should not be nil")
        
        // Verify the chunk has a primary assigned
        assert.NotNil(t, chunk.PrimaryLocation, "Chunk should have a primary location")
        assert.NotEmpty(t, chunk.PrimaryLocation.ServerId, "Primary location should have a server ID")
        
        // Verify we have secondary locations
        assert.NotEmpty(t, chunk.SecondaryLocations, "Chunk should have secondary locations")
        
        // Verify the primary is not in the secondary locations
        primaryID := chunk.PrimaryLocation.ServerId
        for _, loc := range chunk.SecondaryLocations {
            assert.NotEqual(t, primaryID, loc.ServerId, "Primary should not be in secondary locations")
        }
    })

    t.Run("No available servers for primary assignment", func(t *testing.T) {
        filename := "no_servers.txt"
        addFileToMaster(masterServer.Master, filename, []string{"chunk6"})

        // Make all servers inactive
        masterServer.Master.serversMu.Lock()
        for _, server := range masterServer.Master.servers {
            server.Status = "INACTIVE"
        }
        masterServer.Master.serversMu.Unlock()

        // Remove primary
        masterServer.Master.chunksMu.Lock()
        masterServer.Master.chunks["chunk6"].Primary = ""
        masterServer.Master.chunksMu.Unlock()

        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: 0,
            EndChunk:   0,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        assert.NoError(t, err)
        assert.Equal(t, common.Status_ERROR, resp.Status.Code)
        assert.Contains(t, resp.Status.Message, "No available chunk servers with valid primaries")
    })

    t.Run("File not found", func(t *testing.T) {
        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   "nonexistent.txt",
            StartChunk: 0,
            EndChunk:   0,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        assert.NoError(t, err)
        assert.Equal(t, common.Status_ERROR, resp.Status.Code)
        assert.Equal(t, ErrFileNotFound.Error(), resp.Status.Message)
    })

    t.Run("Invalid chunk range", func(t *testing.T) {
        filename := "test.txt"
        addFileToMaster(masterServer.Master, filename, []string{"chunk1", "chunk2", "chunk3"})

        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: -1,
            EndChunk:   3,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        assert.NoError(t, err)
        assert.Equal(t, common.Status_ERROR, resp.Status.Code)
        assert.Equal(t, ErrInvalidChunkRange.Error(), resp.Status.Message)
    })
    
    t.Run("New chunk handle creation and replication verification", func(t *testing.T) {
        filename := "new_chunks.txt"

        
        for _, server := range masterServer.Master.servers {
            server.Status = "ACTIVE"
        }

        // Add file to master but don't initialize the chunk info
        addFileToMaster(masterServer.Master, filename, []string{})
        
        // Request chunk info which should trigger replication
        req := &client_pb.GetFileChunksInfoRequest{
            Filename:   filename,
            StartChunk: 0,
            EndChunk:   0,
        }

        resp, err := masterServer.GetFileChunksInfo(context.Background(), req)
        
        // Verify the response
        assert.NoError(t, err)
        assert.Equal(t, common.Status_OK, resp.Status.Code)
        assert.Equal(t, 1, len(resp.Chunks))

        // Get the chunk info from response - resp.Chunks is a map[int64]*ChunkInfo
        responseChunk, exists := resp.Chunks[0]  // get chunk at index 0
        assert.True(t, exists, "Chunk at index 0 should exist")
        
        // Verify that a primary was assigned
        assert.NotNil(t, responseChunk.PrimaryLocation)
        assert.NotEmpty(t, responseChunk.PrimaryLocation.ServerId)
        
        // Verify that we have secondary locations
        assert.NotEmpty(t, responseChunk.SecondaryLocations)
        assert.GreaterOrEqual(t, len(responseChunk.SecondaryLocations), 1, "Should have at least one secondary location")

        // Verify that the primary is not in secondary locations
        primaryID := responseChunk.PrimaryLocation.ServerId
        for _, loc := range responseChunk.SecondaryLocations {
            assert.NotEqual(t, primaryID, loc.ServerId, "Primary should not be in secondary locations")
        }

        // Verify the chunk info was properly updated in the master
        masterServer.Master.filesMu.RLock()
        fileInfo := masterServer.Master.files[filename]
        fileInfo.mu.RLock()
        chunkHandle := fileInfo.Chunks[0]  // Get the first chunk's handle
        fileInfo.mu.RUnlock()
        masterServer.Master.filesMu.RUnlock()
        masterServer.Master.chunksMu.Lock()
        updatedChunk := masterServer.Master.chunks[chunkHandle]
        assert.NotEmpty(t, updatedChunk.Primary, "Chunk should have a primary assigned")
        assert.NotEmpty(t, updatedChunk.Locations, "Chunk should have locations assigned")
        assert.Greater(t, updatedChunk.LeaseExpiration.Unix(), time.Now().Unix(), "Lease should be set in the future")
        masterServer.Master.chunksMu.Unlock()
    })
}

func TestMasterServer_CreateFile(t *testing.T) {
	// Setup
	masterServer := setupMasterServer(t)
	defer masterServer.Stop()

	t.Run("Create new file", func(t *testing.T) {
		req := &client_pb.CreateFileRequest{
			Filename: "test.txt",
		}

		resp, err := masterServer.CreateFile(context.Background(), req)
		if err != nil {
			t.Errorf("CreateFile() error = %v", err)
		}

		if resp.Status.Code != common.Status_OK {
			t.Errorf("CreateFile() returned non-OK status: %v", resp.Status)
		}
	})

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

    t.Run("Delete existing file", func(t *testing.T) {
        // First create a file using the actual CreateFile method
        filename := "test.txt"
        createReq := &client_pb.CreateFileRequest{
            Filename: filename,
        }

        createResp, err := masterServer.CreateFile(context.Background(), createReq)
        if err != nil {
            t.Fatalf("Failed to create test file: %v", err)
        }
        if createResp.Status.Code != common.Status_OK {
            t.Fatalf("CreateFile failed with status: %v", createResp.Status)
        }

        // Verify file exists before deletion
        masterServer.Master.filesMu.RLock()
        _, exists := masterServer.Master.files[filename]
        masterServer.Master.filesMu.RUnlock()
        if !exists {
            t.Fatal("Test setup failed: file not created")
        }

        // // Now test the delete operation
        deleteReq := &client_pb.DeleteFileRequest{
            Filename: filename,
        }

        deleteResp, err := masterServer.DeleteFile(context.Background(), deleteReq)
        if err != nil {
            t.Errorf("DeleteFile() error = %v", err)
        }

        if deleteResp.Status.Code != common.Status_OK {
            t.Errorf("DeleteFile() returned non-OK status: %v", deleteResp.Status)
        }

        // Verify file deletion
        masterServer.Master.filesMu.RLock()
        _, exists = masterServer.Master.files[filename]
        masterServer.Master.filesMu.RUnlock()
        if exists {
            t.Errorf("DeleteFile() failed: file %s still exists", filename)
        }

        // Verify file is moved to deletedFiles
        trashDirPrefix := "/.trash/"
        trashPath := fmt.Sprintf("%s%s", trashDirPrefix, filename)

        masterServer.Master.deletedFilesMu.RLock()
        deletedFile, exists := masterServer.Master.deletedFiles[trashPath]
        masterServer.Master.deletedFilesMu.RUnlock()
        if !exists {
            t.Errorf("DeleteFile() failed: file %s not moved to deletedFiles", filename)
        }
        if deletedFile.OriginalPath != filename {
            t.Errorf("DeleteFile() incorrect original path: got %s, want %s", 
                deletedFile.OriginalPath, filename)
        }
    })

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