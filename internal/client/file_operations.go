package client

import (
    "context"
    "fmt"
    "hash/crc32"

    chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
    common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"

    "github.com/google/uuid"
    "google.golang.org/grpc"
)

const ChunkSize = 1 * 1024 * 1024 // 1MB

func (c *Client) PushDataToPrimary(ctx context.Context, chunkHandle string, data []byte) error {
    // Get chunk information from cache
    c.chunkCacheMu.RLock()
    chunkInfo, exists := c.chunkHandleCache[chunkHandle]
    c.chunkCacheMu.RUnlock()

    if !exists {
        return fmt.Errorf("chunk information not found in cache for handle, request again: %s", chunkHandle)
    }

    if chunkInfo.PrimaryLocation == nil {
        return fmt.Errorf("primary location not found for chunk: %s", chunkHandle)
    }

    conn, err := grpc.Dial(chunkInfo.PrimaryLocation.ServerAddress, grpc.WithInsecure())
    if err != nil {
        return fmt.Errorf("failed to connect to primary server: %v", err)
    }
    defer conn.Close()

    client := chunk_ops.NewChunkOperationServiceClient(conn)

    checksum := crc32.ChecksumIEEE(data)

    req := &chunk_ops.PushDataToPrimaryRequest{
        ChunkHandle: &common_pb.ChunkHandle{
            Handle: chunkHandle,
        },
        Data:              data,
        Checksum:          checksum,
        OperationId:       uuid.New().String(), // Generate unique operation ID
        SecondaryLocations: chunkInfo.SecondaryLocations,
    }

    resp, err := client.PushDataToPrimary(ctx, req)
    if err != nil {
        return fmt.Errorf("failed to push data: %v", err)
    }

    if resp.Status.Code != common_pb.Status_OK {
        return fmt.Errorf("push data failed: %s", resp.Status.Message)
    }

    return nil
}

func (fh *FileHandle) Read(handle string, offset int64, length int64) ([]byte, error) {
    if length <= 0 {
        return nil, fmt.Errorf("invalid read length: %d", length)
    }

    // Create a buffer to store the read data
    // buffer := make([]byte, length)

    // TODO: Implement connection pooling to chunk servers
    // Connect to chunk server and read data
    // For now, return unimplemented error
    return nil, fmt.Errorf("read operation not yet implemented")
}

func (fh *FileHandle) Write(primary string, secondaries []string, offset int64, data []byte) (int, error) {
    if len(data) == 0 {
        return 0, nil
    }

    if offset < 0 {
        return 0, fmt.Errorf("invalid write offset: %d", offset)
    }

    // Ensure write doesn't exceed chunk boundaries
    if offset+int64(len(data)) > ChunkSize {
        return 0, fmt.Errorf("write would exceed chunk size")
    }

    // Create write operation
    // writeOp := &WriteOperation{
    //     Primary:     primary,
    //     Secondaries: secondaries,
    //     Offset:      offset,
    //     Data:        data,
    // }

    // TODO: Implement connection pooling to chunk servers
    // Connect to primary chunk server and initiate write operation
    // Primary will coordinate with secondaries for replication
    // For now, return unimplemented error
    return 0, fmt.Errorf("write operation not yet implemented")
}

// Supporting types for write operations
type WriteOperation struct {
    Primary     string   // Primary chunk server handle
    Secondaries []string // Secondary chunk server handles
    Offset      int64    // Write offset within chunk
    Data        []byte   // Data to be written
}

// // Helper method to check if current position is at end of file
// func (fh *FileHandle) isEOF() bool {
//     fh.mu.RLock()
//     defer fh.mu.RUnlock()
//     return fh.position >= fh.size
// }

// // Seek sets the offset for the next Read or Write on file to offset, interpreted
// // according to whence: 0 means relative to the origin of the file, 1 means
// // relative to the current offset, and 2 means relative to the end.
// func (fh *FileHandle) Seek(offset int64, whence int) (int64, error) {
//     fh.mu.Lock()
//     defer fh.mu.Unlock()

//     var abs int64
//     switch whence {
//     case io.SeekStart:
//         abs = offset
//     case io.SeekCurrent:
//         abs = fh.position + offset
//     case io.SeekEnd:
//         abs = fh.size + offset
//     default:
//         return 0, fmt.Errorf("invalid whence: %d", whence)
//     }

//     if abs < 0 {
//         return 0, fmt.Errorf("negative position: %d", abs)
//     }

//     fh.position = abs
//     return abs, nil
// }