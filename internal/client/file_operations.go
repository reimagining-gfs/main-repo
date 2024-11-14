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


func (c *Client) PushDataToPrimary(ctx context.Context, chunkHandle string, data []byte) (string, error) {
    // Get chunk information from cache
    c.chunkCacheMu.RLock()
    chunkInfo, exists := c.chunkHandleCache[chunkHandle]
    c.chunkCacheMu.RUnlock()

    if !exists {
        return "", fmt.Errorf("chunk information not found in cache for handle, request again: %s", chunkHandle)
    }

    if chunkInfo.PrimaryLocation == nil {
        return "", fmt.Errorf("primary location not found for chunk: %s", chunkHandle)
    }

    conn, err := grpc.Dial(chunkInfo.PrimaryLocation.ServerAddress, grpc.WithInsecure())
    if err != nil {
        return "", fmt.Errorf("failed to connect to primary server: %v", err)
    }
    defer conn.Close()

    client := chunk_ops.NewChunkOperationServiceClient(conn)

    checksum := crc32.ChecksumIEEE(data)
    operationId := uuid.New().String() // Generate unique operation ID

    req := &chunk_ops.PushDataToPrimaryRequest{
        ChunkHandle: &common_pb.ChunkHandle{
            Handle: chunkHandle,
        },
        Data:              data,
        Checksum:          checksum,
        OperationId:       operationId,
        SecondaryLocations: chunkInfo.SecondaryLocations,
    }

    resp, err := client.PushDataToPrimary(ctx, req)
    if err != nil {
        return "", fmt.Errorf("failed to push data: %v", err)
    }

    if resp.Status.Code != common_pb.Status_OK {
        return "", fmt.Errorf("push data failed: %s", resp.Status.Message)
    }

    return operationId, nil
}

func (c *Client) Read(ctx context.Context, filename string, offset int64, length int64) ([]byte, error) {
    if length <= 0 {
        return nil, fmt.Errorf("invalid read length: %d", length)
    }

    startChunk := offset / ChunkSize
    endChunk := (offset + length - 1) / ChunkSize

    chunks, err := c.GetChunkInfo(ctx, filename, startChunk, endChunk)
    if err != nil {
        return nil, fmt.Errorf("failed to get chunk info: %v", err)
    }

    result := make([]byte, 0, length)
    remainingLength := length
    currentOffset := offset

    for i := startChunk; i <= endChunk; i++ {
        chunkInfo, ok := chunks[i]
        if !ok {
            return nil, fmt.Errorf("chunk info missing for index %d", i)
        }

        chunkOffset := currentOffset % ChunkSize

        bytesToRead := ChunkSize - chunkOffset
        if bytesToRead > remainingLength {
            bytesToRead = remainingLength
        }

        var serverAddr string
        if chunkInfo.PrimaryLocation != nil {
            serverAddr = chunkInfo.PrimaryLocation.ServerAddress
        } else if len(chunkInfo.SecondaryLocations) > 0 {
            serverAddr = chunkInfo.SecondaryLocations[0].ServerAddress
        } else {
            return nil, fmt.Errorf("no available servers for chunk %s", chunkInfo.ChunkHandle.Handle)
        }

        conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
        if err != nil {
            return nil, fmt.Errorf("failed to connect to chunk server: %v", err)
        }
        defer conn.Close()

        client := chunk_ops.NewChunkOperationServiceClient(conn)

        readReq := &chunk_ops.ReadChunkRequest{
            ChunkHandle: chunkInfo.ChunkHandle,
            Offset:     chunkOffset,
            Length:     bytesToRead,
        }

        // Execute read request
        readResp, err := client.ReadChunk(ctx, readReq)
        if err != nil {
            return nil, fmt.Errorf("failed to read from chunk %s: %v", 
                chunkInfo.ChunkHandle.Handle, err)
        }

        if readResp.Status.Code != common_pb.Status_OK {
            return nil, fmt.Errorf("read chunk failed: %s", readResp.Status.Message)
        }

        result = append(result, readResp.Data...)

        bytesRead := int64(len(readResp.Data))
        remainingLength -= bytesRead
        currentOffset += bytesRead

        if remainingLength <= 0 {
            break
        }
    }

    return result, nil
}

func (c *Client) Write(ctx context.Context, filename string, offset int64, data []byte) (int, error) {
    // Get chunk information from the master
    startChunk := offset / ChunkSize
    endChunk := (offset + int64(len(data))) / ChunkSize
    chunks, err := c.GetChunkInfo(ctx, filename, startChunk, endChunk)
    if err != nil {
        return 0, fmt.Errorf("failed to get chunk info: %v", err)
    }

    totalWritten := 0
    remainingData := data
    currentOffset := offset

    // Process each chunk
    for i := startChunk; i <= endChunk; i++ {
        chunkInfo, ok := chunks[i]
        if !ok {
            return totalWritten, fmt.Errorf("chunk info missing for index %d", i)
        }

        // Calculate the offset within this chunk
        chunkOffset := currentOffset % ChunkSize

        var chunkData []byte
        bytesRemaining := ChunkSize - chunkOffset
        if int64(len(remainingData)) > bytesRemaining {
            chunkData = remainingData[:bytesRemaining]
            remainingData = remainingData[bytesRemaining:]
        } else {
            chunkData = remainingData
            remainingData = nil
        }

        operationId, err := c.PushDataToPrimary(ctx, chunkInfo.ChunkHandle.Handle, chunkData)
        if err != nil {
            return totalWritten, fmt.Errorf("failed to push data to primary for chunk %s: %v", 
                chunkInfo.ChunkHandle.Handle, err)
        }

        conn, err := grpc.Dial(chunkInfo.PrimaryLocation.ServerAddress, grpc.WithInsecure())
        if err != nil {
            return totalWritten, fmt.Errorf("failed to connect to primary server: %v", err)
        }
        defer conn.Close()

        writeReq := &chunk_ops.WriteChunkRequest{
            ChunkHandle: chunkInfo.ChunkHandle,
            Offset:     chunkOffset,
            Secondaries: chunkInfo.SecondaryLocations,
            OperationId: operationId,
        }

        client := chunk_ops.NewChunkOperationServiceClient(conn)
        writeResp, err := client.WriteChunk(ctx, writeReq)
        if err != nil {
            return totalWritten, fmt.Errorf("failed to write chunk %s: %v", 
                chunkInfo.ChunkHandle.Handle, err)
        }

        if writeResp.Status.Code != common_pb.Status_OK {
            return totalWritten, fmt.Errorf("write chunk failed: %s", writeResp.Status.Message)
        }

        bytesWritten := len(chunkData)
        totalWritten += bytesWritten
        currentOffset += int64(bytesWritten)

        if len(remainingData) == 0 {
            break
        }
    }

    return totalWritten, nil
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