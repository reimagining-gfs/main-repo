package chunkserver

import (
    "context"
    "fmt"
    "hash/crc32"
    "io"
    "log"
    "sync"
    "time"

    chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
    chunkserver_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk"
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
                Message: "primary's lease has expired",
            },
        }, nil
    }

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

    chunk := &chunkserver_pb.DataChunk{
        OperationId: operationID,
        ChunkHandle: chunkHandle,
        Data:       data,
        Checksum:   checksum,
        Offset:     0,
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