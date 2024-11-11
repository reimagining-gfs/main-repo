package client

import (
    "context"
    "fmt"
    "time"

    client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
    common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
    "google.golang.org/grpc"
)

func NewClient(configPath string) (*Client, error) {
    config, err := LoadConfig(configPath)
    if err != nil {
        return nil, fmt.Errorf("failed to load config: %v", err)
    }

    // Convert the loaded config to ClientConfig
    clientConfig := config.ToClientConfig()

    ctx, cancel := context.WithTimeout(context.Background(), 
        time.Duration(config.Connection.Master.Timeout)*time.Second)
    defer cancel()

    // Create gRPC connection to master
    conn, err := grpc.DialContext(ctx, 
        fmt.Sprintf("%s:%d", config.Connection.Master.Host, config.Connection.Master.Port), 
        grpc.WithInsecure())
    if err != nil {
        return nil, fmt.Errorf("failed to connect to master: %v", err)
    }

    return &Client{
        config:     clientConfig,
        conn:       conn,
        client:     client_pb.NewClientMasterServiceClient(conn),
        chunkCache: make(map[string]*ChunkLocationCache),
        activeOps:  make(map[string]*Operation),
    }, nil
}

func (c *Client) Close() error {
    return c.conn.Close()
}

func (c *Client) Create(ctx context.Context, filename string) error {
    req := &client_pb.CreateFileRequest{
        Filename: filename,
    }

    resp, err := c.client.CreateFile(ctx, req)
    if err != nil {
        return fmt.Errorf("failed to create file: %v", err)
    }

    if resp.Status.Code != common_pb.Status_OK {
        return fmt.Errorf("create file failed: %s", resp.Status.Message)
    }

    return nil
}

func (c *Client) Open(filename string, readOnly bool) (*FileHandle, error) {
    return &FileHandle{
        client:   c,
        filename: filename,
        readOnly: readOnly,
        position: 0,
    }, nil
}

func (c *Client) Delete(ctx context.Context, filename string) error {
    req := &client_pb.DeleteFileRequest{
        Filename: filename,
    }

    resp, err := c.client.DeleteFile(ctx, req)
    if err != nil {
        return fmt.Errorf("failed to delete file: %v", err)
    }

    if resp.Status.Code != common_pb.Status_OK {
        return fmt.Errorf("delete file failed: %s", resp.Status.Message)
    }

    return nil
}

func (c *Client) getChunkInfo(ctx context.Context, filename string, chunkIndex int64) (*client_pb.ChunkInfo, error) {
    cacheKey := fmt.Sprintf("%s-%d", filename, chunkIndex)

    c.chunkCacheMu.RLock()
    if cached, ok := c.chunkCache[cacheKey]; ok {
        if time.Now().Before(cached.ExpiresAt) {
            c.chunkCacheMu.RUnlock()
            return cached.Info, nil
        }
    }
    c.chunkCacheMu.RUnlock()

    req := &client_pb.GetFileChunksInfoRequest{
        Filename:   filename,
        StartChunk: chunkIndex,
        EndChunk:   chunkIndex,
    }

    resp, err := c.client.GetFileChunksInfo(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("failed to get chunk info: %v", err)
    }

    if resp.Status.Code != common_pb.Status_OK {
        return nil, fmt.Errorf("get chunk info failed: %s", resp.Status.Message)
    }

    chunkInfo, ok := resp.Chunks[chunkIndex]
    if !ok {
        return nil, fmt.Errorf("chunk index %d not found", chunkIndex)
    }

    c.chunkCacheMu.Lock()
    c.chunkCache[cacheKey] = &ChunkLocationCache{
        Info:      chunkInfo,
        ExpiresAt: time.Now().Add(c.config.Cache.ChunkTTL),
    }
    c.chunkCacheMu.Unlock()

    return chunkInfo, nil
}
