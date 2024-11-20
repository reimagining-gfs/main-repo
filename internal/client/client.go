package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
	"google.golang.org/grpc"
)

func (c *Client) cleanupChunkCache(filename string) {
	c.chunkCacheMu.Lock()
	defer c.chunkCacheMu.Unlock()

	for cacheKey := range c.chunkCache {
		if strings.HasPrefix(cacheKey, filename+"-") {
			delete(c.chunkCache, cacheKey)
		}
	}
}

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
		config:           clientConfig,
		conn:             conn,
		client:           client_pb.NewClientMasterServiceClient(conn),
		chunkCache:       make(map[string]*ChunkLocationCache),
		chunkHandleCache: make(map[string]*client_pb.ChunkInfo),
		activeOps:        make(map[string]*Operation),
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

func (c *Client) Rename(ctx context.Context, old_filename, new_filename string) error {
	req := &client_pb.RenameFileRequest{
		OldFilename: old_filename,
		NewFilename: new_filename,
	}

	resp, err := c.client.RenameFile(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to rename file: %v", err)
	}

	if resp.Status.Code != common_pb.Status_OK {
		return fmt.Errorf("rename file failed: %s", resp.Status.Message)
	}

	c.cleanupChunkCache(old_filename)

	return nil
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

	c.cleanupChunkCache(filename)

	return nil
}

func (c *Client) GetChunkInfo(ctx context.Context, filename string, startIndex, endIndex int64) (map[int64]*client_pb.ChunkInfo, error) {
	results := make(map[int64]*client_pb.ChunkInfo)

	c.chunkCacheMu.RLock()
	needFetch := false
	for idx := startIndex; idx <= endIndex; idx++ {
		cacheKey := fmt.Sprintf("%s-%d", filename, idx)
		if cached, ok := c.chunkCache[cacheKey]; ok && time.Now().Before(cached.ExpiresAt) {
			results[idx] = cached.Info
		} else {
			needFetch = true
			break
		}
	}
	c.chunkCacheMu.RUnlock()

	if !needFetch {
		return results, nil
	}

	req := &client_pb.GetFileChunksInfoRequest{
		Filename:   filename,
		StartChunk: startIndex,
		EndChunk:   endIndex,
	}

	resp, err := c.client.GetFileChunksInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk info: %v", err)
	}

	if resp.Status.Code != common_pb.Status_OK {
		return nil, fmt.Errorf("get chunk info failed: %s", resp.Status.Message)
	}

	c.chunkCacheMu.Lock()
	for idx := startIndex; idx <= endIndex; idx++ {
		chunkInfo, ok := resp.Chunks[idx]
		if !ok {
			c.chunkCacheMu.Unlock()
			return nil, fmt.Errorf("chunk index %d not found", idx)
		}

		cacheKey := fmt.Sprintf("%s-%d", filename, idx)
		c.chunkCache[cacheKey] = &ChunkLocationCache{
			Info:      chunkInfo,
			ExpiresAt: time.Now().Add(c.config.Cache.ChunkTTL),
		}
		c.chunkHandleCache[chunkInfo.ChunkHandle.Handle] = chunkInfo
		results[idx] = chunkInfo
	}
	c.chunkCacheMu.Unlock()

	return results, nil
}
