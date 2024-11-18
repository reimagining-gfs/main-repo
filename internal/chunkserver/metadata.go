package chunkserver

import (
    "encoding/json"
    "fmt"
	"log"
    "os"
    "path/filepath"
    "time"
)

type MetadataStore struct {
    Chunks map[string]*ChunkMetadata `json:"chunks"`
    LastCheckpoint time.Time         `json:"last_checkpoint"`
}

func (cs *ChunkServer) saveMetadata() error {
    metadataPath := filepath.Join(cs.serverDir, "metadata.json")
    
    store := MetadataStore{
        Chunks:         cs.chunks,
        LastCheckpoint: time.Now(),
    }
    
    tempPath := metadataPath + ".tmp"
    
    data, err := json.MarshalIndent(store, "", "    ")
    if err != nil {
        return fmt.Errorf("failed to marshal metadata: %v", err)
    }
    
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return fmt.Errorf("failed to write metadata to temporary file: %v", err)
    }
    
    if err := os.Rename(tempPath, metadataPath); err != nil {
        os.Remove(tempPath)
        return fmt.Errorf("failed to save metadata file: %v", err)
    }
    
    return nil
}

func (cs *ChunkServer) loadMetadata() error {
    metadataPath := filepath.Join(cs.serverDir, "metadata.json")
    
    data, err := os.ReadFile(metadataPath)
    if err != nil {
        if os.IsNotExist(err) {
            // Initialize empty metadata if file doesn't exist
            cs.chunks = make(map[string]*ChunkMetadata)
            return nil
        }
        return fmt.Errorf("failed to read metadata file: %v", err)
    }
    
    var store MetadataStore
    if err := json.Unmarshal(data, &store); err != nil {
        return fmt.Errorf("failed to unmarshal metadata: %v", err)
    }
    
    cs.chunks = store.Chunks
    return nil
}

func (cs *ChunkServer) StartMetadataCheckpointing(interval time.Duration) {
    ticker := time.NewTicker(interval)
    go func() {
        for {
            select {
            case <-ticker.C:
                cs.mu.RLock()
                if err := cs.saveMetadata(); err != nil {
                    log.Printf("Failed to checkpoint metadata: %v", err)
                }
                cs.mu.RUnlock()
            }
        }
    }()
}

func (cs *ChunkServer) RecoverMetadata() error {
    cs.mu.Lock()
    defer cs.mu.Unlock()
    
    if err := cs.loadMetadata(); err != nil {
        return fmt.Errorf("failed to recover metadata: %v", err)
    }
    
    for handle := range cs.chunks {
        chunkPath := filepath.Join(cs.serverDir, handle+".chunk")
        if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
            delete(cs.chunks, handle)
            log.Printf("Removing metadata for missing chunk: %s", handle)
        }
    }
    
    if err := cs.saveMetadata(); err != nil {
        return fmt.Errorf("failed to save recovered metadata: %v", err)
    }
    
    return nil
}