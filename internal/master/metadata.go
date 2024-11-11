package master

import (
    "bufio"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
)

func (m *Master) LoadMetadata(path string) error {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        if !os.IsNotExist(err) {
            return err
        }
        log.Printf("Metadata file doesn't exist, starting with empty state.")
    } else if len(data) > 0 {
        var metadata struct {
            Files  map[string]*FileInfo
            Chunks map[string]*ChunkInfo
        }

        if err := json.Unmarshal(data, &metadata); err != nil {
            return err
        }

        m.filesMu.Lock()
        m.chunksMu.Lock()
        m.files = metadata.Files
        m.chunks = metadata.Chunks
        m.filesMu.Unlock()
        m.chunksMu.Unlock()
    }

    return m.replayOperationLog()
}

func (m *Master) checkpointMetadata() error {
    m.filesMu.RLock()
    m.chunksMu.RLock()

    metadata := struct {
        Files  map[string]*FileInfo  `json:"files"`
        Chunks map[string]*ChunkInfo `json:"chunks"`
    }{
        Files:  m.files,
        Chunks: m.chunks,
    }

    data, err := json.MarshalIndent(metadata, "", "  ")
    m.filesMu.RUnlock()
    m.chunksMu.RUnlock()

    if err != nil {
        return fmt.Errorf("failed to marshal metadata: %v", err)
    }

    tempFile := m.Config.Metadata.Database.Path + ".tmp"
    if err := ioutil.WriteFile(tempFile, data, 0644); err != nil {
        return fmt.Errorf("failed to write temporary metadata file: %v", err)
    }

    // Atomically rename temporary file to actual metadata file
    if err := os.Rename(tempFile, m.Config.Metadata.Database.Path); err != nil {
        return fmt.Errorf("failed to rename metadata file: %v", err)
    }

    // After successful checkpoint, truncate the operation log
    m.opLog.mu.Lock()
    defer m.opLog.mu.Unlock()

    if err := m.opLog.logFile.Truncate(0); err != nil {
        return fmt.Errorf("failed to truncate operation log: %v", err)
    }
    
    if _, err := m.opLog.logFile.Seek(0, 0); err != nil {
        return fmt.Errorf("failed to reset operation log position: %v", err)
    }

    // Reset the writer
    m.opLog.writer = bufio.NewWriter(m.opLog.logFile)

    return nil
}
