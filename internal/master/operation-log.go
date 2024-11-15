package master

import (
    "bufio"
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "time"
)

const (
    OpCreateFile    = "CREATE_FILE"
    OpDeleteFile    = "DELETE_FILE"
    OpAddChunk      = "ADD_CHUNK"
    OpUpdateChunk   = "UPDATE_CHUNK"
    OpDeleteChunk   = "DELETE_CHUNK"
    OpRenameFile    = "RENAME_FILE"
)

type LogEntry struct {
    Timestamp   time.Time       `json:"timestamp"`
    Operation   string         `json:"operation"`
    Filename    string         `json:"filename,omitempty"`
    NewFilename string         `json:"new_filename,omitempty"`
    ChunkHandle string         `json:"chunk_handle,omitempty"`
    Metadata    interface{}    `json:"metadata,omitempty"`
}

type OperationLog struct {
    mu             sync.RWMutex
    logFile        *os.File
    writer         *bufio.Writer
    logPath        string
    metadataPath   string
}

func NewOperationLog(logPath, metadataPath string) (*OperationLog, error) {
    file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil, fmt.Errorf("failed to open operation log: %v", err)
    }

    return &OperationLog{
        logFile:      file,
        writer:       bufio.NewWriter(file),
        logPath:      logPath,
        metadataPath: metadataPath,
    }, nil
}

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

    data, err := json.Marshal(entry)
    if err != nil {
        return fmt.Errorf("failed to marshal log entry: %v", err)
    }

    if _, err := ol.writer.WriteString(string(data) + "\n"); err != nil {
        return fmt.Errorf("failed to write log entry: %v", err)
    }

    return ol.writer.Flush()
}

func (ol *OperationLog) Close() error {
    ol.mu.Lock()
    defer ol.mu.Unlock()
    
    if err := ol.writer.Flush(); err != nil {
        return err
    }
    return ol.logFile.Close()
}

func (m *Master) replayOperationLog() error {
    file, err := os.Open(m.Config.OperationLog.Path)
    if err != nil {
        if os.IsNotExist(err) {
            return nil
        }
        return err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        var entry LogEntry
        if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
            return fmt.Errorf("failed to unmarshal log entry: %v", err)
        }

        switch entry.Operation {
        case OpCreateFile:
            m.filesMu.Lock()
            m.files[entry.Filename] = &FileInfo{
                Chunks: make(map[int64]string),
            }
            m.filesMu.Unlock()

        case OpDeleteFile:
            m.filesMu.Lock()
            delete(m.files, entry.Filename)
            m.filesMu.Unlock()

        case OpRenameFile:
            m.filesMu.Lock()
            if fileInfo, exists := m.files[entry.Filename]; exists {
                m.files[entry.NewFilename] = fileInfo
                delete(m.files, entry.Filename)
            }
            m.filesMu.Unlock()

        case OpAddChunk:
            var chunkInfo ChunkInfo
            metadataBytes, _ := json.Marshal(entry.Metadata)
            if err := json.Unmarshal(metadataBytes, &chunkInfo); err != nil {
                return fmt.Errorf("failed to unmarshal chunk info: %v", err)
            }

            m.chunksMu.Lock()
            m.chunks[entry.ChunkHandle] = &chunkInfo
            m.chunksMu.Unlock()

        case OpUpdateChunk:
            var chunkInfo ChunkInfo
            metadataBytes, _ := json.Marshal(entry.Metadata)
            if err := json.Unmarshal(metadataBytes, &chunkInfo); err != nil {
                return fmt.Errorf("failed to unmarshal chunk info: %v", err)
            }

            m.chunksMu.Lock()
            if chunk, exists := m.chunks[entry.ChunkHandle]; exists {
                chunk.Locations = chunkInfo.Locations
                chunk.Primary = chunkInfo.Primary
                chunk.LeaseExpiration = chunkInfo.LeaseExpiration
            }
            m.chunksMu.Unlock()

        case OpDeleteChunk:
            m.chunksMu.Lock()
            delete(m.chunks, entry.ChunkHandle)
            m.chunksMu.Unlock()
        }
    }

    return scanner.Err()
}
