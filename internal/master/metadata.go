package master

import (
    "encoding/json"
    "io/ioutil"
    "log"
)

func (m *Master) LoadMetadata(path string) error {
    data, err := ioutil.ReadFile(path)
    if err != nil {
        return err
    }

    if len(data) == 0 {
        log.Printf("Metadata file is empty, starting with empty state.")
        return nil
    }

    var metadata struct {
        Files           map[string]*FileInfo
        Chunks          map[string]*ChunkInfo
        NextChunkHandle int64
    }

    if err := json.Unmarshal(data, &metadata); err != nil {
        return err
    }

    m.filesMu.Lock()
    m.chunksMu.Lock()
    defer m.filesMu.Unlock()
    defer m.chunksMu.Unlock()

    m.files = metadata.Files
    m.chunks = metadata.Chunks
    m.nextChunkHandle = metadata.NextChunkHandle

    return nil
}

func (m *Master) SaveMetadata(path string) error {
    m.filesMu.RLock()
    m.chunksMu.RLock()
    defer m.filesMu.RUnlock()
    defer m.chunksMu.RUnlock()

    log.Printf("Check Pointing")

    metadata := struct {
        Files map[string]*FileInfo
        Chunks map[string]*ChunkInfo
        NextChunkHandle int64
    }{
        Files: m.files,
        Chunks: m.chunks,
        NextChunkHandle: m.nextChunkHandle,
    }

    data, err := json.Marshal(metadata)
    if err != nil {
        return err
    }

    return ioutil.WriteFile(path, data, 0644)
}
