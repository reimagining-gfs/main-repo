package client

import (
    "context"
    "fmt"
    "io"
)

const ChunkSize = 1 * 1024 * 1024 // 1MB

func (fh *FileHandle) Read(p []byte) (n int, err error) {
    if fh.readOnly {
        return 0, fmt.Errorf("file not opened for reading")
    }

    fh.mu.RLock()
    position := fh.position
    fh.mu.RUnlock()

    chunkIndex := position / ChunkSize
    chunkOffset := position % ChunkSize

    chunkInfo, err := fh.client.getChunkInfo(context.Background(), fh.filename, chunkIndex)
    if err != nil {
        return 0, fmt.Errorf("failed to get chunk info: %v", err)
    }
	fmt.Print(chunkInfo, chunkOffset)

    // TODO: Implement chunk server read
    return 0, fmt.Errorf("read operation not yet implemented")
}

func (fh *FileHandle) Write(p []byte) (n int, err error) {
    if fh.readOnly {
        return 0, fmt.Errorf("file not opened for writing")
    }

    fh.mu.RLock()
    position := fh.position
    fh.mu.RUnlock()

    chunkIndex := position / ChunkSize
    chunkOffset := position % ChunkSize

    chunkInfo, err := fh.client.getChunkInfo(context.Background(), fh.filename, chunkIndex)
    if err != nil {
        return 0, fmt.Errorf("failed to get chunk info: %v", err)
    }
	fmt.Print(chunkInfo, chunkOffset)

    // TODO: Implement chunk server write
    return 0, fmt.Errorf("write operation not yet implemented")
}

func (fh *FileHandle) Seek(offset int64, whence int) (int64, error) {
    fh.mu.Lock()
    defer fh.mu.Unlock()

    var absolute int64
    switch whence {
    case io.SeekStart:
        absolute = offset
    case io.SeekCurrent:
        absolute = fh.position + offset
    case io.SeekEnd:
        return 0, fmt.Errorf("seek from end not implemented")
    default:
        return 0, fmt.Errorf("invalid whence")
    }

    if absolute < 0 {
        return 0, fmt.Errorf("negative position")
    }

    fh.position = absolute
    return absolute, nil
}

func (fh *FileHandle) Close() error {
    return nil
}
