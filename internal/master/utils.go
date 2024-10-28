package master

import (
	"fmt"
	"log"
	"math"
    "math/rand"
	"path"
	"sort"
	"strings"
	"time"

    chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
    common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
)

func (m *Master) assignNewPrimary(chunkHandle string) error {
    m.chunksMu.Lock()
    defer m.chunksMu.Unlock()

    chunkInfo, exists := m.chunks[chunkHandle]
    if !exists {
        return fmt.Errorf("chunk-handle does not exist: %s", chunkHandle)
    }

    if chunkInfo == nil {
        return fmt.Errorf("chunk not found: %s", chunkHandle)
    }

    chunkInfo.mu.Lock()
    defer chunkInfo.mu.Unlock()

    // Double-check if chunk still needs a primary
    if chunkInfo.Primary != "" && time.Now().Before(chunkInfo.LeaseExpiration) {
        return fmt.Errorf("primary already exists")
    }

    // Get list of available servers that have this chunk
    availableServers := make([]string, 0)
    m.serversMu.RLock()
    for serverId := range chunkInfo.Locations {
        if server, exists := m.servers[serverId]; exists {
            server.mu.RLock()
            if server.Status == "ACTIVE" && server.ActiveOps < 100 { // Threshold for active operations
                availableServers = append(availableServers, serverId)
            }
            server.mu.RUnlock()
        }
    }
    m.serversMu.RUnlock()

    if len(availableServers) == 0 {
        return fmt.Errorf("no available servers for chunk: %s", chunkHandle)
    }
    
    newPrimary := availableServers[rand.Intn(len(availableServers))]
    chunkInfo.Primary = newPrimary
    chunkInfo.LeaseExpiration = time.Now().Add(time.Duration(m.Config.Lease.LeaseTimeout) * time.Second)

    return nil
}

func (s *MasterServer) updateServerStatus(serverId string, req *chunk_pb.HeartBeatRequest) error {
    s.Master.serversMu.Lock()
    defer s.Master.serversMu.Unlock()

    serverInfo, exists := s.Master.servers[serverId]
    if !exists {
        return fmt.Errorf("server %s not found", serverId)
    }

    serverInfo.mu.Lock()
    defer serverInfo.mu.Unlock()

    serverInfo.LastHeartbeat = time.Now()
    serverInfo.AvailableSpace = req.AvailableSpace
    serverInfo.CPUUsage = req.CpuUsage
    serverInfo.ActiveOps = req.ActiveOperations
    serverInfo.LastUpdated = time.Now()
    serverInfo.Status = "ACTIVE"
    serverInfo.FailureCount = 0

    // Update chunk information
    s.Master.chunksMu.Lock()
    defer s.Master.chunksMu.Unlock()

    for _, chunkStatus := range req.Chunks {
        chunkHandle := chunkStatus.ChunkHandle.Handle
        if _, exists := s.Master.chunks[chunkHandle]; !exists {
            s.Master.chunks[chunkHandle] = &ChunkInfo{
                Size:      chunkStatus.Size,
                Locations: make(map[string]bool),
            }
        }
        
        chunkInfo := s.Master.chunks[chunkHandle]
        chunkInfo.mu.Lock()
        chunkInfo.Size = chunkStatus.Size
        chunkInfo.Locations[serverId] = true
        chunkInfo.mu.Unlock()

        serverInfo.Chunks[chunkHandle] = true
    }

    return nil
}

func (m *Master) handleServerFailure(serverId string) {
    if serverId == "" {
        return
    }

    m.serversMu.Lock()
    defer m.serversMu.Unlock()

    serverInfo, exists := m.servers[serverId]
    if !exists {
        return
    }

    serverInfo.mu.RLock()
    defer serverInfo.mu.RUnlock()

    m.chunksMu.Lock()
    defer m.chunksMu.Unlock()

    // Update chunk locations and trigger re-replication if needed
    for chunkHandle := range serverInfo.Chunks {
        if chunkInfo, exists := m.chunks[chunkHandle]; exists {
            chunkInfo.mu.Lock()
            delete(chunkInfo.Locations, serverId)
            if chunkInfo.Primary == serverId {
                chunkInfo.Primary = ""
                chunkInfo.LeaseExpiration = time.Time{}
            }
             
            // Trigger re-replication if needed
            if len(chunkInfo.Locations) < m.Config.Replication.Factor {
                go m.initiateReplication(chunkHandle)
            }
            chunkInfo.mu.Unlock()
        }
    }

    // Remove server from active servers
    delete(m.servers, serverId)
}

func (m *Master) initiateReplication(chunkHandle string) {
    m.chunksMu.RLock()
    chunkInfo, exists := m.chunks[chunkHandle]
    if !exists {
        m.chunksMu.RUnlock()
        return
    }
    
    chunkInfo.mu.RLock()
    currentReplicas := len(chunkInfo.Locations)
    neededReplicas := m.Config.Replication.Factor - currentReplicas
    chunkInfo.mu.RUnlock()
    m.chunksMu.RUnlock()

    if neededReplicas <= 0 {
        return
    }

    // Select target servers for replication
    targets := m.selectReplicationTargets(chunkHandle, neededReplicas)
    if len(targets) == 0 {
        log.Printf("No suitable targets found for replicating chunk %s", chunkHandle)
        return
    }

    // Issue replication commands
    command := &chunk_pb.ChunkCommand{
        Type:       chunk_pb.ChunkCommand_REPLICATE,
        ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
        TargetLocations: targets,
    }

    // Find a source server to replicate from
    sourceServer := m.selectReplicationSource(chunkHandle)
    if sourceServer == "" {
        log.Printf("No source server available for replicating chunk %s", chunkHandle)
        return
    }

    // Send replication command to source server
    m.chunkServerMgr.mu.RLock()
    if responseChannel, exists := m.chunkServerMgr.activeStreams[sourceServer]; exists {
        select {
        case responseChannel <- &chunk_pb.HeartBeatResponse{
            Status:   &common_pb.Status{Code: common_pb.Status_OK},
            Commands: []*chunk_pb.ChunkCommand{command},
        }:
            log.Printf("Initiated replication of chunk %s from %s to %d targets", 
                chunkHandle, sourceServer, len(targets))
        default:
            log.Printf("Failed to send replication command: channel full")
        }
    }
    m.chunkServerMgr.mu.RUnlock()
}

func (m *Master) selectReplicationTargets(chunkHandle string, count int) []*common_pb.ChunkLocation {
    var targets []*common_pb.ChunkLocation
    
    m.serversMu.RLock()
    defer m.serversMu.RUnlock()

    chunkInfo := m.chunks[chunkHandle]
    if chunkInfo == nil {
        return targets
    }

    chunkInfo.mu.RLock()
    chunkSize := chunkInfo.Size
    existingLocations := make(map[string]bool)
    for loc := range chunkInfo.Locations {
        existingLocations[loc] = true
    }
    chunkInfo.mu.RUnlock()

    // Score and sort servers based on multiple criteria
    type serverScore struct {
        id    string
        score float64
    }
    var scoredServers []serverScore

    for serverId, serverInfo := range m.servers {
        // Skip if server already has the chunk
        if existingLocations[serverId] {
            continue
        }

        serverInfo.mu.RLock()
        // Skip servers that don't have enough space
        if serverInfo.AvailableSpace < chunkSize {
            serverInfo.mu.RUnlock()
            continue
        }

        // Calculate score based on multiple factors
        score := 100.0
        // Prefer servers with more available space
        score += float64(serverInfo.AvailableSpace) / float64(1<<30) // normalize by GB
        // Prefer servers with lower CPU usage
        score -= serverInfo.CPUUsage
        // Prefer servers with fewer active operations
        score -= float64(serverInfo.ActiveOps) / 100
        serverInfo.mu.RUnlock()

        scoredServers = append(scoredServers, serverScore{serverId, score})
    }

    // Sort servers by score
    sort.Slice(scoredServers, func(i, j int) bool {
        return scoredServers[i].score > scoredServers[j].score
    })

    // Select top N servers
    for i := 0; i < len(scoredServers) && len(targets) < count; i++ {
        targets = append(targets, &common_pb.ChunkLocation{
            ServerId: scoredServers[i].id,
        })
    }

    return targets
}

func (m *Master) selectReplicationSource(chunkHandle string) string {
    m.chunksMu.RLock()
    chunkInfo, exists := m.chunks[chunkHandle]
    if !exists {
        m.chunksMu.RUnlock()
        return ""
    }

    chunkInfo.mu.RLock()
    defer chunkInfo.mu.RUnlock()
    m.chunksMu.RUnlock()

    // Prefer primary if it exists and is alive
    if chunkInfo.Primary != "" {
        m.serversMu.RLock()
        if server, exists := m.servers[chunkInfo.Primary]; exists {
            server.mu.RLock()
            if server.Status == "ACTIVE" && server.ActiveOps < 100 { // Configurable threshold
                server.mu.RUnlock()
                m.serversMu.RUnlock()
                return chunkInfo.Primary
            }
            server.mu.RUnlock()
        }
        m.serversMu.RUnlock()
    }

    // Otherwise, select the least loaded replica
    var bestServer string
    var minLoad int32 = math.MaxInt32

    m.serversMu.RLock()
    defer m.serversMu.RUnlock()

    for serverId := range chunkInfo.Locations {
        if server, exists := m.servers[serverId]; exists {
            server.mu.RLock()
            if server.Status == "ACTIVE" && server.ActiveOps < minLoad {
                minLoad = server.ActiveOps
                bestServer = serverId
            }
            server.mu.RUnlock()
        }
    }

    return bestServer
}

func (s *MasterServer) generateChunkCommands(serverId string) []*chunk_pb.ChunkCommand {
    var commands []*chunk_pb.ChunkCommand

    s.Master.serversMu.RLock()
    serverInfo, exists := s.Master.servers[serverId]
    if !exists {
        s.Master.serversMu.RUnlock()
        return commands
    }
    serverInfo.mu.RLock()
    s.Master.serversMu.RUnlock()

    // Check if server is overloaded
    if serverInfo.ActiveOps > 100 || serverInfo.CPUUsage > 80 { // Configurable thresholds
        serverInfo.mu.RUnlock()
        return commands
    }
    serverInfo.mu.RUnlock()

    s.Master.chunksMu.RLock()
    defer s.Master.chunksMu.RUnlock()

    // Generate necessary commands based on system state
    for chunkHandle, chunkInfo := range s.Master.chunks {
        chunkInfo.mu.RLock()

        // Check if this chunk needs replication
        if len(chunkInfo.Locations) < s.Master.Config.Replication.Factor {
            // Check if this server is a good candidate for replication
            if _, hasChunk := chunkInfo.Locations[serverId]; !hasChunk {
                commands = append(commands, &chunk_pb.ChunkCommand{
                    Type:       chunk_pb.ChunkCommand_REPLICATE,
                    ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
                })
            }
        }

        // Check if this server should become primary
        if chunkInfo.Primary == "" && chunkInfo.Locations[serverId] {
            commands = append(commands, &chunk_pb.ChunkCommand{
                Type:       chunk_pb.ChunkCommand_BECOME_PRIMARY,
                ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
            })
        }

        chunkInfo.mu.RUnlock()

        if len(commands) >= 5 { // Limit number of commands per heartbeat
            break
        }
    }

    return commands
}

// Add garbage collection related methods
func (m *Master) runGarbageCollection() {
    ticker := time.NewTicker(time.Duration(m.Config.Deletion.GCInterval) * time.Second)
    for range ticker.C {
        m.gcMu.Lock()
        if m.gcInProgress {
            m.gcMu.Unlock()
            continue
        }
        m.gcInProgress = true
        m.gcMu.Unlock()

        log.Printf("Starting garbage collection cycle")
        
        // Get list of files to process
        filesToProcess := m.getExpiredDeletedFiles()
        
        // Process files in batches
        for i := 0; i < len(filesToProcess); i += m.Config.Deletion.GCDeleteBatchSize {
            end := i + m.Config.Deletion.GCDeleteBatchSize
            if end > len(filesToProcess) {
                end = len(filesToProcess)
            }
            
            batch := filesToProcess[i:end]
            m.processGCBatch(batch)
        }
        
        m.gcMu.Lock()
        m.gcInProgress = false
        m.gcMu.Unlock()
        
        log.Printf("Completed garbage collection cycle")
    }
}

func (m *Master) getExpiredDeletedFiles() []string {
    var expiredFiles []string
    cutoffTime := time.Now().Add(-time.Duration(m.Config.Deletion.RetentionPeriod) * time.Second)
    
    m.deletedFilesMu.RLock()
    defer m.deletedFilesMu.RUnlock()
    
    for deletedPath, info := range m.deletedFiles {
        if info.DeleteTime.Before(cutoffTime) {
            expiredFiles = append(expiredFiles, deletedPath)
        }
    }
    
    return expiredFiles
}

func (m *Master) processGCBatch(deletedPaths []string) {
    for _, deletedPath := range deletedPaths {
        m.deletedFilesMu.Lock()
        fileInfo, exists := m.deletedFiles[deletedPath]
        if !exists {
            m.deletedFilesMu.Unlock()
            continue
        }
        
        // Collect chunks to delete
        chunksToDelete := make([]string, 0)
        fileInfo.FileInfo.mu.RLock()
        for _, chunkHandle := range fileInfo.FileInfo.Chunks {
            chunksToDelete = append(chunksToDelete, chunkHandle)
        }
        fileInfo.FileInfo.mu.RUnlock()
        
        // Remove from deleted files map
        delete(m.deletedFiles, deletedPath)
        m.deletedFilesMu.Unlock()
        
        // Clean up chunks
        m.chunksMu.Lock()
        for _, chunkHandle := range chunksToDelete {
            if chunkInfo, exists := m.chunks[chunkHandle]; exists {
                chunkInfo.mu.Lock()
                // Send delete commands to all chunk servers
                for serverId := range chunkInfo.Locations {
                    m.sendDeleteChunkCommand(serverId, chunkHandle)
                }
                chunkInfo.mu.Unlock()
                delete(m.chunks, chunkHandle)
            }
        }
        m.chunksMu.Unlock()
        
        log.Printf("GC: Permanently deleted file %s and its %d chunks", 
            fileInfo.OriginalPath, len(chunksToDelete))
    }
}

func (m *Master) sendDeleteChunkCommand(serverId, chunkHandle string) {
    command := &chunk_pb.ChunkCommand{
        Type:        chunk_pb.ChunkCommand_DELETE,
        ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
    }

    m.chunkServerMgr.mu.RLock()
    if responseChannel, exists := m.chunkServerMgr.activeStreams[serverId]; exists {
        select {
        case responseChannel <- &chunk_pb.HeartBeatResponse{
            Status:   &common_pb.Status{Code: common_pb.Status_OK},
            Commands: []*chunk_pb.ChunkCommand{command},
        }:
            log.Printf("Sent delete command for chunk %s to server %s", chunkHandle, serverId)
        default:
            log.Printf("Failed to send delete command: channel full")
        }
    }
    m.chunkServerMgr.mu.RUnlock()
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
	Validation Utils Start:- 
*/

func (s *MasterServer) validateFilename(filename string) error {
    if filename == "" {
        return ErrInvalidFileName
    }

    if len(filename) > s.Master.Config.Metadata.MaxFilenameLength {
        return ErrInvalidFileName
    }

    if strings.Count(filename, "/") > s.Master.Config.Metadata.MaxDirectoryDepth {
        return ErrInvalidFileName
    }

    if !validatePath(filename) {
        return ErrInvalidFileName
    }

    return nil
}

func validatePath(filepath string) bool {
    cleaned := path.Clean(filepath)
    
    if path.IsAbs(cleaned) || strings.Contains(cleaned, "..") || strings.Contains(cleaned, "./") {
        return false
    }
    
    for _, char := range filepath {
        if (char < 32 || char > 126) && char != '/' {
            return false
        }
    }
    
    return true
}