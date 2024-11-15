package master

import (
    "time"
)

func (m *Master) monitorServerHealth() {
    ticker := time.NewTicker(time.Duration(m.Config.Health.CheckInterval) * time.Second)
    for range ticker.C {
        m.serversMu.Lock()
        for serverId, server := range m.servers {
            server.mu.Lock()
            timeSinceLastHeartbeat := time.Since(server.LastHeartbeat)
            if timeSinceLastHeartbeat > time.Duration(m.Config.Health.Timeout)*time.Second {
                if server.Status == "ACTIVE" {
                    server.Status = "INACTIVE"
                    server.FailureCount++
                } else if server.Status == "INACTIVE" {
                    server.FailureCount++
                    if server.FailureCount >= m.Config.Health.MaxFailures {
                        server.Status = "DEAD"
                        go m.handleServerFailure(serverId)
                    }
                }
            }
            server.mu.Unlock()
        }
        m.serversMu.Unlock()
    }
}

func (m *Master) monitorChunkReplication() {
    ticker := time.NewTicker(120 * time.Second)
    for range ticker.C {
        m.chunksMu.RLock()
        for chunkHandle, chunkInfo := range m.chunks {
            chunkInfo.mu.RLock()
            if len(chunkInfo.Locations) < m.Config.Replication.Factor {
                go m.initiateReplication(chunkHandle)
            }
            chunkInfo.mu.RUnlock()
        }
        m.chunksMu.RUnlock()
    }
}

func (m *Master) cleanupExpiredLeases() {
    ticker := time.NewTicker(time.Duration(m.Config.Health.CheckInterval) * time.Second)
    for range ticker.C {
        m.chunksMu.Lock()
        now := time.Now()
        for _, chunkInfo := range m.chunks {
            chunkInfo.mu.Lock()
            if !chunkInfo.LeaseExpiration.IsZero() && now.After(chunkInfo.LeaseExpiration) {
                chunkInfo.Primary = ""
                chunkInfo.LeaseExpiration = time.Time{}
            }
            chunkInfo.mu.Unlock()
        }
        m.chunksMu.Unlock()
    }
}
