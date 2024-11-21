# Master

The entry point into the master is the [`../cmd/master/main.go`](../cmd/master/main.go) script and loads the configuration from [`../configs/chunkserver-config.yml`](../configs/general-config.yml).

## Table of Contents

- [Proto Definitions](#proto-definitions)
- [Initialization](#initialization)
- [Proto Definitions](#proto-definitions)
- [Communications with Chunk Servers](#communications-with-chunk-servers)
- [Communications with Clients](#communications-with-clients)

## Proto Definitions

These are available in `api/proto/client_master/client_master.proto` and refer to the communications between the client and the master server.

```proto
service ClientMasterService {
    rpc GetFileChunksInfo(GetFileChunksInfoRequest) returns (GetFileChunksInfoResponse) {}
    rpc CreateFile(CreateFileRequest) returns (CreateFileResponse) {}
    rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse) {}
    rpc RenameFile(RenameFileRequest) returns (RenameFileResponse) {}
}
```

As for the communications between the chunk servers and the masters, refer to `api/proto/chunk_master/chunk_master.proto`.

```proto
service ChunkMasterService {
    rpc ReportChunk(ReportChunkRequest) returns (stream ReportChunkResponse) {} // For reporting all the hosted chunks to the master on startup
    rpc RequestLease(RequestLeaseRequest) returns (RequestLeaseResponse) {}
    rpc HeartBeat(stream HeartBeatRequest) returns (stream HeartBeatResponse) {}
}
```

## Initialization

Initialization is done using the `NewMasterServer` function in `internal/master/master.go`. This returns an object of type `MasterServer` defined as

```go
type MasterServer struct {
    client_pb.UnimplementedClientMasterServiceServer
    chunk_pb.UnimplementedChunkMasterServiceServer
    Master     *Master
    grpcServer *grpc.Server
}
```

Within this, the `Master` is separately init-ed using the `NewMaster` function.

### Spawning a new `Master`

A `Master` is defined as the following:

```go
type Master struct {
    Config *Config
    // File namespace
    files   map[string]*FileInfo
    filesMu sync.RWMutex
    // Chunk management
    chunks   map[string]*ChunkInfo // chunk_handle -> chunk info
    chunksMu sync.RWMutex
    deletedChunks   map[string]bool
    deletedChunksMu sync.RWMutex
    // Server management
    servers   map[string]*ServerInfo // server_id -> server info
    serversMu sync.RWMutex
    // Chunk server manager
    chunkServerMgr *ChunkServerManager
    gcInProgress bool
    gcMu         sync.Mutex
    pendingOpsMu sync.RWMutex
    pendingOps   map[string][]*PendingOperation // serverId -> pending operations
    opLog *OperationLog
}
```

On start up, it reads the operation log and replays it using the `NewOperationLog` and `replayOperationLog` functions (defined within `internal/master/operation-log.go`).

#### Operation Log Replay

The `replayOperationLog` function replays the operation log to restore the state of the Master server during startup. It reads entries from a log file, deserializes them, and applies operations to update in-memory metadata.

- **Purpose**: Reconstruct the state of files and chunks in the Master server using the operation log.
- **Input**: None (reads from the operation log file).
- **Output**: Returns an error if the operation log is malformed or an I/O operation fails.

The _Process Overview_ is as follows:

1. **Open Log File**:
   - Opens the log file specified in the Master configuration.
   - Skips if the file does not exist.

2. **Iterate Over Log Entries**:
   - Uses a scanner to read log entries line-by-line.
   - Deserializes each log entry into a `LogEntry` struct.

3. **Apply Operations**:
   - Executes the appropriate operation based on the `Operation` field in the log entry.
   - Supported operations:
     - **Create File (`OpCreateFile`)**: Adds a new file with empty chunk metadata.
     - **Delete File (`OpDeleteFile`)**: Removes file metadata.
     - **Rename File (`OpRenameFile`)**: Renames file metadata.
     - **Add Chunk (`OpAddChunk`)**:
       - Adds a new chunk to the chunk map.
       - Updates the file’s chunk-to-handle mapping.
     - **Update Chunk (`OpUpdateChunk`)**: Updates chunk metadata, including locations, primary, and lease expiration.
     - **Delete Chunk (`OpDeleteChunk`)**: Removes chunk metadata.
     - **Update Chunk Version (`OpUpdateChunkVersion`)**:
       - Updates the chunk version.
       - Optionally updates the chunk's primary location.

4. **Handle Metadata**:
   - Parses metadata as needed for chunk operations (e.g., chunk info, file index, version).
   - Validates and deserializes metadata fields.

5. **Error Handling**:
   - Reports errors for invalid log entry formats or deserialization issues.

#### Background Processes

These are initialized as Goroutines within the Master.

1. **`monitorServerHealth`**:
   - Periodically checks the health of chunk servers.
   - Marks servers as "INACTIVE" if no heartbeat is received within the timeout.
   - Escalates "INACTIVE" servers to "DEAD" after multiple failures and handles their removal.

2. **`monitorChunkReplication`**:
   - Ensures chunk replication meets the desired replication factor.
   - Identifies under-replicated chunks and triggers replication for them.

3. **`cleanupExpiredLeases`**:
   - Removes expired primary leases for chunks.
   - Resets primary designation and lease expiration timestamps for expired leases.

4. **`runGarbageCollection`**:
   - Periodically deletes files marked for deletion.
   - Processes expired files in batches to ensure efficient cleanup without blocking operations.
   - Prevents concurrent garbage collection cycles.

5. **`runPendingOpsCleanup`**:
   - Cleans up expired or failed pending operations.
   - Deletes operations exceeding the allowed retry count or time limit.
   - Retains only valid operations for further processing.

6. **`cleanupExpiredOperations`**:
   - Core logic for cleaning pending operations.
   - Logs failed operations for auditing and debugging purposes.
   - Updates the pending operations map to reflect only active tasks.

### Other Initializations

The Master Server also sets `grpc.NewServer()` as its `grpcServer`.

Additionally, the Master Server starts listening on the given address and starts two Goroutines:

1. The Serve method of the gRPC server is called with the listener (lis). This method will block and handle incoming RPC requests until it encounters an error (e.g., when shutting down).

2. Another that uses a ticker to perform periodic metadata checkpointing based on a backup interval specified in the configuration.

Lastly, it loads the (or creates is empty) the metadata file.

## Communications with Chunk Servers

Functionality for the following are defined within `internal/master/master.go`.

### Reporting a Chunk (`ReportChunk`)

This facilitates the registration and reporting of chunk information by a chunk server to the master. It also establishes a communication channel to send future commands to the chunk server. The method handles chunk updates, stale replica identification, and cleanup.

#### `ReportChunk` Workflow

1. **Validation**:
   - Ensure `ServerId` is provided in the request; otherwise, return an error.

2. **Channel Setup**:
   - Create a response channel for sending commands to the chunk server.
   - Register the channel in `ChunkServerManager`'s `activeStreams` map using the server ID.

3. **Resource Cleanup**:
   - Ensure the response channel is closed, and the server's entry in `activeStreams` is removed upon function exit.

4. **Server Registration**:
   - Check if the server is already registered in `servers`. If not, add it with default metadata, such as the address, current time as the last heartbeat, and an active status.

5. **Chunk Updates**:
   - Process the list of chunks reported by the server:
     - For each chunk:
       - Add it to the master's global chunk map (`chunks`) if it doesn't already exist.
       - Check for stale replicas by comparing reported chunk versions with the master’s version.
         - If stale, add the server to the `StaleReplicas` list and schedule deletion using `scheduleStaleReplicaDelete`.
         - Otherwise, update the chunk's locations and server addresses.

6. **Stream Commands**:
   - Continuously listen for commands to send to the server:
     - Convert `HeartBeatResponse` messages into `ReportChunkResponse` messages and send them over the stream.
     - Handle client disconnection gracefully.

#### `ReportChunk` Auxiliary Functions

- **`scheduleStaleReplicaDelete`**:
  - Deletes a stale replica of a chunk from a specified server.
  - Checks if the replica is already up-to-date or if sufficient up-to-date replicas exist before proceeding.
  - Sends a `DELETE` command to the chunk server via the response channel.

### Requesting a Lease (`RequestLease`)

Handles lease extension requests for chunk management in a distributed system. It ensures that only the active primary server with the latest chunk version can extend the lease.

#### `RequestLease` Workflow

1. **Validate Input**:
   - Ensure `ChunkHandle.Handle` is provided.
   - Log the request details.

2. **Chunk Existence Check**:
   - Acquire a read lock on the chunk metadata.
   - Return an error if the chunk does not exist.

3. **Chunk Info Validation**:
   - Acquire a write lock on the chunk metadata.
   - Check if the server has a stale version. Deny lease if true.
   - Ensure the requesting server is the current primary. Deny lease if not.

4. **Lease Extension**:
   - Update `LeaseExpiration` to extend the lease.
   - Increment the chunk version.

5. **Broadcast Version Update**:
   - Notify all servers storing the chunk about the version update via their active response channels.
   - Log successful and failed updates.

6. **Update Metadata**:
   - Increment the chunk version if updates were sent successfully.

7. **Response**:
   - Return a successful response with the new lease expiration and chunk version.

#### Key Features

- Ensures lease can only be extended by the active primary server.
- Verifies the requesting server has the latest chunk version.
- Synchronizes chunk version updates across all servers storing the chunk.

### Heartbeat Mechanism (`HeartBeat`)

Handles periodic heartbeat messages from chunk servers, ensuring real-time monitoring and management of server statuses, chunk replication, and task execution.

#### `HeartBeat` Workflow

1. **Initialize Variables**:
   - Create a `responseChannel` for sending heartbeat responses.
   - Start a goroutine to listen for and send responses through the stream.

2. **Process Incoming Heartbeats**:
   - Receive messages using `stream.Recv()`.
   - If the connection closes, invoke `handleServerFailure` to manage the failure and return the error.

3. **Server Initialization**:
   - If the server is new (`serverId` not set), register it in `servers` with relevant metadata (e.g., address, available space, CPU usage, active operations).

4. **Update Server Status**:
   - Call `updateServerStatus` to refresh server heartbeat timestamp and update server and chunk data.

5. **Generate Commands**:
   - Use `generateChunkCommands` to retrieve any pending operations (e.g., replication or updates) for the server.
   - Prepare and send these commands via the `responseChannel`.

6. **Concurrency**:
   - Use `select` to handle cases where the `responseChannel` is full, logging a warning if a response cannot be sent.

#### `HeartBeat` Auxiliary Functions

- **`handleServerFailure`**:
  - Handles cleanup and replication triggers when a server fails.
  - Workflow:
    - Lock `servers` and remove the failed server.
    - For all chunks hosted by the server:
      - Remove the server from `Locations`.
      - Clear primary information if the server was the primary.
      - Trigger re-replication if the number of replicas falls below the desired replication factor.
    - Log the failure and update the server's removal in `servers`.

- **`updateServerStatus`**:
  - Updates the status and metadata of an active server based on a heartbeat request.
  - Workflow:
    - Lock `servers` to update server-specific metadata (heartbeat time, CPU usage, etc.).
    - Lock `chunks` to add or update chunk information:
      - Ensure chunks from the request exist in `chunks`.
      - Update size and locations.
      - Add new chunk information if missing.
    - Skip deleted chunks.

- **`generateChunkCommands`**:
  - Generates pending operations (e.g., chunk replication) for a server.
  - Workflow:
    - Lock `pendingOps` to access pending operations for the server.
    - Prepare commands for operations like chunk replication or updates.
    - Increment operation attempt count and update timestamp.
    - Remove completed operations from `pendingOps`.

## Communications with Clients

Functionality for the following are defined within `internal/master/master.go`.

### Getting Information of File Chunks (`GetFileChunksInfo`)

Fetches metadata about file chunks, initializes missing chunks, and assigns chunk servers.

Expects the file name, start chunk and end chunk. On success, returns a mapping of chunk indices to `ChunkInfo` objects (which contains the handle, primary and secondary locations, and the version).

#### `GetFileChunksInfo` Workflow

1. Validates the filename.
2. Checks if the file exists in the metadata.
3. Initializes missing chunks, selecting servers and sending `INIT_EMPTY` commands.
4. Gathers chunk metadata, assigns new primaries if necessary.
5. Returns metadata for the requested chunk range.

### Creating a File (`CreateFile`)

Creates an empty file entry in the system using the input filename.

#### `CreateFile` Workflow

1. Validates the filename.
2. Checks if the file already exists.
3. Creates a new file metadata entry.
4. Logs the operation in the operation log.

_NOTE_: An empty file is not mapped to any chunks to save space.

### Renaming a File (`RenameFile`)

Expects the old filename and the new filename as inputs and processes the command through a series of validations.

### Deleting a File (`DeleteFile`)

Performs a soft delete of a file by moving it to a trash directory. Garbage collection deletes these later.
