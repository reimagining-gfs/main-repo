PROTO_DIR = api/proto
CHUNK_MASTER = chunk_master

PROTO_FILE_CHUNK_MASTER = $(PROTO_DIR)/chunk_master/chunk_master.proto
PROTO_FILE_CHUNK_OPERATIONS = $(PROTO_DIR)/chunk_operations/chunk_operations.proto
PROTO_FILE_CLIENT_MASTER = $(PROTO_DIR)/client_master/client_master.proto
PROTO_FILE_COMMON = $(PROTO_DIR)/common/common.proto
PROTO_FILE_CHUNK = $(PROTO_DIR)/chunk/chunk.proto

PROTO_OUT_DIR = .

GO_FLAGS = --go_out=$(PROTO_OUT_DIR) --go_opt=paths=source_relative \
           --go-grpc_out=$(PROTO_OUT_DIR) --go-grpc_opt=paths=source_relative

.PHONY: proto clean

proto:
	protoc $(GO_FLAGS) $(PROTO_FILE_CHUNK_MASTER) $(PROTO_FILE_CHUNK_OPERATIONS) $(PROTO_FILE_CLIENT_MASTER) $(PROTO_FILE_COMMON) $(PROTO_FILE_CHUNK)

clean:
	find $(PROTO_DIR) -type f -name '*.pb.go' -exec rm -rf {} +
