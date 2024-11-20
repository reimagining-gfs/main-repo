# Setup Instructions

## Install the Required Dependencies

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

```sh
export PATH="$PATH:$(go env GOPATH)/bin"
```

## Make the project

```sh
make clean && make proto
```
