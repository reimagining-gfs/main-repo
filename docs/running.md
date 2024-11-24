# Running Instructions

The entry points for all the components (master, clients and chunkservers) is present within `./cmd` directory.

## Running the Master

Only one master is allowed to run. By default, it uses the 50051 port.

```sh
cd cmd/master
go run main.go
```

## Running Chunk Servers

Open different terminal instances per chunk server. Replicatin kicks in when there are >1 chunk servers running.

```sh
cd cmd/chunkserver
go run main.go --port <port>
```

## Running Clients

Open different terminal instances per client.

```sh
cd cmd/client
go run main.go
```

## Configurations

All configurations are modifiable from within the `configs` directory.
