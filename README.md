# GFS Implementation

A code repository that implements the GFS with the change that appends are now exactly-once mutations.

---

## Table of Contents

- [Project Overview](#project-overview)
  - [Running Instructions](#running-instructions)
- [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [References](#references)
  - [Research Papers and Articles](#research-papers-and-articles)
  - [Technical Blogs and Tutorials](#technical-blogs-and-tutorials)
  - [Libraries and Tools](#libraries-and-tools)

---

## Project Overview

This project is a localized implementation of the Google File System (GFS) including functionalities such as read and write handling, replication, chunk handling, etc. and the salient addition of exactly-once record-append semantics.

Please refer to our detailed technical document for analysis and design choices (**TODO**: Add link).

Code documentation is available in the `/docs` directory.

### Running Instructions

For running this project, take a look at [docs/setup.md](docs/setup.md) and [docs/running.md](docs/running.md).

---

## Project Structure

The organization of this repository is:

```plaintext
GFS-DISTRIBUTED-SYSTEM/
├── api                     # Protocol buffer definitions for inter-component communication
│   └── proto
│       ├── chunk
│       │   └── chunk.proto            # Defines the communication between chunk servers
│       ├── chunk_master
│       │   └── chunk_master.proto     # Defines the communication between master and chunk servers
│       ├── chunk_operations
│       │   └── chunk_operations.proto # Defines the communication between clients and chunk servers
│       ├── client_master
│       │   └── client_master.proto    # Defines the communication between clients and master
│       └── common
│           └── common.proto           # Shared message definitions and utilities
├── cmd                     # Entry points for running system components
│   ├── chunkserver
│   │   └── main.go                   # Main executable for chunk server instances
│   ├── client
│   │   └── main.go                   # Main executable for client instances
│   └── master
│       └── main.go                   # Main executable for the master server
├── configs                 # Configuration files for all system components
│   ├── chunkserver-config.yml        # Configurations for chunk server instances
│   ├── client-config.yml             # Configurations for clients
│   ├── error-config.yml              # Error handling and message configurations
│   └── general-config.yml            # General system-wide configurations (used by master)
├── docs                    # Documentation for the system and its components
│   ├── ai-generated-code-docs        # AI-generated code documentation
│   │   ├── chunk-server-code-documentation.md     # Documentation for chunk server code
│   │   ├── master-code-documentation.md           # Documentation for master server code
│   │   └── workflow-documentation-of-chunk-server-interations.md # Explanation of chunk server workflows
│   ├── chunkserver.md                # Manual for chunk server operations
│   ├── master.md                     # Manual for master server operations
│   ├── media                         # Media assets (e.g., diagrams, images)
│   ├── Project-Proposal.pdf          # Original project proposal document
│   ├── running.md                    # Instructions for running the system
│   └── setup.md                      # Setup guide for the system
├── go.mod                 # Go module definition
├── go.sum                 # Go module dependencies checksum
├── internal               # Core logic for system components
│   ├── chunkserver
│   │   ├── chunk_operations.go       # Implements chunk-level operations (e.g., reads, writes)
│   │   ├── chunkserver.go            # Core chunk server logic
│   │   ├── config.go                 # Configuration loader for chunk server
│   │   ├── metadata.go               # Metadata handling for chunks
│   │   ├── operation_queue.go        # Operation queuing and management
│   │   └── types.go                  # Shared types for chunk server
│   ├── client
│   │   ├── client.go                 # Core client logic for interacting with master and chunk servers
│   │   ├── config.go                 # Configuration loader for clients
│   │   ├── file_operations.go        # File-level operations (e.g., reads, appends)
│   │   └── types.go                  # Shared types for clients
│   └── master
│       ├── config.go                 # Configuration loader for master server
│       ├── master.go                 # Core master server logic
│       ├── metadata.go               # Metadata management (e.g., chunk ownership)
│       ├── monitor.go                # Monitoring and health checks
│       ├── operation-log.go          # Operation log for tracking system changes
│       ├── types.go                  # Shared types for master server
│       └── utils.go                  # Utility functions for master server
├── Makefile               # Build and task automation
├── README.md              # Overview of the project and setup instructions
```

Note that the structure followed is according to a common go project pattern mentioned [here](https://github.com/golang-standards/project-layout)

---

## Dependencies

- **gRPC** for client-server communication
- **Protocol Buffers** for message serialization

---

## References

### Research Papers and Articles

**The Google File System (GFS) - 2003**
   - Authors: Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung
   - Link: [The Google File System](https://research.google/pubs/archive/51.pdf)
   - Summary: This paper introduces the foundational concepts of GFS, which inspired this project. Key features include chunk-based storage, master-based metadata management, and replication for fault tolerance.

### Technical Blogs and Tutorials

1. **MIT Lecture Notes on GFS**
   - Link: [Notes Link](https://timilearning.com/posts/mit-6.824/lecture-3-gfs/#record-appends)  
   - FAQ Doc Link: [Doc Link](https://pdos.csail.mit.edu/6.824/papers/gfs-faq.txt)
   - Summary: Blog which goes through every component of GFS in an iterative manner and diving till a nice extent to bring out the overall design.

### Libraries and Tools

- **gRPC Go Library**
  - GitHub: [grpc-go](https://github.com/grpc/grpc-go)
  - Description: Official gRPC library for Go, used for defining and implementing RPCs in this project.

---
