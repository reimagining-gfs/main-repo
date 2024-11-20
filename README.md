# GFS Implementation

This is a comprehensive reference guide for our implementation of the GFS, detailing resources and references for understanding concepts, coding strategies, and design considerations.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [References](#references)
  - [Research Papers and Articles](#research-papers-and-articles)
  - [Technical Blogs and Tutorials](#technical-blogs-and-tutorials)
  - [Libraries and Tools](#libraries-and-tools)
- [Notes and Observations](#notes-and-observations)

---

## Project Overview

This project is a localized implementation of the Google File System (GFS) including functionalities such as read and write handling, replication, chunk handling, etc. and the salient addition of exactly-once record-append semantics.

Key features include:

- **Client-Server Communication** using gRPC
- **File Chunking and Replication** to support large file storage
- **Chunk Location Management** by the Master node
- **Leasing and Heartbeats** for consistency and fault detection
- _**TODO**_

---

## Project Structure

The organization of this repository is:

```plaintext
GFS-DISTRIBUTED-SYSTEM/
├── api/proto/                      # Protobuf definitions for gRPC communication
│   ├── chunk_master/
│   ├── chunk_operations/
│   ├── client_master/
│   └── common/
├── cmd/                            # Main executable files for various components
│   ├── chunkserver/
│   │   └── main.go
│   ├── client/
│   │   └── main.go
│   └── master/
│       └── main.go
├── configs/                        # Configuration files
│   ├── error-config.yml
│   └── general-config.yml
├── docs/                           # Documentation
│   ├── master.md
│   └── Project-Proposal.pdf
├── internal/                       # Core logic and utilities
│   ├── chunkserver/
│   ├── client/
│   └── master/
│       ├── config.go
│       ├── master.go
│       ├── master_test.go
│       ├── metadata.go
│       ├── monitor.go
│       ├── types.go
│       └── utils.go
├── storage/                        # Persistent storage
│   ├── chunks/
│   ├── metadata.json               # JSON metadata for chunks
├── go.mod
├── go.sum
├── Makefile
└── README.md

```

Note that the structure followed is according to a common go project pattern mentioned at: https://github.com/golang-standards/project-layout

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

## Notes and Observations

- **Leasing Mechanism**: Implemented a basic leasing mechanism as inspired by GFS. The Master server manages leases and renews them based on client access patterns.
- **Chunk Replication**: Ensured that each chunk is replicated across multiple chunk servers for data redundancy and fault tolerance.

- Detailed doumentation about every component can be found in the `/docs` folder.

---
