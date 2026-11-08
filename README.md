# TinyKV

TinyKV is a distributed key-value storage engine in Go implementing Raft consensus, Multi-Raft region split, and Percolator-style MVCC transactions.

## Core Features

- **Standalone Storage Engine**: Embedded Key-Value engine built on Badger KV, supporting `Get`, `Put`, `Delete`, and `Scan` operations.
- **Raft Consensus Layer**: Full implementation of the Raft consensus protocol including leader election, log replication, snapshotting, and compaction.
- **Multi-Raft & Sharding**: Sharded key space across dynamic Raft groups (regions) with automated region splits and balance scheduling.
- **Percolator MVCC Transactions**: Distributed 2-phase commit (2PC) transaction engine supporting Multi-Version Concurrency Control, lock management, and rollback handling.

## Architecture & Module Overview

- `kv/`: Storage engine interfaces and gRPC service implementations.
- `raft/`: Core Raft state machine and consensus protocol engine.
- `kv/storage/standalone_storage/` & `kv/raftstore/`: Storage engine wrappers and Multi-Raft message routing layer.
- `scheduler/`: Central coordinator managing cluster heartbeat collection, region splits, and store balancing.

## Quick Start & Testing

Run unit and integration test suites for all components:

```bash
# Run Project 1 (Standalone KV) tests
make project1

# Run Project 2 (Raft KV) tests
make project2

# Run Project 3 (Multi-Raft KV) tests
make project3

# Run Project 4 (Percolator MVCC Transactions) tests
make project4
```
