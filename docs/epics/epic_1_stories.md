# EPIC 1 — Core Messaging System (MVP Foundation)

## Goal

Build a single-node event streaming system with append-only logs, producers, and consumers.

## Description

This epic establishes the core abstraction of AxiomStream: a partitioned, append-only log with ordered event delivery.

---

# AS-101 — Topic & Partition Model

## User Story

As a system developer, I want to create and manage topics with multiple partitions so that I can organize event streams and enable scalable message distribution.

## Description

The system must support a logical abstraction of topics, each of which contains multiple partitions. Partitions act as ordered, append-only logs.

## Acceptance Criteria

* Topics can be created dynamically
* Each topic supports multiple partitions
* Partitions maintain ordered event sequences
* Topic names must be unique
* Partitions are assigned stable IDs from `0..N-1`
* Topics are retrievable from the registry/service layer
* Invalid topic creation requests are rejected

---

# AS-102 — Append-Only Log Storage Engine

## User Story

As a system developer, I want events to be stored in an append-only, disk-backed log so that I can ensure durability and replayability of messages.

## Description

Each partition maintains a persistent log stored in disk segments. Events are appended sequentially and can be replayed using offsets.

## Acceptance Criteria

* Events are appended sequentially per partition
* Log data is persisted to disk
* Log segments rotate after configurable size threshold
* Events are assigned monotonically increasing offsets
* System supports reading events by offset
* Corrupted or missing log segments fail safely

---

# AS-103 — Producer API (Single Broker)

## User Story

As a producer application, I want to publish events to a topic so that downstream systems can process them asynchronously.

## Description

A producer interface allows clients to publish events to a topic. The system routes events to partitions using deterministic key hashing.

## Acceptance Criteria

* Exposes API endpoint for producing events
* Accepts topic name, key, and payload
* Routes events to partitions via key hashing
* Returns assigned partition and offset on success
* Rejects writes to non-existent topics
* Handles concurrent writes safely

---

# AS-104 — Consumer API (Single Broker)

## User Story

As a consumer application, I want to read events from a topic partition starting at a specific offset so that I can process messages sequentially and reliably.

## Description

Consumers fetch events from a partition using offset-based reads.

## Acceptance Criteria

* Exposes API endpoint for consuming events
* Supports fetch by topic, partition, and offset
* Returns events in sequential order
* Returns next readable offset
* Handles invalid/out-of-range offsets gracefully
* Supports sequential replay of messages

---

# AS-105 — Basic In-Memory Offset Tracking

## User Story

As a consumer system, I want to track consumer offsets so that I can resume processing from where I left off.

## Description

The system maintains consumer progress per topic-partition-consumer combination.

## Acceptance Criteria

* Stores offsets in memory
* Tracks offsets per consumer ID
* Supports retrieval of last committed offset
* Allows consumer resume from last known offset
* Optional persistence support for restart recovery
