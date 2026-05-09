# AxiomStream System Overview

## Purpose

AxiomStream is a Java-based event streaming platform inspired by systems like Apache Kafka.

The goal is to build a distributed messaging system from first principles, starting with a single-node broker that supports topics, partitions, producers, consumers, and append-only log storage.

## EPIC 1 Goal

EPIC 1 focuses on the MVP foundation:

- Topic and partition management
- Append-only log storage
- Producer publishing
- Consumer fetching
- Basic offset tracking

## High-Level Architecture

```text
Producer
   |
   v
Broker API
   |
   v
Topic Registry
   |
   v
Topic
   |
   +--> Partition 0 --> Append-Only Log
   +--> Partition 1 --> Append-Only Log
   +--> Partition 2 --> Append-Only Log

Consumer
   |
   v
Broker API
   |
   v
Read from Topic Partition by Offset