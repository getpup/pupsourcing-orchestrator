# pupsourcing-orchestrator Development Plan

## Overview

`pupsourcing-orchestrator` is a companion library to `pupsourcing` that handles operational coordination for running 
projections in production.  It implements the **Recreate** strategy:  when workers join or leave, all workers pause, 
reconfigure partition assignments, and restart together.

## Key Concepts

### Replica Set
A **Replica Set** is a named group of projections that scale together. Each replica set:
- Has a unique name (e.g., "main-projections", "analytics-projections")
- Contains one or more projections
- Scales independently of other replica sets
- Coordinates workers via a shared generation store

### Generation
A **Generation** represents a specific partition configuration. When workers join/leave:
1. A new generation is created with updated `TotalPartitions`
2. Old generation workers stop
3. New generation workers start with assigned `PartitionKey` values

### Worker
A **Worker** is an orchestrator instance running a replica set. Workers:
- Register with the generation store
- Receive a `PartitionKey` assignment
- Heartbeat to prove liveness
- Stop when their generation is superseded

## Task Dependency Graph

Phase 1: Foundation
- Task 1: Core Types & Interfaces
- Task 2: Generation Store Interface

Phase 2: Storage (parallel after Phase 1)
- Task 3: In-Memory Generation Store
- Task 4: PostgreSQL Generation Store

Phase 3: Coordination (after Phase 2)
- Task 5: Worker Lifecycle Manager
- Task 6: Generation Coordinator
- Task 7: Partition Assigner

Phase 4: Execution (after Phase 3)
- Task 8: Projection Executor
- Task 9: Orchestrator Implementation

Phase 5: Integration (after Phase 4)
- Task 10: PostgreSQL Integration Tests
- Task 11: End-to-End Integration Tests

Phase 6: Polish (after Phase 5)
- Task 12: Public API & Documentation
- Task 13: CI/CD Setup

## Recreate Strategy Flow

Initial state: No workers

Worker A starts:
1. A registers, becomes partition 0 of 1
2. A creates processors with TotalPartitions=1, PartitionKey=0
3. A runs projections

Worker B starts:
1. B registers, signals "pending"
2. Coordinator detects change, creates new generation (2 partitions)
3. A receives "generation superseded" signal, stops processors
4. A and B coordinate:  A gets partition 0, B gets partition 1
5. Both create processors with TotalPartitions=2
6. Both mark "ready"
7. Coordinator confirms all ready, signals "go"
8. Both start running

Worker C starts:
1. Same process:  all pause, reconfigure to 3 partitions, restart together

Worker B crashes:
1. B's heartbeat stops
2. After timeout, coordinator marks B as dead
3. New generation created with 2 partitions (A and C)
4. A and C stop, reconfigure, restart
