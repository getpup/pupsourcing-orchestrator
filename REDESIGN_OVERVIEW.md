# Orchestrator Redesign: Leader Election + Generation State Machine

## Problem Analysis

The current design has fundamental race conditions: 

1. **Distributed decision making:** Multiple workers independently check ShouldTriggerReconfiguration() and may all try to create new generations simultaneously

2. **Unstable leader election:** IsLeader() determines leader by lexicographically smallest worker ID in a generation, but workers register at different times - the "leader" can change mid-coordination

3. **Implicit state transitions:** Generation lifecycle is implicit (existence = active, newer exists = superseded) rather than explicit states

4. **Time-of-check-time-of-use (TOCTOU) races:** Between checking a condition and acting on it, state can change

## Solution:  Explicit Generation State Machine with Database-Level Leader Election

### Core Principles

1. **Single leader per generation:** Use database-level locking (SELECT FOR UPDATE SKIP LOCKED) to elect exactly one leader

2. **Explicit generation states:** Generations have explicit states (pending, active, superseded) stored in the database

3. **Leader drives all transitions:** Only the leader makes decisions and transitions generation state

4. **Workers poll for state:** Non-leader workers simply poll and react to state changes

5. **Atomic operations:** All critical operations use database transactions

### Generation State Machine

    pending ──────► active ──────► superseded
       │                              ▲
       │                              │
       └──────────────────────────────┘
              (if leader abandons)

States:
- pending: Generation created, workers registering, leader coordinating
- active:  All workers running, processing events  
- superseded:  New generation created, workers should stop

### New Coordination Flow

1. Worker starts, tries to acquire leader lock for current generation
2. If becomes leader: 
   - Wait for registration window (configurable time for workers to register)
   - Assign partitions to all registered workers
   - Transition generation:  pending -> active
   - Release lock, start processing
3. If not leader:
   - Register as worker
   - Poll for generation state = active
   - Once active, get partition assignment and start processing
4. All workers watch for generation state = superseded
5. When superseded detected, stop and rejoin coordination

### Key Changes Required

1. Add state column to generations table
2. Add leader_id column to track current leader
3. Create AcquireLeaderLock store method using SELECT FOR UPDATE SKIP LOCKED
4. Create TransitionGenerationState store method  
5. Refactor Coordinator to use leader-driven flow
6. Refactor Orchestrator Run loop for new coordination

## Task Breakdown

- Task R1: Database Schema Migration (add state, leader_id columns)
- Task R2: Update Core Types (Generation with State)
- Task R3: Update GenerationStore Interface (new methods)
- Task R4: Update Memory Store Implementation
- Task R5: Update PostgreSQL Store Implementation
- Task R6: Implement Leader Election Mechanism
- Task R7: Refactor Coordinator for Leader-Driven Flow
- Task R8:  Refactor Orchestrator Run Loop
- Task R9: Update Unit Tests
- Task R10: Update Integration Tests
- Task R11: Update Documentation

## Benefits of This Approach

1. **No race conditions:** Only one leader makes decisions per generation
2. **Predictable behavior:** Explicit state machine with clear transitions
3. **Database guarantees:** Uses PostgreSQL row-level locking for correctness
4. **Simpler debugging:** States are visible in database, easy to inspect
5. **Industry proven:** This pattern is used by Kafka, Kubernetes, etc. 