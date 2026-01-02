package orchestrator

import (
	"context"
	"fmt"
)

// RecreatePhase represents the current phase in the Recreate strategy coordination protocol.
//
// The Recreate protocol enables coordinated deployments across multiple orchestrator workers
// in a distributed system. It ensures that during a deployment/update:
// - All workers drain their current work before reconfiguration
// - Shard ownership is reassigned deterministically
// - Workers resume processing only after all workers are ready
//
// Phase Invariants:
//
// idle:
//   - No coordination activity in progress
//   - Workers may be processing shards normally
//   - Shards may have assigned owners
//   - Safe to initiate new recreate cycle
//   - State is crash-safe: restarting an orchestrator in idle phase is safe
//
// draining:
//   - All workers have been signaled to stop pulling new events
//   - Workers are finishing in-flight event batches
//   - Workers must transition to 'ready' state when done
//   - No new shard assignments are made
//   - Transition to 'assigning' only when all workers are 'ready' or 'stopped'
//   - State is crash-safe: if orchestrator crashes during draining, workers continue
//     draining based on their local state. New orchestrator can resume coordination
//     by checking worker states and waiting for all to be ready.
//
// assigning:
//   - All shard ownership has been cleared
//   - Shard-to-worker mapping is being recalculated
//   - New ownership records are being persisted to projection_shards
//   - Workers are waiting for assignment
//   - Transition to 'running' only after all assignments are complete
//   - State is crash-safe: if orchestrator crashes during assignment, operation
//     is idempotent. Restarted orchestrator can detect partial assignments and
//     either complete them or clear and restart assignment.
//
// running:
//   - Workers have resumed processing their assigned shards
//   - Normal operation until next recreate cycle
//   - Can transition back to 'idle' when recreate cycle completes
//   - State is crash-safe: workers continue processing based on their assigned
//     shards. New orchestrator can observe running state and continue coordination.
//
// Crash-Safety Guarantees:
//
// The protocol is designed to be fully crash-safe at any phase:
//
// 1. All phase transitions use SQL transactions to ensure atomic updates
// 2. Phase transitions are idempotent - can be safely retried
// 3. Workers maintain their state independently and can recover from crashes
// 4. The protocol state (generation + phase) persists in the database
// 5. New orchestrator instances can resume coordination from any phase
// 6. No distributed locks or consensus algorithms required - uses database ACID properties
//
// Recovery Scenarios:
//
// - Orchestrator crashes in idle: New orchestrator reads idle state and continues normally
// - Orchestrator crashes in draining: New orchestrator waits for workers to be ready
// - Orchestrator crashes in assigning: New orchestrator completes shard assignment
// - Orchestrator crashes in running: New orchestrator observes running state
// - Worker crashes: Other workers continue; stale worker is cleaned up by heartbeat timeout
// - Database connection lost: Operations fail and can be retried when connection restored
//
// Late-Joining Workers:
//
// Workers that start after a recreate cycle has begun will:
// 1. Observe the current generation from the database
// 2. Transition to 'ready' state if in draining phase
// 3. Wait for shard assignment if in assigning phase
// 4. Receive assigned shards once protocol reaches running phase
// 5. Begin processing their assigned shards normally
type RecreatePhase string

const (
	// RecreatePhaseIdle indicates normal operation with no coordination activity
	RecreatePhaseIdle RecreatePhase = "idle"

	// RecreatePhaseDraining indicates workers are draining in-flight batches
	RecreatePhaseDraining RecreatePhase = "draining"

	// RecreatePhaseAssigning indicates shard ownership is being recalculated and assigned
	RecreatePhaseAssigning RecreatePhase = "assigning"

	// RecreatePhaseRunning indicates workers are actively processing assigned shards
	RecreatePhaseRunning RecreatePhase = "running"
)

// ProtocolState represents the current state of the Recreate coordination protocol.
type ProtocolState struct {
	Generation int64
	Phase      RecreatePhase
}

// ProtocolPersistence defines the interface for persisting protocol state transitions.
// All operations must be idempotent and crash-safe.
type ProtocolPersistence interface {
	// GetProtocolState retrieves the current protocol state (generation and phase).
	// Returns the current state or an error if retrieval fails.
	GetProtocolState(ctx context.Context) (*ProtocolState, error)

	// TransitionPhase atomically transitions to a new phase within the current generation.
	// Must be idempotent - calling with the same phase multiple times should succeed.
	// Returns error if the transition is invalid or if a database error occurs.
	TransitionPhase(ctx context.Context, newPhase RecreatePhase) error

	// IncrementGeneration atomically increments the generation counter and transitions to draining phase.
	// This starts a new recreate cycle.
	// Must be idempotent based on current phase - if already in draining with higher generation, no-op.
	// Returns the new generation number or an error.
	IncrementGeneration(ctx context.Context) (int64, error)

	// GetReadyWorkerCount returns the count of workers in 'ready' or 'stopped' state.
	// Used to determine when all workers have finished draining.
	GetReadyWorkerCount(ctx context.Context) (int, error)

	// GetTotalActiveWorkerCount returns the count of workers that are not in 'stopped' state.
	// Used to validate all workers are ready before transitioning to assigning phase.
	GetTotalActiveWorkerCount(ctx context.Context) (int, error)

	// ClearAllShardOwnership removes all owner_id assignments from projection_shards.
	// Sets all shards to 'idle' state.
	// Must be idempotent - safe to call multiple times.
	ClearAllShardOwnership(ctx context.Context) error

	// AssignShardsToWorker assigns specific shards of a projection to a worker.
	// Sets shard state to 'assigned' and owner_id to the specified worker.
	// Must be idempotent - reassigning same shards to same worker should succeed.
	AssignShardsToWorker(ctx context.Context, projectionName string, shardIDs []int, workerID string) error

	// GetAllProjectionNames returns all distinct projection names from projection_shards.
	GetAllProjectionNames(ctx context.Context) ([]string, error)

	// GetActiveWorkerIDs returns IDs of all workers not in 'stopped' state.
	GetActiveWorkerIDs(ctx context.Context) ([]string, error)

	// GetShardCount returns the shard_count for a given projection.
	// All shards for a projection must have the same shard_count.
	GetShardCount(ctx context.Context, projectionName string) (int, error)
}

// ProtocolCoordinator manages the Recreate strategy coordination protocol.
// It ensures proper phase transitions and maintains protocol invariants.
type ProtocolCoordinator struct {
	persistence ProtocolPersistence
}

// NewProtocolCoordinator creates a new protocol coordinator.
func NewProtocolCoordinator(persistence ProtocolPersistence) (*ProtocolCoordinator, error) {
	if persistence == nil {
		return nil, fmt.Errorf("persistence cannot be nil")
	}
	return &ProtocolCoordinator{
		persistence: persistence,
	}, nil
}

// GetCurrentState returns the current protocol state.
func (pc *ProtocolCoordinator) GetCurrentState(ctx context.Context) (*ProtocolState, error) {
	return pc.persistence.GetProtocolState(ctx)
}

// StartRecreate initiates a new recreate cycle by incrementing the generation and transitioning to draining.
// This is idempotent - if already in draining phase, it's a no-op.
func (pc *ProtocolCoordinator) StartRecreate(ctx context.Context) (int64, error) {
	generation, err := pc.persistence.IncrementGeneration(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to start recreate cycle: %w", err)
	}
	return generation, nil
}

// TryTransitionToAssigning attempts to transition from draining to assigning phase.
// This succeeds only if all active workers are in 'ready' or 'stopped' state.
// Returns true if transition succeeded, false if workers are still draining.
func (pc *ProtocolCoordinator) TryTransitionToAssigning(ctx context.Context) (bool, error) {
	// Get current state
	state, err := pc.persistence.GetProtocolState(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get protocol state: %w", err)
	}

	// Only transition from draining phase
	if state.Phase != RecreatePhaseDraining {
		return false, nil
	}

	// Check if all active workers are ready
	readyCount, err := pc.persistence.GetReadyWorkerCount(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get ready worker count: %w", err)
	}

	totalCount, err := pc.persistence.GetTotalActiveWorkerCount(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get total worker count: %w", err)
	}

	// All active workers must be ready
	if readyCount < totalCount {
		return false, nil
	}

	// Transition to assigning phase
	if err := pc.persistence.TransitionPhase(ctx, RecreatePhaseAssigning); err != nil {
		return false, fmt.Errorf("failed to transition to assigning: %w", err)
	}

	return true, nil
}

// PerformShardAssignment clears all existing shard ownership and assigns shards to active workers.
// Shards are distributed deterministically across workers using consistent hashing.
// Must only be called when in assigning phase.
func (pc *ProtocolCoordinator) PerformShardAssignment(ctx context.Context) error {
	// Verify we're in assigning phase
	state, err := pc.persistence.GetProtocolState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get protocol state: %w", err)
	}

	if state.Phase != RecreatePhaseAssigning {
		return fmt.Errorf("cannot perform shard assignment in phase %s, must be in assigning phase", state.Phase)
	}

	// Clear all existing shard ownership
	if err := pc.persistence.ClearAllShardOwnership(ctx); err != nil {
		return fmt.Errorf("failed to clear shard ownership: %w", err)
	}

	// Get all active workers
	workerIDs, err := pc.persistence.GetActiveWorkerIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active workers: %w", err)
	}

	// If no workers, nothing to assign
	if len(workerIDs) == 0 {
		return nil
	}

	// Get all projection names
	projections, err := pc.persistence.GetAllProjectionNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get projections: %w", err)
	}

	// Assign shards for each projection
	for _, projectionName := range projections {
		shardCount, err := pc.persistence.GetShardCount(ctx, projectionName)
		if err != nil {
			return fmt.Errorf("failed to get shard count for %s: %w", projectionName, err)
		}

		// Distribute shards across workers deterministically
		assignments := pc.distributeShards(shardCount, workerIDs)

		// Persist assignments
		for workerID, shardIDs := range assignments {
			if len(shardIDs) > 0 {
				if err := pc.persistence.AssignShardsToWorker(ctx, projectionName, shardIDs, workerID); err != nil {
					return fmt.Errorf("failed to assign shards to worker %s: %w", workerID, err)
				}
			}
		}
	}

	return nil
}

// TransitionToRunning transitions from assigning to running phase.
// Should be called after all shard assignments are complete.
func (pc *ProtocolCoordinator) TransitionToRunning(ctx context.Context) error {
	// Verify we're in assigning phase
	state, err := pc.persistence.GetProtocolState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get protocol state: %w", err)
	}

	if state.Phase != RecreatePhaseAssigning {
		return fmt.Errorf("cannot transition to running from phase %s, must be in assigning phase", state.Phase)
	}

	if err := pc.persistence.TransitionPhase(ctx, RecreatePhaseRunning); err != nil {
		return fmt.Errorf("failed to transition to running: %w", err)
	}

	return nil
}

// TransitionToIdle transitions from running to idle phase, completing the recreate cycle.
func (pc *ProtocolCoordinator) TransitionToIdle(ctx context.Context) error {
	// Verify we're in running phase
	state, err := pc.persistence.GetProtocolState(ctx)
	if err != nil {
		return fmt.Errorf("failed to get protocol state: %w", err)
	}

	if state.Phase != RecreatePhaseRunning {
		return fmt.Errorf("cannot transition to idle from phase %s, must be in running phase", state.Phase)
	}

	if err := pc.persistence.TransitionPhase(ctx, RecreatePhaseIdle); err != nil {
		return fmt.Errorf("failed to transition to idle: %w", err)
	}

	return nil
}

// distributeShards distributes shard IDs across workers deterministically.
// Uses round-robin distribution to ensure even distribution.
func (pc *ProtocolCoordinator) distributeShards(shardCount int, workerIDs []string) map[string][]int {
	assignments := make(map[string][]int)
	for _, workerID := range workerIDs {
		assignments[workerID] = []int{}
	}

	// Round-robin distribution
	for shardID := 0; shardID < shardCount; shardID++ {
		workerIndex := shardID % len(workerIDs)
		workerID := workerIDs[workerIndex]
		assignments[workerID] = append(assignments[workerID], shardID)
	}

	return assignments
}
