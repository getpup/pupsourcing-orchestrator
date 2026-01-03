package orchestrator

import "time"

// ReplicaSetName identifies a group of projections that scale together.
// Different replica sets scale independently.
type ReplicaSetName string

// Generation represents a specific partition configuration for a replica set.
// When workers join or leave, a new generation is created with updated partition assignments.
type Generation struct {
	// ID is the unique identifier for this generation (UUID).
	ID string

	// ReplicaSet is the name of the replica set this generation belongs to.
	ReplicaSet ReplicaSetName

	// TotalPartitions is the number of partitions in this generation.
	TotalPartitions int

	// CreatedAt is when this generation was created.
	CreatedAt time.Time
}

// WorkerState represents the lifecycle state of a worker.
type WorkerState string

const (
	// WorkerStatePending indicates the worker is registered and awaiting partition assignment.
	WorkerStatePending WorkerState = "pending"

	// WorkerStateReady indicates the worker has a partition assignment and is ready to start.
	WorkerStateReady WorkerState = "ready"

	// WorkerStateRunning indicates the worker is actively processing events.
	WorkerStateRunning WorkerState = "running"

	// WorkerStateStopping indicates the worker is gracefully stopping.
	WorkerStateStopping WorkerState = "stopping"

	// WorkerStateStopped indicates the worker has fully stopped.
	WorkerStateStopped WorkerState = "stopped"
)

// Worker represents an orchestrator instance for a replica set.
// Each worker processes a specific partition of the event stream.
type Worker struct {
	// ID is the unique identifier for this worker (UUID).
	ID string

	// ReplicaSet is the name of the replica set this worker belongs to.
	ReplicaSet ReplicaSetName

	// GenerationID identifies which generation this worker is part of.
	GenerationID string

	// PartitionKey is this worker's assigned partition (0-indexed).
	// A value of -1 indicates the partition has not yet been assigned.
	PartitionKey int

	// State is the current lifecycle state of the worker.
	State WorkerState

	// LastHeartbeat is the last time this worker reported health.
	LastHeartbeat time.Time

	// StartedAt is when this worker started.
	StartedAt time.Time
}

// PartitionAssignment contains the partition configuration for a worker.
// This is provided to the worker after coordination is complete.
type PartitionAssignment struct {
	// PartitionKey is the specific partition assigned to this worker (0-indexed).
	PartitionKey int

	// TotalPartitions is the total number of partitions in this generation.
	TotalPartitions int

	// GenerationID identifies which generation this assignment belongs to.
	GenerationID string
}
