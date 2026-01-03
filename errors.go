package orchestrator

import "errors"

var (
	// ErrGenerationSuperseded indicates this generation has been replaced by a newer one.
	// Workers should stop processing and await a new partition assignment.
	ErrGenerationSuperseded = errors.New("generation superseded")

	// ErrWorkerNotFound indicates the specified worker does not exist in the generation store.
	ErrWorkerNotFound = errors.New("worker not found")

	// ErrReplicaSetNotFound indicates no active generation exists for the replica set.
	ErrReplicaSetNotFound = errors.New("replica set not found")

	// ErrPartitionNotAssigned indicates the worker doesn't have a partition assignment yet.
	// This occurs when a worker is in the pending state.
	ErrPartitionNotAssigned = errors.New("partition not assigned")

	// ErrCoordinationTimeout indicates coordination took too long to complete.
	// This may occur when workers fail to reach consensus on partition assignments.
	ErrCoordinationTimeout = errors.New("coordination timeout")
)
