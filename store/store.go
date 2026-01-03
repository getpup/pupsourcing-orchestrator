package store

import (
	"context"

	"github.com/getpup/pupsourcing-orchestrator"
)

// GenerationStore provides persistence for generation coordination.
// Implementations must be safe for concurrent access from multiple workers.
type GenerationStore interface {
	// GetActiveGeneration returns the current active generation for a replica set.
	// Returns ErrReplicaSetNotFound if no generation exists for the replica set.
	GetActiveGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName) (orchestrator.Generation, error)

	// CreateGeneration creates a new generation for a replica set.
	// Should be called when the number of workers changes.
	// Returns the newly created generation.
	CreateGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error)

	// RegisterWorker registers a new worker for a replica set.
	// The worker starts in Pending state without a partition assignment.
	// Returns the newly registered worker.
	RegisterWorker(ctx context.Context, replicaSet orchestrator.ReplicaSetName, generationID string) (orchestrator.Worker, error)

	// AssignPartition assigns a partition to a worker.
	// Returns an error if the worker does not exist.
	AssignPartition(ctx context.Context, workerID string, partitionKey int) error

	// UpdateWorkerState updates the state of a worker.
	// Returns ErrWorkerNotFound if the worker does not exist.
	UpdateWorkerState(ctx context.Context, workerID string, state orchestrator.WorkerState) error

	// Heartbeat updates the last heartbeat time for a worker.
	// Returns ErrWorkerNotFound if the worker does not exist.
	Heartbeat(ctx context.Context, workerID string) error

	// GetWorker returns a worker by ID.
	// Returns ErrWorkerNotFound if the worker does not exist.
	GetWorker(ctx context.Context, workerID string) (orchestrator.Worker, error)

	// GetWorkersByGeneration returns all workers for a given generation.
	// Returns an empty slice if no workers exist for the generation.
	GetWorkersByGeneration(ctx context.Context, generationID string) ([]orchestrator.Worker, error)

	// GetActiveWorkers returns all non-stopped workers for a replica set.
	// Returns an empty slice if no active workers exist.
	GetActiveWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error)

	// GetPendingWorkers returns workers in Pending state for a replica set.
	// Returns an empty slice if no pending workers exist.
	GetPendingWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error)

	// MarkWorkerDead marks a worker as stopped (for stale workers).
	// Returns ErrWorkerNotFound if the worker does not exist.
	MarkWorkerDead(ctx context.Context, workerID string) error
}
