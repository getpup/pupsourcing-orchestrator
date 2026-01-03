package memory

import (
	"context"
	"sync"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/google/uuid"
)

// Store is an in-memory implementation of GenerationStore for testing.
// It provides thread-safe access to generation and worker data using a sync.RWMutex.
type Store struct {
	mu          sync.RWMutex
	generations map[string]orchestrator.Generation     // generationID -> generation
	workers     map[string]orchestrator.Worker         // workerID -> worker
	activeGen   map[orchestrator.ReplicaSetName]string // replicaSet -> active generationID
}

// New creates a new in-memory store with initialized maps.
func New() *Store {
	return &Store{
		generations: make(map[string]orchestrator.Generation),
		workers:     make(map[string]orchestrator.Worker),
		activeGen:   make(map[orchestrator.ReplicaSetName]string),
	}
}

// GetActiveGeneration returns the current active generation for a replica set.
// Returns orchestrator.ErrReplicaSetNotFound if no generation exists for the replica set.
func (s *Store) GetActiveGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	genID, ok := s.activeGen[replicaSet]
	if !ok {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	gen, ok := s.generations[genID]
	if !ok {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	return gen, nil
}

// CreateGeneration creates a new generation for a replica set.
// Should be called when the number of workers changes.
// Returns the newly created generation.
func (s *Store) CreateGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	genID := uuid.New().String()
	gen := orchestrator.Generation{
		ID:              genID,
		ReplicaSet:      replicaSet,
		TotalPartitions: totalPartitions,
		CreatedAt:       time.Now(),
	}

	s.generations[genID] = gen
	s.activeGen[replicaSet] = genID

	return gen, nil
}

// RegisterWorker registers a new worker for a replica set.
// The worker starts in Pending state without a partition assignment.
// Returns the newly registered worker.
func (s *Store) RegisterWorker(ctx context.Context, replicaSet orchestrator.ReplicaSetName, generationID string) (orchestrator.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate that generationID exists
	if _, ok := s.generations[generationID]; !ok {
		return orchestrator.Worker{}, store.ErrGenerationNotFound
	}

	workerID := uuid.New().String()
	now := time.Now()
	worker := orchestrator.Worker{
		ID:            workerID,
		ReplicaSet:    replicaSet,
		GenerationID:  generationID,
		PartitionKey:  -1,
		State:         orchestrator.WorkerStatePending,
		LastHeartbeat: now,
		StartedAt:     now,
	}

	s.workers[workerID] = worker

	return worker, nil
}

// AssignPartition assigns a partition to a worker.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) AssignPartition(ctx context.Context, workerID string, partitionKey int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return store.ErrWorkerNotFound
	}

	worker.PartitionKey = partitionKey
	s.workers[workerID] = worker

	return nil
}

// UpdateWorkerState updates the state of a worker.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) UpdateWorkerState(ctx context.Context, workerID string, state orchestrator.WorkerState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return store.ErrWorkerNotFound
	}

	worker.State = state
	s.workers[workerID] = worker

	return nil
}

// Heartbeat updates the last heartbeat time for a worker.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return store.ErrWorkerNotFound
	}

	worker.LastHeartbeat = time.Now()
	s.workers[workerID] = worker

	return nil
}

// GetWorker returns a worker by ID.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) GetWorker(ctx context.Context, workerID string) (orchestrator.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return orchestrator.Worker{}, store.ErrWorkerNotFound
	}

	return worker, nil
}

// GetWorkersByGeneration returns all workers for a given generation.
// Returns an empty slice if no workers exist for the generation.
func (s *Store) GetWorkersByGeneration(ctx context.Context, generationID string) ([]orchestrator.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workers []orchestrator.Worker
	for _, worker := range s.workers {
		if worker.GenerationID == generationID {
			workers = append(workers, worker)
		}
	}

	return workers, nil
}

// GetActiveWorkers returns all non-stopped workers for a replica set.
// Returns an empty slice if no active workers exist.
func (s *Store) GetActiveWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workers []orchestrator.Worker
	for _, worker := range s.workers {
		if worker.ReplicaSet == replicaSet && worker.State != orchestrator.WorkerStateStopped {
			workers = append(workers, worker)
		}
	}

	return workers, nil
}

// GetPendingWorkers returns workers in Pending state for a replica set.
// Returns an empty slice if no pending workers exist.
func (s *Store) GetPendingWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workers []orchestrator.Worker
	for _, worker := range s.workers {
		if worker.ReplicaSet == replicaSet && worker.State == orchestrator.WorkerStatePending {
			workers = append(workers, worker)
		}
	}

	return workers, nil
}

// MarkWorkerDead marks a worker as stopped (for stale workers).
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) MarkWorkerDead(ctx context.Context, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return store.ErrWorkerNotFound
	}

	worker.State = orchestrator.WorkerStateStopped
	s.workers[workerID] = worker

	return nil
}
