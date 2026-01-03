package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/google/uuid"
)

// Store is a PostgreSQL implementation of GenerationStore.
// It provides persistent storage for generation and worker metadata.
type Store struct {
	db               *sql.DB
	generationsTable string
	workersTable     string
}

// New creates a new PostgreSQL store with default table names.
func New(db *sql.DB) *Store {
	config := DefaultTableConfig()
	return NewWithConfig(db, config)
}

// NewWithConfig creates a new PostgreSQL store with custom table names.
func NewWithConfig(db *sql.DB, config TableConfig) *Store {
	return &Store{
		db:               db,
		generationsTable: config.GenerationsTable,
		workersTable:     config.WorkersTable,
	}
}

// GetActiveGeneration returns the current active generation for a replica set.
// Returns orchestrator.ErrReplicaSetNotFound if no generation exists for the replica set.
func (s *Store) GetActiveGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
	query := fmt.Sprintf(`
		SELECT id, replica_set, total_partitions, created_at 
		FROM %s 
		WHERE replica_set = $1 
		ORDER BY created_at DESC 
		LIMIT 1
	`, s.generationsTable)

	var gen orchestrator.Generation
	err := s.db.QueryRowContext(ctx, query, string(replicaSet)).Scan(
		&gen.ID,
		&gen.ReplicaSet,
		&gen.TotalPartitions,
		&gen.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}
	if err != nil {
		return orchestrator.Generation{}, fmt.Errorf("failed to get active generation: %w", err)
	}

	return gen, nil
}

// CreateGeneration creates a new generation for a replica set.
// Should be called when the number of workers changes.
// Returns the newly created generation.
func (s *Store) CreateGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
	genID := uuid.New().String()

	query := fmt.Sprintf(`
		INSERT INTO %s (id, replica_set, total_partitions, created_at) 
		VALUES ($1, $2, $3, NOW()) 
		RETURNING created_at
	`, s.generationsTable)

	gen := orchestrator.Generation{
		ID:              genID,
		ReplicaSet:      replicaSet,
		TotalPartitions: totalPartitions,
	}

	err := s.db.QueryRowContext(ctx, query, genID, string(replicaSet), totalPartitions).Scan(&gen.CreatedAt)
	if err != nil {
		return orchestrator.Generation{}, fmt.Errorf("failed to create generation: %w", err)
	}

	return gen, nil
}

// RegisterWorker registers a new worker for a replica set.
// The worker starts in Pending state without a partition assignment.
// Returns the newly registered worker.
func (s *Store) RegisterWorker(ctx context.Context, replicaSet orchestrator.ReplicaSetName, generationID string) (orchestrator.Worker, error) {
	workerID := uuid.New().String()

	query := fmt.Sprintf(`
		INSERT INTO %s (id, replica_set, generation_id, partition_key, state, last_heartbeat, started_at) 
		VALUES ($1, $2, $3, -1, 'pending', NOW(), NOW()) 
		RETURNING last_heartbeat, started_at
	`, s.workersTable)

	worker := orchestrator.Worker{
		ID:           workerID,
		ReplicaSet:   replicaSet,
		GenerationID: generationID,
		PartitionKey: -1,
		State:        orchestrator.WorkerStatePending,
	}

	err := s.db.QueryRowContext(ctx, query, workerID, string(replicaSet), generationID).Scan(
		&worker.LastHeartbeat,
		&worker.StartedAt,
	)
	if err != nil {
		return orchestrator.Worker{}, fmt.Errorf("failed to register worker: %w", err)
	}

	return worker, nil
}

// AssignPartition assigns a partition to a worker.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) AssignPartition(ctx context.Context, workerID string, partitionKey int) error {
	query := fmt.Sprintf(`
		UPDATE %s 
		SET partition_key = $2 
		WHERE id = $1
	`, s.workersTable)

	result, err := s.db.ExecContext(ctx, query, workerID, partitionKey)
	if err != nil {
		return fmt.Errorf("failed to assign partition: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return store.ErrWorkerNotFound
	}

	return nil
}

// UpdateWorkerState updates the state of a worker.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) UpdateWorkerState(ctx context.Context, workerID string, state orchestrator.WorkerState) error {
	query := fmt.Sprintf(`
		UPDATE %s 
		SET state = $2 
		WHERE id = $1
	`, s.workersTable)

	result, err := s.db.ExecContext(ctx, query, workerID, string(state))
	if err != nil {
		return fmt.Errorf("failed to update worker state: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return store.ErrWorkerNotFound
	}

	return nil
}

// Heartbeat updates the last heartbeat time for a worker.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	query := fmt.Sprintf(`
		UPDATE %s 
		SET last_heartbeat = NOW() 
		WHERE id = $1
	`, s.workersTable)

	result, err := s.db.ExecContext(ctx, query, workerID)
	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return store.ErrWorkerNotFound
	}

	return nil
}

// GetWorker returns a worker by ID.
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) GetWorker(ctx context.Context, workerID string) (orchestrator.Worker, error) {
	query := fmt.Sprintf(`
		SELECT id, replica_set, generation_id, partition_key, state, last_heartbeat, started_at 
		FROM %s 
		WHERE id = $1
	`, s.workersTable)

	var worker orchestrator.Worker
	var state string
	err := s.db.QueryRowContext(ctx, query, workerID).Scan(
		&worker.ID,
		&worker.ReplicaSet,
		&worker.GenerationID,
		&worker.PartitionKey,
		&state,
		&worker.LastHeartbeat,
		&worker.StartedAt,
	)

	if err == sql.ErrNoRows {
		return orchestrator.Worker{}, store.ErrWorkerNotFound
	}
	if err != nil {
		return orchestrator.Worker{}, fmt.Errorf("failed to get worker: %w", err)
	}

	worker.State = orchestrator.WorkerState(state)
	return worker, nil
}

// GetWorkersByGeneration returns all workers for a given generation.
// Returns an empty slice if no workers exist for the generation.
func (s *Store) GetWorkersByGeneration(ctx context.Context, generationID string) ([]orchestrator.Worker, error) {
	query := fmt.Sprintf(`
		SELECT id, replica_set, generation_id, partition_key, state, last_heartbeat, started_at 
		FROM %s 
		WHERE generation_id = $1
	`, s.workersTable)

	rows, err := s.db.QueryContext(ctx, query, generationID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workers by generation: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	var workers []orchestrator.Worker
	for rows.Next() {
		var worker orchestrator.Worker
		var state string
		err := rows.Scan(
			&worker.ID,
			&worker.ReplicaSet,
			&worker.GenerationID,
			&worker.PartitionKey,
			&state,
			&worker.LastHeartbeat,
			&worker.StartedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan worker: %w", err)
		}
		worker.State = orchestrator.WorkerState(state)
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workers: %w", err)
	}

	return workers, nil
}

// GetActiveWorkers returns all non-stopped workers for a replica set.
// Returns an empty slice if no active workers exist.
func (s *Store) GetActiveWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
	query := fmt.Sprintf(`
		SELECT id, replica_set, generation_id, partition_key, state, last_heartbeat, started_at 
		FROM %s 
		WHERE replica_set = $1 AND state != 'stopped'
	`, s.workersTable)

	rows, err := s.db.QueryContext(ctx, query, string(replicaSet))
	if err != nil {
		return nil, fmt.Errorf("failed to get active workers: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	var workers []orchestrator.Worker
	for rows.Next() {
		var worker orchestrator.Worker
		var state string
		err := rows.Scan(
			&worker.ID,
			&worker.ReplicaSet,
			&worker.GenerationID,
			&worker.PartitionKey,
			&state,
			&worker.LastHeartbeat,
			&worker.StartedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan worker: %w", err)
		}
		worker.State = orchestrator.WorkerState(state)
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workers: %w", err)
	}

	return workers, nil
}

// GetPendingWorkers returns workers in Pending state for a replica set.
// Returns an empty slice if no pending workers exist.
func (s *Store) GetPendingWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
	query := fmt.Sprintf(`
		SELECT id, replica_set, generation_id, partition_key, state, last_heartbeat, started_at 
		FROM %s 
		WHERE replica_set = $1 AND state = 'pending'
	`, s.workersTable)

	rows, err := s.db.QueryContext(ctx, query, string(replicaSet))
	if err != nil {
		return nil, fmt.Errorf("failed to get pending workers: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close rows: %w", closeErr)
		}
	}()

	var workers []orchestrator.Worker
	for rows.Next() {
		var worker orchestrator.Worker
		var state string
		err := rows.Scan(
			&worker.ID,
			&worker.ReplicaSet,
			&worker.GenerationID,
			&worker.PartitionKey,
			&state,
			&worker.LastHeartbeat,
			&worker.StartedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan worker: %w", err)
		}
		worker.State = orchestrator.WorkerState(state)
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workers: %w", err)
	}

	return workers, nil
}

// MarkWorkerDead marks a worker as stopped (for stale workers).
// Returns store.ErrWorkerNotFound if the worker does not exist.
func (s *Store) MarkWorkerDead(ctx context.Context, workerID string) error {
	query := fmt.Sprintf(`
		UPDATE %s 
		SET state = 'stopped' 
		WHERE id = $1
	`, s.workersTable)

	result, err := s.db.ExecContext(ctx, query, workerID)
	if err != nil {
		return fmt.Errorf("failed to mark worker dead: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return store.ErrWorkerNotFound
	}

	return nil
}
