package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SQLWorkerPersistence is a SQL-based implementation of WorkerPersistenceAdapter.
// It provides worker registration, heartbeat, and lifecycle management.
type SQLWorkerPersistence struct {
	db                *sql.DB
	schemaName        string
	workersTable      string
	recreateLockTable string
}

// SQLWorkerPersistenceConfig configures the SQL worker persistence adapter.
type SQLWorkerPersistenceConfig struct {
	// DB is the database connection
	DB *sql.DB

	// SchemaName is the schema/database name for tables
	SchemaName string

	// WorkersTable is the table name for workers
	// Default: "workers"
	WorkersTable string

	// RecreateLockTable is the table name for the recreate lock
	// Default: "recreate_lock"
	RecreateLockTable string
}

// NewSQLWorkerPersistence creates a new SQL worker persistence adapter.
func NewSQLWorkerPersistence(config SQLWorkerPersistenceConfig) (*SQLWorkerPersistence, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	// Set defaults
	if config.SchemaName == "" {
		config.SchemaName = "orchestrator"
	}
	if config.WorkersTable == "" {
		config.WorkersTable = "workers"
	}
	if config.RecreateLockTable == "" {
		config.RecreateLockTable = "recreate_lock"
	}

	return &SQLWorkerPersistence{
		db:                config.DB,
		schemaName:        config.SchemaName,
		workersTable:      config.WorkersTable,
		recreateLockTable: config.RecreateLockTable,
	}, nil
}

// RegisterWorker inserts or updates a worker record in the database.
func (s *SQLWorkerPersistence) RegisterWorker(ctx context.Context, workerID string, generation int64, state WorkerState) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s (worker_id, generation_seen, status, last_heartbeat)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (worker_id) 
		DO UPDATE SET generation_seen = $2, status = $3, last_heartbeat = $4`,
		s.schemaName, s.workersTable)

	_, err := s.db.ExecContext(ctx, query, workerID, generation, string(state), time.Now())
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	return nil
}

// UpdateWorkerHeartbeat updates the last_heartbeat timestamp for a worker.
func (s *SQLWorkerPersistence) UpdateWorkerHeartbeat(ctx context.Context, workerID string, state WorkerState) error {
	query := fmt.Sprintf(
		"UPDATE %s.%s SET last_heartbeat = $1, status = $2 WHERE worker_id = $3",
		s.schemaName, s.workersTable)

	result, err := s.db.ExecContext(ctx, query, time.Now(), string(state), workerID)
	if err != nil {
		return fmt.Errorf("failed to update worker heartbeat: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("worker not found: %s", workerID)
	}

	return nil
}

// UpdateWorkerState updates the worker's state.
func (s *SQLWorkerPersistence) UpdateWorkerState(ctx context.Context, workerID string, state WorkerState, generation int64) error {
	query := fmt.Sprintf(
		"UPDATE %s.%s SET status = $1, generation_seen = $2 WHERE worker_id = $3",
		s.schemaName, s.workersTable)

	result, err := s.db.ExecContext(ctx, query, string(state), generation, workerID)
	if err != nil {
		return fmt.Errorf("failed to update worker state: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("worker not found: %s", workerID)
	}

	return nil
}

// GetCurrentGeneration retrieves the current recreate generation.
func (s *SQLWorkerPersistence) GetCurrentGeneration(ctx context.Context) (int64, error) {
	query := fmt.Sprintf(
		"SELECT generation FROM %s.%s WHERE lock_id = 1",
		s.schemaName, s.recreateLockTable)

	var generation int64
	err := s.db.QueryRowContext(ctx, query).Scan(&generation)
	if err != nil {
		return 0, fmt.Errorf("failed to get current generation: %w", err)
	}

	return generation, nil
}

// DeleteStaleWorkers removes workers whose last heartbeat is older than the threshold.
func (s *SQLWorkerPersistence) DeleteStaleWorkers(ctx context.Context, staleThreshold time.Duration) error {
	cutoffTime := time.Now().Add(-staleThreshold)

	query := fmt.Sprintf(
		"DELETE FROM %s.%s WHERE last_heartbeat < $1",
		s.schemaName, s.workersTable)

	_, err := s.db.ExecContext(ctx, query, cutoffTime)
	if err != nil {
		return fmt.Errorf("failed to delete stale workers: %w", err)
	}

	return nil
}
