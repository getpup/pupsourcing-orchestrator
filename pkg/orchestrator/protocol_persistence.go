package orchestrator

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SQLProtocolPersistence is a SQL-based implementation of ProtocolPersistence.
// It provides crash-safe, transactional protocol state management using
// standard SQL with READ COMMITTED isolation level.
//
// All operations are idempotent and designed to recover from crashes at any point.
type SQLProtocolPersistence struct {
	db                    *sql.DB
	schemaName            string
	recreateLockTable     string
	projectionShardsTable string
	workersTable          string
}

// SQLPersistenceConfig configures the SQL persistence adapter.
type SQLPersistenceConfig struct {
	// DB is the database connection
	DB *sql.DB

	// SchemaName is the schema/database name for tables
	// For PostgreSQL: schema name (default: "orchestrator")
	// For MySQL: database name (default: "orchestrator")
	// For SQLite: prefix for table names (default: "orchestrator")
	SchemaName string

	// RecreateLockTable is the table name for the recreate lock
	// Default: "recreate_lock"
	RecreateLockTable string

	// ProjectionShardsTable is the table name for projection shards
	// Default: "projection_shards"
	ProjectionShardsTable string

	// WorkersTable is the table name for workers
	// Default: "workers"
	WorkersTable string
}

// NewSQLProtocolPersistence creates a new SQL protocol persistence adapter.
func NewSQLProtocolPersistence(config SQLPersistenceConfig) (*SQLProtocolPersistence, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	// Set defaults
	if config.SchemaName == "" {
		config.SchemaName = "orchestrator"
	}
	if config.RecreateLockTable == "" {
		config.RecreateLockTable = "recreate_lock"
	}
	if config.ProjectionShardsTable == "" {
		config.ProjectionShardsTable = "projection_shards"
	}
	if config.WorkersTable == "" {
		config.WorkersTable = "workers"
	}

	return &SQLProtocolPersistence{
		db:                    config.DB,
		schemaName:            config.SchemaName,
		recreateLockTable:     config.RecreateLockTable,
		projectionShardsTable: config.ProjectionShardsTable,
		workersTable:          config.WorkersTable,
	}, nil
}

// GetProtocolState retrieves the current protocol state (generation and phase).
func (s *SQLProtocolPersistence) GetProtocolState(ctx context.Context) (*ProtocolState, error) {
	query := fmt.Sprintf("SELECT generation, phase FROM %s.%s WHERE lock_id = 1",
		s.schemaName, s.recreateLockTable)

	var generation int64
	var phase string

	err := s.db.QueryRowContext(ctx, query).Scan(&generation, &phase)
	if err != nil {
		return nil, fmt.Errorf("failed to get protocol state: %w", err)
	}

	return &ProtocolState{
		Generation: generation,
		Phase:      RecreatePhase(phase),
	}, nil
}

// TransitionPhase atomically transitions to a new phase within the current generation.
// This operation is idempotent - calling with the same phase multiple times succeeds.
func (s *SQLProtocolPersistence) TransitionPhase(ctx context.Context, newPhase RecreatePhase) error {
	query := fmt.Sprintf(
		"UPDATE %s.%s SET phase = $1, updated_at = $2 WHERE lock_id = 1",
		s.schemaName, s.recreateLockTable)

	result, err := s.db.ExecContext(ctx, query, string(newPhase), time.Now())
	if err != nil {
		return fmt.Errorf("failed to transition phase: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no rows updated, lock_id=1 not found")
	}

	return nil
}

// IncrementGeneration atomically increments the generation counter and transitions to draining phase.
// This starts a new recreate cycle.
// Idempotent: If already in draining or later phase with same or higher generation, returns current generation.
func (s *SQLProtocolPersistence) IncrementGeneration(ctx context.Context) (int64, error) {
	// Start a transaction for atomic increment and phase transition
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get current state
	selectQuery := fmt.Sprintf("SELECT generation, phase FROM %s.%s WHERE lock_id = 1 FOR UPDATE",
		s.schemaName, s.recreateLockTable)

	var currentGen int64
	var currentPhase string
	err = tx.QueryRowContext(ctx, selectQuery).Scan(&currentGen, &currentPhase)
	if err != nil {
		return 0, fmt.Errorf("failed to get current generation: %w", err)
	}

	// If already in draining phase or later, return current generation (idempotent)
	if currentPhase != string(RecreatePhaseIdle) && currentPhase != string(RecreatePhaseRunning) {
		// Already in progress, return current generation
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return currentGen, nil
	}

	// Increment generation and transition to draining
	newGen := currentGen + 1
	updateQuery := fmt.Sprintf(
		"UPDATE %s.%s SET generation = $1, phase = $2, updated_at = $3 WHERE lock_id = 1",
		s.schemaName, s.recreateLockTable)

	_, err = tx.ExecContext(ctx, updateQuery, newGen, string(RecreatePhaseDraining), time.Now())
	if err != nil {
		return 0, fmt.Errorf("failed to increment generation: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return newGen, nil
}

// GetReadyWorkerCount returns the count of workers in 'ready' or 'stopped' state.
func (s *SQLProtocolPersistence) GetReadyWorkerCount(ctx context.Context) (int, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s WHERE status IN ('ready', 'stopped')",
		s.schemaName, s.workersTable)

	var count int
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get ready worker count: %w", err)
	}

	return count, nil
}

// GetTotalActiveWorkerCount returns the count of workers that are not in 'stopped' state.
func (s *SQLProtocolPersistence) GetTotalActiveWorkerCount(ctx context.Context) (int, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s WHERE status != 'stopped'",
		s.schemaName, s.workersTable)

	var count int
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total active worker count: %w", err)
	}

	return count, nil
}

// ClearAllShardOwnership removes all owner_id assignments from projection_shards.
// Sets all shards to 'idle' state.
// This operation is idempotent - safe to call multiple times.
func (s *SQLProtocolPersistence) ClearAllShardOwnership(ctx context.Context) error {
	query := fmt.Sprintf(
		"UPDATE %s.%s SET owner_id = NULL, state = 'idle', updated_at = $1",
		s.schemaName, s.projectionShardsTable)

	_, err := s.db.ExecContext(ctx, query, time.Now())
	if err != nil {
		return fmt.Errorf("failed to clear shard ownership: %w", err)
	}

	return nil
}

// AssignShardsToWorker assigns specific shards of a projection to a worker.
// Sets shard state to 'assigned' and owner_id to the specified worker.
// This operation is idempotent - reassigning same shards to same worker succeeds.
func (s *SQLProtocolPersistence) AssignShardsToWorker(ctx context.Context, projectionName string, shardIDs []int, workerID string) error {
	if len(shardIDs) == 0 {
		return nil // Nothing to assign
	}

	// Build parameterized query for multiple shard IDs
	// We need to update each shard individually or use a more complex query
	// For simplicity and safety, we'll use a transaction with individual updates
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	updateQuery := fmt.Sprintf(
		"UPDATE %s.%s SET owner_id = $1, state = 'assigned', updated_at = $2 WHERE projection_name = $3 AND shard_id = $4",
		s.schemaName, s.projectionShardsTable)

	now := time.Now()
	for _, shardID := range shardIDs {
		_, err := tx.ExecContext(ctx, updateQuery, workerID, now, projectionName, shardID)
		if err != nil {
			return fmt.Errorf("failed to assign shard %d: %w", shardID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetAllProjectionNames returns all distinct projection names from projection_shards.
func (s *SQLProtocolPersistence) GetAllProjectionNames(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf(
		"SELECT DISTINCT projection_name FROM %s.%s ORDER BY projection_name",
		s.schemaName, s.projectionShardsTable)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query projection names: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan projection name: %w", err)
		}
		names = append(names, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating projection names: %w", err)
	}

	return names, nil
}

// GetActiveWorkerIDs returns IDs of all workers not in 'stopped' state.
func (s *SQLProtocolPersistence) GetActiveWorkerIDs(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf(
		"SELECT worker_id FROM %s.%s WHERE status != 'stopped' ORDER BY worker_id",
		s.schemaName, s.workersTable)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active workers: %w", err)
	}
	defer rows.Close()

	var workerIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan worker ID: %w", err)
		}
		workerIDs = append(workerIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating worker IDs: %w", err)
	}

	return workerIDs, nil
}

// GetShardCount returns the shard_count for a given projection.
// All shards for a projection must have the same shard_count.
func (s *SQLProtocolPersistence) GetShardCount(ctx context.Context, projectionName string) (int, error) {
	query := fmt.Sprintf(
		"SELECT shard_count FROM %s.%s WHERE projection_name = $1 LIMIT 1",
		s.schemaName, s.projectionShardsTable)

	var count int
	err := s.db.QueryRowContext(ctx, query, projectionName).Scan(&count)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("projection not found: %s", projectionName)
		}
		return 0, fmt.Errorf("failed to get shard count: %w", err)
	}

	return count, nil
}
