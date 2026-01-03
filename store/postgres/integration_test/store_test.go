//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	pgstore "github.com/getpup/pupsourcing-orchestrator/store/postgres"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain controls test execution and ensures tests run sequentially (not in parallel).
// Integration tests share a database and must not run concurrently.
func TestMain(m *testing.M) {
	// Set parallelism to 1 to ensure tests run sequentially
	// This is critical for integration tests that share database state
	os.Exit(m.Run())
}

// getTestDB returns a database connection for integration tests.
// It reads the DATABASE_URL environment variable and skips the test if not set.
func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	return db
}

// setupTables creates the orchestrator tables using the default configuration.
// It first drops any existing tables to ensure a clean state.
func setupTables(t *testing.T, db *sql.DB) {
	t.Helper()

	config := pgstore.DefaultTableConfig()

	// Drop tables first to ensure clean state (idempotent)
	migrationDown := pgstore.MigrationDown(config)
	if _, err := db.Exec(migrationDown); err != nil {
		t.Logf("warning: failed to drop tables (may not exist): %v", err)
	}

	// Create tables
	migrationSQL := pgstore.MigrationUp(config)
	if _, err := db.Exec(migrationSQL); err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}
}

// cleanupTables truncates the orchestrator tables to clean up test data.
// Errors are logged but don't fail the test (cleanup is best-effort).
func cleanupTables(t *testing.T, db *sql.DB) {
	t.Helper()

	config := pgstore.DefaultTableConfig()

	// TRUNCATE workers table first (has foreign key to generations)
	_, err := db.Exec("TRUNCATE " + config.WorkersTable + " CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate workers table: %v", err)
	}

	// TRUNCATE generations table
	_, err = db.Exec("TRUNCATE " + config.GenerationsTable + " CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate generations table: %v", err)
	}
}

// TestCreateGeneration verifies that generations can be created and stored correctly.
func TestCreateGeneration(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	totalPartitions := 3

	gen, err := s.CreateGeneration(ctx, replicaSet, totalPartitions)
	require.NoError(t, err)

	assert.NotEmpty(t, gen.ID)
	assert.Equal(t, replicaSet, gen.ReplicaSet)
	assert.Equal(t, totalPartitions, gen.TotalPartitions)
	assert.False(t, gen.CreatedAt.IsZero())
	assert.WithinDuration(t, time.Now(), gen.CreatedAt, 5*time.Second)
}

// TestGetActiveGeneration verifies that the most recent generation is returned.
func TestGetActiveGeneration(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	gen1, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	gen2, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	activeGen, err := s.GetActiveGeneration(ctx, replicaSet)
	require.NoError(t, err)

	assert.Equal(t, gen2.ID, activeGen.ID)
	assert.Equal(t, gen2.TotalPartitions, activeGen.TotalPartitions)
	assert.True(t, activeGen.CreatedAt.After(gen1.CreatedAt) || activeGen.CreatedAt.Equal(gen1.CreatedAt))
}

// TestGetActiveGeneration_NoGenerations verifies appropriate error when no generations exist.
func TestGetActiveGeneration_NoGenerations(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("nonexistent-replica-set")

	_, err := s.GetActiveGeneration(ctx, replicaSet)
	assert.ErrorIs(t, err, orchestrator.ErrReplicaSetNotFound)
}

// TestRegisterWorker verifies that workers can be registered correctly.
func TestRegisterWorker(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	assert.NotEmpty(t, worker.ID)
	assert.Equal(t, replicaSet, worker.ReplicaSet)
	assert.Equal(t, gen.ID, worker.GenerationID)
	assert.Equal(t, -1, worker.PartitionKey)
	assert.Equal(t, orchestrator.WorkerStatePending, worker.State)
	assert.False(t, worker.LastHeartbeat.IsZero())
	assert.False(t, worker.StartedAt.IsZero())
}

// TestAssignPartition verifies that partitions can be assigned to workers.
func TestAssignPartition(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 3)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	partitionKey := 1
	err = s.AssignPartition(ctx, worker.ID, partitionKey)
	require.NoError(t, err)

	retrievedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.Equal(t, partitionKey, retrievedWorker.PartitionKey)
}

// TestUpdateWorkerState verifies that worker states can be updated.
func TestUpdateWorkerState(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	stateTransitions := []orchestrator.WorkerState{
		orchestrator.WorkerStateReady,
		orchestrator.WorkerStateRunning,
		orchestrator.WorkerStateStopping,
		orchestrator.WorkerStateStopped,
	}

	for _, state := range stateTransitions {
		err = s.UpdateWorkerState(ctx, worker.ID, state)
		require.NoError(t, err)

		retrievedWorker, err := s.GetWorker(ctx, worker.ID)
		require.NoError(t, err)
		assert.Equal(t, state, retrievedWorker.State)
	}
}

// TestHeartbeat verifies that worker heartbeats are updated.
func TestHeartbeat(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	initialHeartbeat := worker.LastHeartbeat

	time.Sleep(100 * time.Millisecond)

	err = s.Heartbeat(ctx, worker.ID)
	require.NoError(t, err)

	retrievedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.True(t, retrievedWorker.LastHeartbeat.After(initialHeartbeat))
}

// TestGetWorkersByGeneration verifies that workers can be retrieved by generation.
func TestGetWorkersByGeneration(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	gen1, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	gen2, err := s.CreateGeneration(ctx, replicaSet, 3)
	require.NoError(t, err)

	worker1, err := s.RegisterWorker(ctx, replicaSet, gen1.ID)
	require.NoError(t, err)

	worker2, err := s.RegisterWorker(ctx, replicaSet, gen1.ID)
	require.NoError(t, err)

	worker3, err := s.RegisterWorker(ctx, replicaSet, gen2.ID)
	require.NoError(t, err)

	gen1Workers, err := s.GetWorkersByGeneration(ctx, gen1.ID)
	require.NoError(t, err)
	assert.Len(t, gen1Workers, 2)

	gen1WorkerIDs := []string{gen1Workers[0].ID, gen1Workers[1].ID}
	assert.Contains(t, gen1WorkerIDs, worker1.ID)
	assert.Contains(t, gen1WorkerIDs, worker2.ID)

	gen2Workers, err := s.GetWorkersByGeneration(ctx, gen2.ID)
	require.NoError(t, err)
	assert.Len(t, gen2Workers, 1)
	assert.Equal(t, worker3.ID, gen2Workers[0].ID)
}

// TestGetActiveWorkers verifies that stopped workers are excluded.
func TestGetActiveWorkers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 3)
	require.NoError(t, err)

	worker1, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker2, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker3, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	err = s.UpdateWorkerState(ctx, worker1.ID, orchestrator.WorkerStateRunning)
	require.NoError(t, err)

	err = s.UpdateWorkerState(ctx, worker2.ID, orchestrator.WorkerStateStopped)
	require.NoError(t, err)

	activeWorkers, err := s.GetActiveWorkers(ctx, replicaSet)
	require.NoError(t, err)

	assert.Len(t, activeWorkers, 2)

	activeWorkerIDs := []string{activeWorkers[0].ID, activeWorkers[1].ID}
	assert.Contains(t, activeWorkerIDs, worker1.ID)
	assert.Contains(t, activeWorkerIDs, worker3.ID)
	assert.NotContains(t, activeWorkerIDs, worker2.ID)
}

// TestGetPendingWorkers verifies that only pending workers are returned.
func TestGetPendingWorkers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 3)
	require.NoError(t, err)

	worker1, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker2, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker3, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	err = s.UpdateWorkerState(ctx, worker2.ID, orchestrator.WorkerStateRunning)
	require.NoError(t, err)

	pendingWorkers, err := s.GetPendingWorkers(ctx, replicaSet)
	require.NoError(t, err)

	assert.Len(t, pendingWorkers, 2)

	pendingWorkerIDs := []string{pendingWorkers[0].ID, pendingWorkers[1].ID}
	assert.Contains(t, pendingWorkerIDs, worker1.ID)
	assert.Contains(t, pendingWorkerIDs, worker3.ID)
	assert.NotContains(t, pendingWorkerIDs, worker2.ID)
}

// TestMarkWorkerDead verifies that workers can be marked as dead.
func TestMarkWorkerDead(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateRunning)
	require.NoError(t, err)

	err = s.MarkWorkerDead(ctx, worker.ID)
	require.NoError(t, err)

	retrievedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.Equal(t, orchestrator.WorkerStateStopped, retrievedWorker.State)
}

// TestConcurrentAccess verifies that the store handles concurrent access correctly.
func TestConcurrentAccess(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 10)
	require.NoError(t, err)

	var wg sync.WaitGroup
	numWorkers := 10

	workerIDs := make([]string, numWorkers)
	errors := make([]error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
			if err != nil {
				errors[index] = err
				return
			}
			workerIDs[index] = worker.ID
		}(i)
	}

	wg.Wait()

	for i, err := range errors {
		assert.NoError(t, err, "worker %d failed to register", i)
	}

	workers, err := s.GetWorkersByGeneration(ctx, gen.ID)
	require.NoError(t, err)
	assert.Len(t, workers, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			if workerIDs[index] == "" {
				return
			}

			for j := 0; j < 5; j++ {
				err := s.Heartbeat(ctx, workerIDs[index])
				if err != nil {
					errors[index] = err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	for i, err := range errors {
		assert.NoError(t, err, "worker %d failed heartbeat", i)
	}
}

// TestGetWorker_NotFound verifies error when getting non-existent worker.
func TestGetWorker_NotFound(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	// Use a valid UUID format that doesn't exist in the database
	_, err := s.GetWorker(ctx, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, store.ErrWorkerNotFound)
}

// TestAssignPartition_WorkerNotFound verifies error when assigning partition to non-existent worker.
func TestAssignPartition_WorkerNotFound(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	// Use a valid UUID format that doesn't exist in the database
	err := s.AssignPartition(ctx, "00000000-0000-0000-0000-000000000000", 0)
	assert.ErrorIs(t, err, store.ErrWorkerNotFound)
}

// TestUpdateWorkerState_WorkerNotFound verifies error when updating state of non-existent worker.
func TestUpdateWorkerState_WorkerNotFound(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	// Use a valid UUID format that doesn't exist in the database
	err := s.UpdateWorkerState(ctx, "00000000-0000-0000-0000-000000000000", orchestrator.WorkerStateRunning)
	assert.ErrorIs(t, err, store.ErrWorkerNotFound)
}

// TestHeartbeat_WorkerNotFound verifies error when heartbeating non-existent worker.
func TestHeartbeat_WorkerNotFound(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	// Use a valid UUID format that doesn't exist in the database
	err := s.Heartbeat(ctx, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, store.ErrWorkerNotFound)
}

// TestMarkWorkerDead_WorkerNotFound verifies error when marking non-existent worker as dead.
func TestMarkWorkerDead_WorkerNotFound(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	// Use a valid UUID format that doesn't exist in the database
	err := s.MarkWorkerDead(ctx, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, store.ErrWorkerNotFound)
}

// TestTransactionIsolation verifies that operations are atomic.
func TestTransactionIsolation(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			state := orchestrator.WorkerStateReady
			if index%2 == 0 {
				state = orchestrator.WorkerStateRunning
			}
			_ = s.UpdateWorkerState(ctx, worker.ID, state)
		}(i)
	}

	wg.Wait()

	retrievedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, retrievedWorker.State)
	assert.True(t, retrievedWorker.State == orchestrator.WorkerStateReady || retrievedWorker.State == orchestrator.WorkerStateRunning)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Heartbeat(ctx, worker.ID)
		}()
	}

	wg.Wait()

	finalWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.False(t, finalWorker.LastHeartbeat.IsZero())
}

// TestRegisterWorker_GenerationNotFound verifies error when registering worker with non-existent generation.
func TestRegisterWorker_GenerationNotFound(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	// Use a valid UUID format that doesn't exist in the database
	_, err := s.RegisterWorker(ctx, replicaSet, "00000000-0000-0000-0000-000000000000")
	assert.ErrorIs(t, err, store.ErrGenerationNotFound)
}

// TestGetWorkersByGeneration_NoWorkers verifies empty slice is returned when no workers exist.
func TestGetWorkersByGeneration_NoWorkers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	gen, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	workers, err := s.GetWorkersByGeneration(ctx, gen.ID)
	require.NoError(t, err)
	assert.Empty(t, workers)
}

// TestGetActiveWorkers_NoWorkers verifies empty slice is returned when no active workers exist.
func TestGetActiveWorkers_NoWorkers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	workers, err := s.GetActiveWorkers(ctx, replicaSet)
	require.NoError(t, err)
	assert.Empty(t, workers)
}

// TestGetPendingWorkers_NoWorkers verifies empty slice is returned when no pending workers exist.
func TestGetPendingWorkers_NoWorkers(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTables(t, db)
	defer cleanupTables(t, db)

	s := pgstore.New(db)
	ctx := context.Background()

	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	workers, err := s.GetPendingWorkers(ctx, replicaSet)
	require.NoError(t, err)
	assert.Empty(t, workers)
}
