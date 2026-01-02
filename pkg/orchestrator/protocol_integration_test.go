//go:build integration

package orchestrator_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
)

func setupPostgresTestDB(t *testing.T) (*sql.DB, string, func()) {
	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		t.Skip("POSTGRES_URL not set, skipping PostgreSQL integration test")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Create test schema
	testSchema := fmt.Sprintf("orchestrator_test_%d", time.Now().Unix())

	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA %s", testSchema))
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create test schema: %v", err)
	}

	// Create tables
	createTables := fmt.Sprintf(`
		CREATE TABLE %s.recreate_lock (
			lock_id INT PRIMARY KEY DEFAULT 1 CHECK (lock_id = 1),
			generation BIGINT NOT NULL DEFAULT 0,
			phase TEXT NOT NULL DEFAULT 'idle' CHECK (phase IN ('idle', 'draining', 'assigning', 'running')),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		INSERT INTO %s.recreate_lock (lock_id, generation, phase, updated_at)
		VALUES (1, 0, 'idle', NOW());

		CREATE TABLE %s.projection_shards (
			projection_name TEXT NOT NULL,
			shard_id INT NOT NULL,
			shard_count INT NOT NULL,
			last_global_position BIGINT NOT NULL DEFAULT 0,
			owner_id TEXT,
			state TEXT NOT NULL DEFAULT 'idle' CHECK (state IN ('idle', 'assigned', 'draining')),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (projection_name, shard_id),
			CHECK (shard_id >= 0 AND shard_id < shard_count)
		);

		CREATE TABLE %s.workers (
			worker_id TEXT PRIMARY KEY,
			generation_seen BIGINT NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'starting' CHECK (status IN ('starting', 'ready', 'running', 'draining', 'stopped')),
			last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`, testSchema, testSchema, testSchema, testSchema)

	_, err = db.Exec(createTables)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create tables: %v", err)
	}

	cleanup := func() {
		_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", testSchema))
		db.Close()
	}

	return db, testSchema, cleanup
}

func TestProtocolCoordinator_Integration_FullCycle(t *testing.T) {
	db, schemaName, cleanup := setupPostgresTestDB(t)
	defer cleanup()

	// Create protocol persistence
	protocolPersistence, err := orchestrator.NewSQLProtocolPersistence(orchestrator.SQLPersistenceConfig{
		DB:                    db,
		SchemaName:            schemaName,
		RecreateLockTable:     "recreate_lock",
		ProjectionShardsTable: "projection_shards",
		WorkersTable:          "workers",
	})
	if err != nil {
		t.Fatalf("Failed to create protocol persistence: %v", err)
	}

	// Create worker persistence
	workerPersistence, err := orchestrator.NewSQLWorkerPersistence(orchestrator.SQLWorkerPersistenceConfig{
		DB:                db,
		SchemaName:        schemaName,
		WorkersTable:      "workers",
		RecreateLockTable: "recreate_lock",
	})
	if err != nil {
		t.Fatalf("Failed to create worker persistence: %v", err)
	}

	// Create protocol coordinator
	coordinator, err := orchestrator.NewProtocolCoordinator(protocolPersistence)
	if err != nil {
		t.Fatalf("Failed to create protocol coordinator: %v", err)
	}

	ctx := context.Background()

	// Insert test projection shards
	insertShards := fmt.Sprintf(`
		INSERT INTO %s.projection_shards (projection_name, shard_id, shard_count)
		VALUES 
			('test_projection', 0, 4),
			('test_projection', 1, 4),
			('test_projection', 2, 4),
			('test_projection', 3, 4)
	`, schemaName)
	_, err = db.ExecContext(ctx, insertShards)
	if err != nil {
		t.Fatalf("Failed to insert test shards: %v", err)
	}

	// Register test workers
	err = workerPersistence.RegisterWorker(ctx, "worker-1", 0, orchestrator.WorkerStateReady)
	if err != nil {
		t.Fatalf("Failed to register worker-1: %v", err)
	}

	err = workerPersistence.RegisterWorker(ctx, "worker-2", 0, orchestrator.WorkerStateReady)
	if err != nil {
		t.Fatalf("Failed to register worker-2: %v", err)
	}

	// Test 1: Initial state should be idle
	state, err := coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get initial state: %v", err)
	}
	if state.Phase != orchestrator.RecreatePhaseIdle {
		t.Errorf("Expected initial phase to be idle, got: %s", state.Phase)
	}
	if state.Generation != 0 {
		t.Errorf("Expected initial generation to be 0, got: %d", state.Generation)
	}

	// Test 2: Start recreate cycle (idle -> draining)
	generation, err := coordinator.StartRecreate(ctx)
	if err != nil {
		t.Fatalf("Failed to start recreate: %v", err)
	}
	if generation != 1 {
		t.Errorf("Expected generation 1, got: %d", generation)
	}

	state, err = coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get state after start: %v", err)
	}
	if state.Phase != orchestrator.RecreatePhaseDraining {
		t.Errorf("Expected phase to be draining, got: %s", state.Phase)
	}

	// Test 3: Transition to assigning (draining -> assigning)
	// Workers are already in ready state
	transitioned, err := coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Failed to transition to assigning: %v", err)
	}
	if !transitioned {
		t.Error("Expected transition to assigning to succeed")
	}

	state, err = coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get state after transition: %v", err)
	}
	if state.Phase != orchestrator.RecreatePhaseAssigning {
		t.Errorf("Expected phase to be assigning, got: %s", state.Phase)
	}

	// Test 4: Perform shard assignment
	err = coordinator.PerformShardAssignment(ctx)
	if err != nil {
		t.Fatalf("Failed to perform shard assignment: %v", err)
	}

	// Verify shard assignments in database
	var assignedCount int
	queryAssigned := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.projection_shards WHERE owner_id IS NOT NULL AND state = 'assigned'",
		schemaName)
	err = db.QueryRowContext(ctx, queryAssigned).Scan(&assignedCount)
	if err != nil {
		t.Fatalf("Failed to query assigned shards: %v", err)
	}
	if assignedCount != 4 {
		t.Errorf("Expected 4 shards to be assigned, got: %d", assignedCount)
	}

	// Verify each worker has shards
	queryWorkerShards := fmt.Sprintf(
		"SELECT owner_id, COUNT(*) FROM %s.projection_shards WHERE owner_id IS NOT NULL GROUP BY owner_id",
		schemaName)
	rows, err := db.QueryContext(ctx, queryWorkerShards)
	if err != nil {
		t.Fatalf("Failed to query worker shards: %v", err)
	}
	defer rows.Close()

	workerShardCounts := make(map[string]int)
	for rows.Next() {
		var workerID string
		var count int
		if err := rows.Scan(&workerID, &count); err != nil {
			t.Fatalf("Failed to scan worker shard count: %v", err)
		}
		workerShardCounts[workerID] = count
	}

	if len(workerShardCounts) != 2 {
		t.Errorf("Expected 2 workers to have shards, got: %d", len(workerShardCounts))
	}

	// Test 5: Transition to running (assigning -> running)
	err = coordinator.TransitionToRunning(ctx)
	if err != nil {
		t.Fatalf("Failed to transition to running: %v", err)
	}

	state, err = coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get state after running transition: %v", err)
	}
	if state.Phase != orchestrator.RecreatePhaseRunning {
		t.Errorf("Expected phase to be running, got: %s", state.Phase)
	}

	// Test 6: Transition to idle (running -> idle)
	err = coordinator.TransitionToIdle(ctx)
	if err != nil {
		t.Fatalf("Failed to transition to idle: %v", err)
	}

	state, err = coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get final state: %v", err)
	}
	if state.Phase != orchestrator.RecreatePhaseIdle {
		t.Errorf("Expected final phase to be idle, got: %s", state.Phase)
	}
	if state.Generation != 1 {
		t.Errorf("Expected generation to remain 1, got: %d", state.Generation)
	}
}

func TestProtocolCoordinator_Integration_Idempotency(t *testing.T) {
	db, schemaName, cleanup := setupPostgresTestDB(t)
	defer cleanup()

	protocolPersistence, err := orchestrator.NewSQLProtocolPersistence(orchestrator.SQLPersistenceConfig{
		DB:                    db,
		SchemaName:            schemaName,
		RecreateLockTable:     "recreate_lock",
		ProjectionShardsTable: "projection_shards",
		WorkersTable:          "workers",
	})
	if err != nil {
		t.Fatalf("Failed to create protocol persistence: %v", err)
	}

	coordinator, err := orchestrator.NewProtocolCoordinator(protocolPersistence)
	if err != nil {
		t.Fatalf("Failed to create protocol coordinator: %v", err)
	}

	ctx := context.Background()

	// Test: Multiple calls to StartRecreate should be idempotent
	gen1, err := coordinator.StartRecreate(ctx)
	if err != nil {
		t.Fatalf("Failed first StartRecreate: %v", err)
	}

	// Call again while in draining phase
	gen2, err := coordinator.StartRecreate(ctx)
	if err != nil {
		t.Fatalf("Failed second StartRecreate: %v", err)
	}

	if gen1 != gen2 {
		t.Errorf("Expected idempotent generation, got: %d and %d", gen1, gen2)
	}

	state, err := coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}
	if state.Phase != orchestrator.RecreatePhaseDraining {
		t.Errorf("Expected phase to remain draining, got: %s", state.Phase)
	}

	// Test: TryTransitionToAssigning should fail without workers ready
	transitioned, err := coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transitioned {
		t.Error("Expected transition to fail when workers not ready")
	}

	// Transition to assigning by force
	err = protocolPersistence.TransitionPhase(ctx, orchestrator.RecreatePhaseAssigning)
	if err != nil {
		t.Fatalf("Failed to transition to assigning: %v", err)
	}

	// Multiple calls should succeed (idempotent)
	err = protocolPersistence.TransitionPhase(ctx, orchestrator.RecreatePhaseAssigning)
	if err != nil {
		t.Errorf("Expected idempotent phase transition to succeed: %v", err)
	}
}

func TestProtocolCoordinator_Integration_CrashRecovery(t *testing.T) {
	db, schemaName, cleanup := setupPostgresTestDB(t)
	defer cleanup()

	protocolPersistence, err := orchestrator.NewSQLProtocolPersistence(orchestrator.SQLPersistenceConfig{
		DB:                    db,
		SchemaName:            schemaName,
		RecreateLockTable:     "recreate_lock",
		ProjectionShardsTable: "projection_shards",
		WorkersTable:          "workers",
	})
	if err != nil {
		t.Fatalf("Failed to create protocol persistence: %v", err)
	}

	coordinator, err := orchestrator.NewProtocolCoordinator(protocolPersistence)
	if err != nil {
		t.Fatalf("Failed to create protocol coordinator: %v", err)
	}

	ctx := context.Background()

	// Simulate crash scenario: Start recreate, then "crash" and recover
	_, err = coordinator.StartRecreate(ctx)
	if err != nil {
		t.Fatalf("Failed to start recreate: %v", err)
	}

	// Simulate crash by creating new coordinator (new instance)
	coordinator2, err := orchestrator.NewProtocolCoordinator(protocolPersistence)
	if err != nil {
		t.Fatalf("Failed to create second coordinator: %v", err)
	}

	// Should be able to read state after "crash"
	state, err := coordinator2.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get state after crash: %v", err)
	}

	if state.Phase != orchestrator.RecreatePhaseDraining {
		t.Errorf("Expected phase draining after recovery, got: %s", state.Phase)
	}

	// Should be able to continue protocol from where it left off
	// This would normally wait for workers, but we can force transition for testing
	err = protocolPersistence.TransitionPhase(ctx, orchestrator.RecreatePhaseAssigning)
	if err != nil {
		t.Fatalf("Failed to continue protocol after recovery: %v", err)
	}

	state, err = coordinator2.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Failed to get state after continuing: %v", err)
	}

	if state.Phase != orchestrator.RecreatePhaseAssigning {
		t.Errorf("Expected phase assigning after continuing, got: %s", state.Phase)
	}
}
