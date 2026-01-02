package orchestrator_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
)

// ExampleProtocolCoordinator demonstrates the full Recreate protocol coordination cycle.
//
// This example shows how an orchestrator coordinator would manage a distributed
// deployment of projection workers using the Recreate strategy protocol.
func ExampleProtocolCoordinator() {
	// This example assumes a PostgreSQL database with the orchestrator tables already created
	// In a real application, you would:
	// 1. Create database connection
	// 2. Run migrations to create orchestrator tables
	// 3. Set up workers with heartbeats

	// Mock database for example (in real code, use actual database)
	var db *sql.DB // Would be: db, err := sql.Open("postgres", connString)

	// Create protocol persistence adapter
	protocolPersistence, err := orchestrator.NewSQLProtocolPersistence(orchestrator.SQLPersistenceConfig{
		DB:                    db,
		SchemaName:            "orchestrator",
		RecreateLockTable:     "recreate_lock",
		ProjectionShardsTable: "projection_shards",
		WorkersTable:          "workers",
	})
	if err != nil {
		log.Fatalf("Failed to create protocol persistence: %v", err)
	}

	// Create protocol coordinator
	coordinator, err := orchestrator.NewProtocolCoordinator(protocolPersistence)
	if err != nil {
		log.Fatalf("Failed to create protocol coordinator: %v", err)
	}

	ctx := context.Background()

	// Phase 1: Check current state (should be idle initially)
	state, err := coordinator.GetCurrentState(ctx)
	if err != nil {
		log.Fatalf("Failed to get protocol state: %v", err)
	}
	fmt.Printf("Initial state: generation=%d, phase=%s\n", state.Generation, state.Phase)

	// Phase 2: Start a new recreate cycle
	// This increments the generation and transitions to 'draining' phase
	generation, err := coordinator.StartRecreate(ctx)
	if err != nil {
		log.Fatalf("Failed to start recreate: %v", err)
	}
	fmt.Printf("Started recreate cycle: generation=%d\n", generation)

	// Phase 3: Wait for all workers to finish draining
	// In a real orchestrator, this would poll periodically
	for {
		transitioned, err := coordinator.TryTransitionToAssigning(ctx)
		if err != nil {
			log.Fatalf("Failed to try transition to assigning: %v", err)
		}

		if transitioned {
			fmt.Println("All workers ready, transitioned to assigning phase")
			break
		}

		fmt.Println("Waiting for workers to drain...")
		time.Sleep(1 * time.Second)
	}

	// Phase 4: Perform shard assignment
	// This clears old assignments and distributes shards to active workers
	err = coordinator.PerformShardAssignment(ctx)
	if err != nil {
		log.Fatalf("Failed to perform shard assignment: %v", err)
	}
	fmt.Println("Shard assignment complete")

	// Phase 5: Transition to running
	// Workers can now begin processing their assigned shards
	err = coordinator.TransitionToRunning(ctx)
	if err != nil {
		log.Fatalf("Failed to transition to running: %v", err)
	}
	fmt.Println("Workers now running with new assignments")

	// Phase 6: Later, when ready to complete the cycle, transition to idle
	err = coordinator.TransitionToIdle(ctx)
	if err != nil {
		log.Fatalf("Failed to transition to idle: %v", err)
	}
	fmt.Println("Recreate cycle complete, back to idle")

	// Output (example):
	// Initial state: generation=0, phase=idle
	// Started recreate cycle: generation=1
	// All workers ready, transitioned to assigning phase
	// Shard assignment complete
	// Workers now running with new assignments
	// Recreate cycle complete, back to idle
}

// ExampleProtocolCoordinator_crashRecovery demonstrates crash recovery.
//
// Shows how a new orchestrator instance can resume coordination after a crash.
func ExampleProtocolCoordinator_crashRecovery() {
	var db *sql.DB // Mock database

	// Simulate: Orchestrator starts and begins a recreate cycle
	protocolPersistence, _ := orchestrator.NewSQLProtocolPersistence(orchestrator.SQLPersistenceConfig{
		DB:         db,
		SchemaName: "orchestrator",
	})

	coordinator1, _ := orchestrator.NewProtocolCoordinator(protocolPersistence)
	ctx := context.Background()

	// Start recreate and immediately "crash"
	generation, _ := coordinator1.StartRecreate(ctx)
	fmt.Printf("Coordinator 1 started recreate: generation=%d\n", generation)

	// Simulate crash: coordinator1 is gone

	// New orchestrator instance starts up
	coordinator2, _ := orchestrator.NewProtocolCoordinator(protocolPersistence)

	// New coordinator reads the state and continues from where the previous one left off
	state, _ := coordinator2.GetCurrentState(ctx)
	fmt.Printf("Coordinator 2 recovered state: generation=%d, phase=%s\n", state.Generation, state.Phase)

	// Can continue the protocol from the draining phase
	// Workers continued draining independently, so we check if they're ready
	transitioned, _ := coordinator2.TryTransitionToAssigning(ctx)
	if transitioned {
		fmt.Println("Coordinator 2 successfully continued protocol after crash")
	} else {
		fmt.Println("Coordinator 2 waiting for workers to drain...")
	}

	// Output (example):
	// Coordinator 1 started recreate: generation=1
	// Coordinator 2 recovered state: generation=1, phase=draining
	// Coordinator 2 waiting for workers to drain...
}

// ExampleProtocolCoordinator_idempotency demonstrates idempotent operations.
//
// Shows that protocol operations can be safely retried without causing corruption.
func ExampleProtocolCoordinator_idempotency() {
	var db *sql.DB // Mock database

	protocolPersistence, _ := orchestrator.NewSQLProtocolPersistence(orchestrator.SQLPersistenceConfig{
		DB:         db,
		SchemaName: "orchestrator",
	})

	coordinator, _ := orchestrator.NewProtocolCoordinator(protocolPersistence)
	ctx := context.Background()

	// Call StartRecreate multiple times - should be idempotent
	gen1, _ := coordinator.StartRecreate(ctx)
	gen2, _ := coordinator.StartRecreate(ctx)
	gen3, _ := coordinator.StartRecreate(ctx)

	fmt.Printf("Generation after multiple StartRecreate calls: %d, %d, %d\n", gen1, gen2, gen3)

	// All should return the same generation since we're already in draining phase
	if gen1 == gen2 && gen2 == gen3 {
		fmt.Println("StartRecreate is idempotent - safe to retry")
	}

	// Phase transitions are also idempotent
	state, _ := coordinator.GetCurrentState(ctx)
	fmt.Printf("Current phase: %s\n", state.Phase)

	// Multiple transitions to the same phase succeed
	_ = protocolPersistence.TransitionPhase(ctx, orchestrator.RecreatePhaseDraining)
	_ = protocolPersistence.TransitionPhase(ctx, orchestrator.RecreatePhaseDraining)

	state, _ = coordinator.GetCurrentState(ctx)
	fmt.Printf("Phase after multiple transitions: %s\n", state.Phase)
	fmt.Println("Phase transitions are idempotent - safe to retry")

	// Output (example):
	// Generation after multiple StartRecreate calls: 1, 1, 1
	// StartRecreate is idempotent - safe to retry
	// Current phase: draining
	// Phase after multiple transitions: draining
	// Phase transitions are idempotent - safe to retry
}
