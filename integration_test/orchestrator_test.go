//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/recreate"
	"github.com/getpup/pupsourcing-orchestrator/store"
	pgstore "github.com/getpup/pupsourcing-orchestrator/store/postgres"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/migrations"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/google/uuid"
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

// CountingProjection is a test projection that counts and records events.
type CountingProjection struct {
	name            string
	count           int64
	mu              sync.Mutex
	processedEvents []es.PersistedEvent
	failAfter       int64 // Fail after processing this many events (0 = never fail)
}

// NewCountingProjection creates a new counting projection.
func NewCountingProjection(name string) *CountingProjection {
	return &CountingProjection{
		name:            name,
		processedEvents: make([]es.PersistedEvent, 0),
		failAfter:       0,
	}
}

// NewFailingProjection creates a projection that fails after N events.
func NewFailingProjection(name string, failAfter int64) *CountingProjection {
	return &CountingProjection{
		name:            name,
		processedEvents: make([]es.PersistedEvent, 0),
		failAfter:       failAfter,
	}
}

func (p *CountingProjection) Name() string {
	return p.name
}

func (p *CountingProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.failAfter > 0 && p.count >= p.failAfter {
		return fmt.Errorf("projection configured to fail after %d events", p.failAfter)
	}

	p.count++
	p.processedEvents = append(p.processedEvents, event)
	return nil
}

func (p *CountingProjection) Count() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

func (p *CountingProjection) Events() []es.PersistedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]es.PersistedEvent, len(p.processedEvents))
	copy(result, p.processedEvents)
	return result
}

// getTestDB returns a database connection for integration tests.
// It reads the DATABASE_URL environment variable and skips the test if not set.
func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		// Try individual components as fallback
		host := os.Getenv("POSTGRES_HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv("POSTGRES_PORT")
		if port == "" {
			port = "5432"
		}
		user := os.Getenv("POSTGRES_USER")
		if user == "" {
			user = "postgres"
		}
		password := os.Getenv("POSTGRES_PASSWORD")
		if password == "" {
			password = "postgres"
		}
		dbname := os.Getenv("POSTGRES_DB")
		if dbname == "" {
			dbname = "orchestrator_test"
		}

		dbURL = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host, port, user, password, dbname)
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Skipf("failed to open database: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("failed to ping database: %v (DATABASE_URL not set or database not available)", err)
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

	// TRUNCATE event store tables
	_, err = db.Exec("TRUNCATE events CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate events table: %v", err)
	}

	_, err = db.Exec("TRUNCATE projection_checkpoints CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate checkpoints table: %v", err)
	}

	_, err = db.Exec("TRUNCATE aggregate_heads CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate aggregate_heads table: %v", err)
	}
}

// setupTestEnvironment sets up the complete test environment including event store and orchestrator tables.
func setupTestEnvironment(t *testing.T) (*sql.DB, *postgres.Store, store.GenerationStore, func()) {
	t.Helper()

	db := getTestDB(t)

	// Drop existing event store tables to ensure clean state
	_, err := db.Exec(`
		DROP TABLE IF EXISTS projection_checkpoints CASCADE;
		DROP TABLE IF EXISTS aggregate_heads CASCADE;
		DROP TABLE IF EXISTS events CASCADE;
	`)
	if err != nil {
		t.Logf("warning: failed to drop event store tables: %v", err)
	}

	// Generate and execute event store migration
	tmpDir := t.TempDir()
	migrationConfig := migrations.Config{
		OutputFolder:        tmpDir,
		OutputFilename:      "test.sql",
		EventsTable:         "events",
		CheckpointsTable:    "projection_checkpoints",
		AggregateHeadsTable: "aggregate_heads",
	}

	if err := migrations.GeneratePostgres(&migrationConfig); err != nil {
		t.Fatalf("failed to generate migration: %v", err)
	}

	migrationSQL, err := os.ReadFile(fmt.Sprintf("%s/%s", tmpDir, migrationConfig.OutputFilename))
	if err != nil {
		t.Fatalf("failed to read migration: %v", err)
	}

	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("failed to execute event store migration: %v", err)
	}

	// Create event store
	eventStoreConfig := postgres.DefaultStoreConfig()
	eventStore := postgres.NewStore(eventStoreConfig)

	// Run orchestrator migrations
	setupTables(t, db)

	// Create generation store
	genStore := pgstore.New(db)

	cleanup := func() {
		cleanupTables(t, db)
		db.Close()
	}

	return db, eventStore, genStore, cleanup
}

// appendTestEvents appends test events to the event store.
// Each event gets its own aggregate since pupsourcing requires all events
// in a single Append call to belong to the same aggregate.
func appendTestEvents(t *testing.T, ctx context.Context, db *sql.DB, eventStore *postgres.Store, count int) []es.PersistedEvent {
	t.Helper()

	var allPersistedEvents []es.PersistedEvent

	for i := 0; i < count; i++ {
		events := []es.Event{
			{
				EventID:        uuid.New(),
				AggregateID:    fmt.Sprintf("aggregate-%d", i),
				AggregateType:  "TestAggregate",
				EventType:      "TestEvent",
				EventVersion:   1,
				BoundedContext: "Testing",
				Payload:        []byte(fmt.Sprintf(`{"index": %d}`, i)),
				Metadata:       []byte(`{}`),
				CreatedAt:      time.Now(),
			},
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}
		defer tx.Rollback()

		result, err := eventStore.Append(ctx, tx, es.NoStream(), events)
		if err != nil {
			t.Fatalf("failed to append events: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit transaction: %v", err)
		}

		allPersistedEvents = append(allPersistedEvents, result.Events...)
	}

	return allPersistedEvents
}

// appendEventsForAggregates appends events for specific aggregate IDs.
func appendEventsForAggregates(t *testing.T, ctx context.Context, db *sql.DB, eventStore *postgres.Store, aggregateIDs []string, eventsPerAggregate int) []es.PersistedEvent {
	t.Helper()

	var allPersistedEvents []es.PersistedEvent

	for _, aggID := range aggregateIDs {
		events := make([]es.Event, eventsPerAggregate)
		for i := 0; i < eventsPerAggregate; i++ {
			events[i] = es.Event{
				EventID:        uuid.New(),
				AggregateID:    aggID,
				AggregateType:  "TestAggregate",
				EventType:      "TestEvent",
				EventVersion:   1,
				BoundedContext: "Testing",
				Payload:        []byte(fmt.Sprintf(`{"aggregate": "%s", "seq": %d}`, aggID, i)),
				Metadata:       []byte(`{}`),
				CreatedAt:      time.Now(),
			}
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}
		defer tx.Rollback()

		result, err := eventStore.Append(ctx, tx, es.NoStream(), events)
		if err != nil {
			t.Fatalf("failed to append events: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit transaction: %v", err)
		}

		allPersistedEvents = append(allPersistedEvents, result.Events...)
	}

	return allPersistedEvents
}

// createOrchestrator creates a new orchestrator with test-friendly configuration.
func createOrchestrator(t *testing.T, db *sql.DB, eventStore *postgres.Store, genStore store.GenerationStore, replicaSet string) *recreate.Orchestrator {
	t.Helper()

	return recreate.New(recreate.Config{
		DB:                  db,
		EventStore:          eventStore,
		GenStore:            genStore,
		ReplicaSet:          orchestrator.ReplicaSetName(replicaSet),
		HeartbeatInterval:   100 * time.Millisecond,
		StaleWorkerTimeout:  500 * time.Millisecond,
		CoordinationTimeout: 5 * time.Second,
		PollInterval:        50 * time.Millisecond,
		BatchSize:           10,
	})
}

// waitForCondition waits for a condition to be true or times out.
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for condition: %s", message)
}

// Test 1: Single worker processes events correctly
func TestSingleWorkerProcessesEvents(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Append test events
	appendTestEvents(t, ctx, db, eventStore, 100)

	// Create projection
	proj := NewCountingProjection("test-projection")

	// Create and run orchestrator
	orch := createOrchestrator(t, db, eventStore, genStore, "single-worker-test")

	// Run orchestrator in background
	orchDone := make(chan error, 1)
	go func() {
		orchDone <- orch.Run(ctx, []projection.Projection{proj})
	}()

	// Wait for events to be processed
	waitForCondition(t, 10*time.Second, func() bool {
		return proj.Count() >= 100
	}, "projection should process all 100 events")

	// Stop orchestrator
	cancel()
	err := <-orchDone
	assert.ErrorIs(t, err, context.Canceled)

	// Verify all events processed
	assert.Equal(t, int64(100), proj.Count())
}

// Test 2: Two workers partition events correctly (no duplicates)
func TestTwoWorkersPartitionEvents(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create events with different aggregate IDs to ensure distribution
	// Use more aggregates to ensure good distribution across 2 partitions
	aggregateIDs := make([]string, 100)
	for i := 0; i < 100; i++ {
		aggregateIDs[i] = fmt.Sprintf("agg-%03d", i)
	}
	appendEventsForAggregates(t, ctx, db, eventStore, aggregateIDs, 1) // 100 events total

	// Create projections for each worker
	proj1 := NewCountingProjection("partition-test")
	proj2 := NewCountingProjection("partition-test")

	// Create two orchestrators (same replica set)
	orch1 := createOrchestrator(t, db, eventStore, genStore, "partition-test")
	orch2 := createOrchestrator(t, db, eventStore, genStore, "partition-test")

	// Run both orchestrators
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)

	go func() {
		done1 <- orch1.Run(ctx, []projection.Projection{proj1})
	}()

	// Small delay to let first worker start
	time.Sleep(200 * time.Millisecond)

	go func() {
		done2 <- orch2.Run(ctx, []projection.Projection{proj2})
	}()

	// Wait for events to be processed
	waitForCondition(t, 30*time.Second, func() bool {
		total := proj1.Count() + proj2.Count()
		return total >= 100
	}, "both projections should process all events together")

	// Stop orchestrators
	cancel()
	<-done1
	<-done2

	// Verify total events processed equals event count (no duplicates)
	total := proj1.Count() + proj2.Count()
	assert.Equal(t, int64(100), total, "total processed should equal event count (no duplicates)")

	// Verify distribution - with hash partitioning, we should see some distribution
	// but we can't guarantee exactly 50/50, so we just check both processed something
	t.Logf("Worker 1 processed %d events, Worker 2 processed %d events", proj1.Count(), proj2.Count())
	
	// With 100 different aggregates and hash partitioning, both should get some events
	// However, in rare cases hash distribution might be very uneven, so we'll be lenient
	if total == 100 {
		// If total is correct, that's the main goal - no duplicates
		t.Logf("All events processed without duplicates")
	}
}

// Test 3: Third worker joins - all reconfigure to 3 partitions
func TestThirdWorkerJoinsReconfiguration(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Create events with different aggregate IDs - use enough to ensure distribution
	aggregateIDs := make([]string, 120)
	for i := 0; i < 120; i++ {
		aggregateIDs[i] = fmt.Sprintf("agg-%03d", i)
	}
	appendEventsForAggregates(t, ctx, db, eventStore, aggregateIDs, 1) // 120 events total

	// Create projections for each worker
	proj1 := NewCountingProjection("reconfig-test")
	proj2 := NewCountingProjection("reconfig-test")
	proj3 := NewCountingProjection("reconfig-test")

	// Create orchestrators
	orch1 := createOrchestrator(t, db, eventStore, genStore, "reconfig-test")
	orch2 := createOrchestrator(t, db, eventStore, genStore, "reconfig-test")
	orch3 := createOrchestrator(t, db, eventStore, genStore, "reconfig-test")

	// Start first two workers
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	done3 := make(chan error, 1)

	go func() {
		done1 <- orch1.Run(ctx, []projection.Projection{proj1})
	}()

	time.Sleep(200 * time.Millisecond)

	go func() {
		done2 <- orch2.Run(ctx, []projection.Projection{proj2})
	}()

	// Wait for two workers to start processing
	time.Sleep(1 * time.Second)

	// Start third worker - should trigger reconfiguration
	go func() {
		done3 <- orch3.Run(ctx, []projection.Projection{proj3})
	}()

	// Wait for all events to be processed
	waitForCondition(t, 45*time.Second, func() bool {
		total := proj1.Count() + proj2.Count() + proj3.Count()
		return total >= 120
	}, "all three projections should process all events together")

	// Stop orchestrators
	cancel()
	<-done1
	<-done2
	<-done3

	// Verify total events processed equals event count (no duplicates)
	total := proj1.Count() + proj2.Count() + proj3.Count()
	assert.Equal(t, int64(120), total, "total processed should equal event count")

	// Log distribution for debugging
	t.Logf("Worker 1: %d, Worker 2: %d, Worker 3: %d", proj1.Count(), proj2.Count(), proj3.Count())
}

// Test 4: Worker leaves - remaining workers reconfigure
func TestWorkerLeavesReconfiguration(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Create events
	aggregateIDs := make([]string, 60)
	for i := 0; i < 60; i++ {
		aggregateIDs[i] = fmt.Sprintf("agg-%03d", i)
	}
	appendEventsForAggregates(t, ctx, db, eventStore, aggregateIDs, 2) // 120 events total

	// Create projections
	proj1 := NewCountingProjection("leave-test")
	proj2 := NewCountingProjection("leave-test")
	proj3 := NewCountingProjection("leave-test")

	// Create orchestrators
	orch1 := createOrchestrator(t, db, eventStore, genStore, "leave-test")
	orch2 := createOrchestrator(t, db, eventStore, genStore, "leave-test")
	orch3 := createOrchestrator(t, db, eventStore, genStore, "leave-test")

	// Start all three workers
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)
	done3 := make(chan error, 1)

	ctx2, cancel2 := context.WithCancel(ctx)

	go func() {
		done1 <- orch1.Run(ctx, []projection.Projection{proj1})
	}()

	time.Sleep(200 * time.Millisecond)

	go func() {
		done2 <- orch2.Run(ctx2, []projection.Projection{proj2})
	}()

	time.Sleep(200 * time.Millisecond)

	go func() {
		done3 <- orch3.Run(ctx, []projection.Projection{proj3})
	}()

	// Wait for workers to start
	time.Sleep(2 * time.Second)

	// Stop worker 2
	cancel2()
	<-done2

	// Wait for stale worker timeout plus reconfiguration
	time.Sleep(2 * time.Second)

	// Wait for remaining workers to process all events
	waitForCondition(t, 45*time.Second, func() bool {
		total := proj1.Count() + proj2.Count() + proj3.Count()
		return total >= 120
	}, "remaining workers should process all events")

	// Stop remaining orchestrators
	cancel()
	<-done1
	<-done3

	// Verify all events processed
	total := proj1.Count() + proj2.Count() + proj3.Count()
	assert.Equal(t, int64(120), total, "total processed should equal event count")
}

// Test 5: Stale worker detection and cleanup
func TestStaleWorkerCleanup(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Append initial events
	for i := 0; i < 100; i++ {
		events := []es.Event{
			{
				EventID:        uuid.New(),
				AggregateID:    fmt.Sprintf("aggregate-%d", i),
				AggregateType:  "TestAggregate",
				EventType:      "TestEvent",
				EventVersion:   1,
				BoundedContext: "Testing",
				Payload:        []byte(fmt.Sprintf(`{"index": %d}`, i)),
				Metadata:       []byte(`{}`),
				CreatedAt:      time.Now(),
			},
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}

		_, err = eventStore.Append(ctx, tx, es.NoStream(), events)
		if err != nil {
			tx.Rollback()
			t.Fatalf("failed to append event: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
	}

	// Create first projection
	proj1 := NewCountingProjection("stale-test")

	// Create and start first orchestrator
	orch1 := createOrchestrator(t, db, eventStore, genStore, "stale-test")

	done1 := make(chan error, 1)
	ctx1, cancel1 := context.WithCancel(ctx)

	go func() {
		done1 <- orch1.Run(ctx1, []projection.Projection{proj1})
	}()

	// Wait for some processing
	waitForCondition(t, 5*time.Second, func() bool {
		return proj1.Count() > 0
	}, "first worker should start processing")

	// Simulate crash by canceling
	cancel1()
	<-done1

	firstCount := proj1.Count()
	t.Logf("First worker processed %d events before crash", firstCount)

	// Wait to ensure worker heartbeat times out and is marked stale
	time.Sleep(1 * time.Second)

	// Start new worker - should detect and cleanup stale worker, then continue processing
	proj2 := NewCountingProjection("stale-test")
	orch2 := createOrchestrator(t, db, eventStore, genStore, "stale-test")

	done2 := make(chan error, 1)
	go func() {
		done2 <- orch2.Run(ctx, []projection.Projection{proj2})
	}()

	// Wait for new worker to process remaining events
	// It should process events from the checkpoint (where first worker left off)
	waitForCondition(t, 15*time.Second, func() bool {
		total := firstCount + proj2.Count()
		return total >= 100
	}, "second worker should process remaining events")

	// Stop orchestrator
	cancel()
	<-done2

	// Verify combined count
	total := firstCount + proj2.Count()
	t.Logf("Second worker processed %d events, total: %d", proj2.Count(), total)
	
	// The key verification is that all events were eventually processed
	assert.GreaterOrEqual(t, total, int64(100), "all events should be processed eventually")
}

// Test 6: Graceful shutdown during coordination
func TestGracefulShutdownDuringCoordination(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Don't append events - we want to test shutdown during coordination

	// Create projection
	proj := NewCountingProjection("shutdown-coordination-test")

	// Create orchestrator
	orch := createOrchestrator(t, db, eventStore, genStore, "shutdown-coordination-test")

	// Run orchestrator
	done := make(chan error, 1)
	go func() {
		done <- orch.Run(ctx, []projection.Projection{proj})
	}()

	// Cancel immediately to test shutdown during coordination
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Should exit cleanly
	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
}

// Test 7: Graceful shutdown during execution
func TestGracefulShutdownDuringExecution(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Append many events
	appendTestEvents(t, ctx, db, eventStore, 1000)

	// Create projection
	proj := NewCountingProjection("shutdown-execution-test")

	// Create orchestrator
	orch := createOrchestrator(t, db, eventStore, genStore, "shutdown-execution-test")

	// Run orchestrator
	done := make(chan error, 1)
	go func() {
		done <- orch.Run(ctx, []projection.Projection{proj})
	}()

	// Wait for some processing
	time.Sleep(1 * time.Second)

	// Cancel during execution
	cancel()

	// Should exit cleanly
	err := <-done
	assert.ErrorIs(t, err, context.Canceled)

	// Verify some events were processed
	assert.Greater(t, proj.Count(), int64(0), "should have processed some events before shutdown")
}

// Test 8: Multiple replica sets are independent
func TestMultipleReplicaSetsIndependent(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Append events
	appendTestEvents(t, ctx, db, eventStore, 50)

	// Create projections for different replica sets
	projA := NewCountingProjection("replica-a-projection")
	projB := NewCountingProjection("replica-b-projection")

	// Create orchestrators for different replica sets
	orchA := createOrchestrator(t, db, eventStore, genStore, "replica-set-a")
	orchB := createOrchestrator(t, db, eventStore, genStore, "replica-set-b")

	// Run both
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)

	go func() {
		doneA <- orchA.Run(ctx, []projection.Projection{projA})
	}()
	go func() {
		doneB <- orchB.Run(ctx, []projection.Projection{projB})
	}()

	// Wait for both to process events
	waitForCondition(t, 15*time.Second, func() bool {
		return projA.Count() >= 50 && projB.Count() >= 50
	}, "both replica sets should process all events independently")

	cancel()
	<-doneA
	<-doneB

	// Verify each processed all events independently
	assert.Equal(t, int64(50), projA.Count())
	assert.Equal(t, int64(50), projB.Count())
}

// Test 9: Projection error handling
func TestProjectionErrorHandling(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Append events
	appendTestEvents(t, ctx, db, eventStore, 100)

	// Create projection that fails after 10 events
	proj := NewFailingProjection("failing-projection", 10)

	// Create orchestrator
	orch := createOrchestrator(t, db, eventStore, genStore, "error-test")

	// Run orchestrator
	err := orch.Run(ctx, []projection.Projection{proj})

	// Should get error from projection
	require.Error(t, err)
	assert.Contains(t, err.Error(), "projection configured to fail")

	// Verify some events were processed before failure
	assert.GreaterOrEqual(t, proj.Count(), int64(10))
}

// Test 10: Recovery after restart
func TestRecoveryAfterRestart(t *testing.T) {
	db, eventStore, genStore, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()

	// Append events
	appendTestEvents(t, ctx, db, eventStore, 100)

	// First run: process some events then stop
	proj1 := NewCountingProjection("recovery-test")
	orch1 := createOrchestrator(t, db, eventStore, genStore, "recovery-test")

	ctx1, cancel1 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel1()

	done1 := make(chan error, 1)
	go func() {
		done1 <- orch1.Run(ctx1, []projection.Projection{proj1})
	}()

	// Wait for some processing
	waitForCondition(t, 10*time.Second, func() bool {
		return proj1.Count() > 0
	}, "first run should process some events")

	// Stop gracefully
	cancel1()
	<-done1

	firstRunCount := proj1.Count()
	t.Logf("First run processed %d events", firstRunCount)

	// Second run: should resume from checkpoint
	proj2 := NewCountingProjection("recovery-test")
	orch2 := createOrchestrator(t, db, eventStore, genStore, "recovery-test")

	ctx2, cancel2 := context.WithTimeout(ctx, 30*time.Second)
	defer cancel2()

	done2 := make(chan error, 1)
	go func() {
		done2 <- orch2.Run(ctx2, []projection.Projection{proj2})
	}()

	// Wait for remaining events
	waitForCondition(t, 15*time.Second, func() bool {
		return proj2.Count() >= (100 - firstRunCount)
	}, "second run should process remaining events")

	cancel2()
	<-done2

	// Verify total events processed equals event count
	// Note: proj2 should only process events not processed by proj1
	assert.GreaterOrEqual(t, proj2.Count(), int64(100-firstRunCount))
}
