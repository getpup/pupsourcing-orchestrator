package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/getpup/pupsourcing/es"
)

// mockTestProjection for testing (also implements projection.Projection)
type mockTestProjection struct {
	name string
}

func (m *mockTestProjection) Name() string {
	return m.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (m *mockTestProjection) Handle(_ context.Context, _ es.PersistedEvent) error {
	return nil
}

// mockRecreatePersistence is a mock implementation of RecreatePersistenceAdapter
type mockRecreatePersistence struct {
	mu                   sync.Mutex
	currentGeneration    int64
	advanceErr           error
	getCurrentErr        error
	advanceCallCount     int
	expectedCurrentOnCAS int64 // for simulating CAS failure
}

func newMockRecreatePersistence() *mockRecreatePersistence {
	return &mockRecreatePersistence{
		currentGeneration: 0,
	}
}

func (m *mockRecreatePersistence) GetCurrentGeneration(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.getCurrentErr != nil {
		return 0, m.getCurrentErr
	}

	return m.currentGeneration, nil
}

func (m *mockRecreatePersistence) AdvanceGeneration(ctx context.Context, expectedCurrent int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.advanceCallCount++

	if m.advanceErr != nil {
		return 0, m.advanceErr
	}

	// Simulate CAS behavior - if expected doesn't match current, fail
	if m.expectedCurrentOnCAS > 0 && expectedCurrent != m.expectedCurrentOnCAS {
		return 0, errors.New("CAS failed: generation mismatch")
	}

	if expectedCurrent != m.currentGeneration {
		return 0, errors.New("CAS failed: generation mismatch")
	}

	m.currentGeneration++
	return m.currentGeneration, nil
}

func TestRecreate_Constructor(t *testing.T) {
	strategy := Recreate()
	if strategy == nil {
		t.Fatal("Expected Recreate() to return a non-nil strategy")
	}
}

func TestRecreateStrategy_Run_RegistersProjections(t *testing.T) {
	strategy := &RecreateStrategy{}

	proj1 := &mockTestProjection{name: "projection1"}
	proj2 := &mockTestProjection{name: "projection2"}
	projections := []Projection{proj1, proj2}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine since it blocks
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_Run_GracefulShutdown(t *testing.T) {
	strategy := &RecreateStrategy{}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	// Cancel immediately
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_Run_EmptyProjectionsList(t *testing.T) {
	strategy := &RecreateStrategy{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run with empty projections
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, []Projection{})
	}()

	// Cancel immediately
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_Run_ContextTimeout(t *testing.T) {
	strategy := &RecreateStrategy{}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Should not panic
	err := strategy.Run(ctx, projections)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context error, got: %v", err)
	}
}

func TestRecreateStrategy_WithWorker_RegistersAndManagesLifecycle(t *testing.T) {
	persistence := newMockWorkerPersistence()
	workerConfig := WorkerConfig{
		HeartbeatInterval:  50 * time.Millisecond,
		PersistenceAdapter: persistence,
	}

	strategy := &RecreateStrategy{
		WorkerConfig:         workerConfig,
		StaleWorkerThreshold: 100 * time.Millisecond,
	}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	// Give worker time to start and send heartbeats
	time.Sleep(200 * time.Millisecond)

	// Check worker was registered
	persistence.mu.Lock()
	workerCount := len(persistence.workers)
	var workerState WorkerState
	for _, record := range persistence.workers {
		workerState = record.state
		break
	}
	persistence.mu.Unlock()

	if workerCount != 1 {
		t.Errorf("Expected 1 worker to be registered, got: %d", workerCount)
	}

	if workerState != WorkerStateRunning {
		t.Errorf("Expected worker state to be running, got: %v", workerState)
	}

	// Cancel and wait for completion
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not complete in time")
	}

	// Check final worker state (should be stopped)
	persistence.mu.Lock()
	var finalState WorkerState
	for _, record := range persistence.workers {
		finalState = record.state
		break
	}
	persistence.mu.Unlock()

	if finalState != WorkerStateStopped {
		t.Errorf("Expected final worker state to be stopped, got: %v", finalState)
	}
}

func TestRecreateStrategy_WithWorker_CleansUpStaleWorkers(t *testing.T) {
	persistence := newMockWorkerPersistence()

	// Add some stale workers
	persistence.mu.Lock()
	persistence.workers["stale-worker-1"] = &workerRecord{
		id:            "stale-worker-1",
		lastHeartbeat: time.Now().Add(-5 * time.Minute),
		state:         WorkerStateRunning,
	}
	persistence.workers["stale-worker-2"] = &workerRecord{
		id:            "stale-worker-2",
		lastHeartbeat: time.Now().Add(-10 * time.Minute),
		state:         WorkerStateRunning,
	}
	staleCount := len(persistence.workers)
	persistence.mu.Unlock()

	if staleCount != 2 {
		t.Fatalf("Expected 2 stale workers, got: %d", staleCount)
	}

	workerConfig := WorkerConfig{
		HeartbeatInterval:  100 * time.Millisecond,
		PersistenceAdapter: persistence,
	}

	strategy := &RecreateStrategy{
		WorkerConfig:         workerConfig,
		StaleWorkerThreshold: 30 * time.Second,
	}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	// Give strategy time to clean up stale workers
	time.Sleep(100 * time.Millisecond)

	// Check that stale workers were removed and new worker registered
	persistence.mu.Lock()
	workerCount := len(persistence.workers)
	hasStale1 := false
	hasStale2 := false
	for id := range persistence.workers {
		if id == "stale-worker-1" {
			hasStale1 = true
		}
		if id == "stale-worker-2" {
			hasStale2 = true
		}
	}
	persistence.mu.Unlock()

	if workerCount != 1 {
		t.Errorf("Expected 1 worker (new worker, stale ones removed), got: %d", workerCount)
	}

	if hasStale1 {
		t.Error("Expected stale-worker-1 to be cleaned up")
	}

	if hasStale2 {
		t.Error("Expected stale-worker-2 to be cleaned up")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_WithoutWorker_WorksNormally(t *testing.T) {
	// Strategy without worker config should work as before
	strategy := &RecreateStrategy{}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_WithGeneration_AdvancesGeneration(t *testing.T) {
recreatePersistence := newMockRecreatePersistence()

strategy := &RecreateStrategy{
RecreatePersistence: recreatePersistence,
}

proj := &mockTestProjection{name: "test"}
projections := []Projection{proj}

ctx, cancel := context.WithCancel(context.Background())

done := make(chan error, 1)
go func() {
done <- strategy.Run(ctx, projections)
}()

// Give strategy time to advance generation
time.Sleep(50 * time.Millisecond)

// Check that generation was advanced
recreatePersistence.mu.Lock()
currentGen := recreatePersistence.currentGeneration
advanceCalls := recreatePersistence.advanceCallCount
recreatePersistence.mu.Unlock()

if currentGen != 1 {
t.Errorf("Expected generation to be 1, got: %d", currentGen)
}

if advanceCalls != 1 {
t.Errorf("Expected AdvanceGeneration to be called once, got: %d", advanceCalls)
}

cancel()

select {
case <-done:
case <-time.After(1 * time.Second):
t.Fatal("Run did not complete in time")
}
}

func TestRecreateStrategy_WithGeneration_FailsOnCASConflict(t *testing.T) {
recreatePersistence := newMockRecreatePersistence()

// Simulate race condition: set mock to expect different generation
recreatePersistence.mu.Lock()
recreatePersistence.currentGeneration = 5
recreatePersistence.expectedCurrentOnCAS = 3 // Force CAS to fail
recreatePersistence.mu.Unlock()

strategy := &RecreateStrategy{
RecreatePersistence: recreatePersistence,
}

proj := &mockTestProjection{name: "test"}
projections := []Projection{proj}

ctx := context.Background()

err := strategy.Run(ctx, projections)

// Should fail because CAS will fail (expected 3 but got 5)
if err == nil {
t.Fatal("Expected error from CAS conflict, got nil")
}
}

func TestRecreateStrategy_WithGenerationAndWorker_UpdatesWorkerGeneration(t *testing.T) {
recreatePersistence := newMockRecreatePersistence()
workerPersistence := newMockWorkerPersistence()

workerConfig := WorkerConfig{
HeartbeatInterval:  100 * time.Millisecond,
PersistenceAdapter: workerPersistence,
}

strategy := &RecreateStrategy{
RecreatePersistence:  recreatePersistence,
WorkerConfig:         workerConfig,
StaleWorkerThreshold: 30 * time.Second,
}

proj := &mockTestProjection{name: "test"}
projections := []Projection{proj}

ctx, cancel := context.WithCancel(context.Background())

done := make(chan error, 1)
go func() {
done <- strategy.Run(ctx, projections)
}()

// Give strategy time to start and advance generation
time.Sleep(150 * time.Millisecond)

// Check that generation was advanced
recreatePersistence.mu.Lock()
currentGen := recreatePersistence.currentGeneration
recreatePersistence.mu.Unlock()

if currentGen != 1 {
t.Errorf("Expected generation to be 1, got: %d", currentGen)
}

// Check that worker sees the new generation
workerPersistence.mu.Lock()
var workerGen int64
for _, record := range workerPersistence.workers {
workerGen = record.generation
break
}
workerPersistence.mu.Unlock()

if workerGen != 1 {
t.Errorf("Expected worker generation to be 1, got: %d", workerGen)
}

cancel()

select {
case <-done:
case <-time.After(2 * time.Second):
t.Fatal("Run did not complete in time")
}
}

