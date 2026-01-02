package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockWorkerPersistence is a mock implementation of WorkerPersistenceAdapter
type mockWorkerPersistence struct {
	mu                        sync.Mutex
	workers                   map[string]*workerRecord
	currentGeneration         int64
	registerErr               error
	updateHeartbeatErr        error
	updateStateErr            error
	getCurrentGenerationErr   error
	deleteStaleWorkersErr     error
	heartbeatCallCount        int
	deleteStaleWorkersCallCount int
}

type workerRecord struct {
	id             string
	generation     int64
	state          WorkerState
	lastHeartbeat  time.Time
}

func newMockWorkerPersistence() *mockWorkerPersistence {
	return &mockWorkerPersistence{
		workers:           make(map[string]*workerRecord),
		currentGeneration: 1,
	}
}

func (m *mockWorkerPersistence) RegisterWorker(ctx context.Context, workerID string, generation int64, state WorkerState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registerErr != nil {
		return m.registerErr
	}

	m.workers[workerID] = &workerRecord{
		id:            workerID,
		generation:    generation,
		state:         state,
		lastHeartbeat: time.Now(),
	}
	return nil
}

func (m *mockWorkerPersistence) UpdateWorkerHeartbeat(ctx context.Context, workerID string, state WorkerState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.heartbeatCallCount++

	if m.updateHeartbeatErr != nil {
		return m.updateHeartbeatErr
	}

	if record, exists := m.workers[workerID]; exists {
		record.lastHeartbeat = time.Now()
		record.state = state
	}
	return nil
}

func (m *mockWorkerPersistence) UpdateWorkerState(ctx context.Context, workerID string, state WorkerState, generation int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.updateStateErr != nil {
		return m.updateStateErr
	}

	if record, exists := m.workers[workerID]; exists {
		record.state = state
		record.generation = generation
	}
	return nil
}

func (m *mockWorkerPersistence) GetCurrentGeneration(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.getCurrentGenerationErr != nil {
		return 0, m.getCurrentGenerationErr
	}

	return m.currentGeneration, nil
}

func (m *mockWorkerPersistence) DeleteStaleWorkers(ctx context.Context, staleThreshold time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.deleteStaleWorkersCallCount++

	if m.deleteStaleWorkersErr != nil {
		return m.deleteStaleWorkersErr
	}

	now := time.Now()
	for id, record := range m.workers {
		if now.Sub(record.lastHeartbeat) > staleThreshold {
			delete(m.workers, id)
		}
	}
	return nil
}

func TestNewWorker_Success(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		HeartbeatInterval:  1 * time.Second,
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if worker == nil {
		t.Fatal("Expected worker to be created")
	}

	if worker.ID() == "" {
		t.Error("Expected worker to have non-empty ID")
	}

	if worker.State() != WorkerStateStarting {
		t.Errorf("Expected initial state to be starting, got: %v", worker.State())
	}
}

func TestNewWorker_RequiresPersistenceAdapter(t *testing.T) {
	config := WorkerConfig{
		HeartbeatInterval: 1 * time.Second,
	}

	_, err := NewWorker(config)
	if err == nil {
		t.Fatal("Expected error when persistence adapter is nil")
	}

	if !strings.Contains(err.Error(), "persistence adapter is required") {
		t.Errorf("Expected error message about persistence adapter, got: %v", err)
	}
}

func TestNewWorker_DefaultHeartbeatInterval(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if worker.heartbeatInterval != 5*time.Second {
		t.Errorf("Expected default heartbeat interval to be 5s, got: %v", worker.heartbeatInterval)
	}
}

func TestWorker_Start_RegistersWorker(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		HeartbeatInterval:  100 * time.Millisecond,
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	defer worker.Stop(ctx)

	persistence.mu.Lock()
	record, exists := persistence.workers[worker.ID()]
	persistence.mu.Unlock()

	if !exists {
		t.Fatal("Expected worker to be registered")
	}

	if record.state != WorkerStateStarting {
		t.Errorf("Expected registered state to be starting, got: %v", record.state)
	}

	if record.generation != persistence.currentGeneration {
		t.Errorf("Expected generation to be %d, got: %d", persistence.currentGeneration, record.generation)
	}
}

func TestWorker_Start_FailsOnRegisterError(t *testing.T) {
	persistence := newMockWorkerPersistence()
	persistence.registerErr = errors.New("register failed")

	config := WorkerConfig{
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err == nil {
		t.Fatal("Expected error when register fails")
	}

	if !strings.Contains(err.Error(), "failed to register worker") {
		t.Errorf("Expected error about registration failure, got: %v", err)
	}
}

func TestWorker_TransitionTo_UpdatesState(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	defer worker.Stop(ctx)

	// Transition to ready
	err = worker.TransitionTo(ctx, WorkerStateReady)
	if err != nil {
		t.Fatalf("Failed to transition to ready: %v", err)
	}

	if worker.State() != WorkerStateReady {
		t.Errorf("Expected state to be ready, got: %v", worker.State())
	}

	persistence.mu.Lock()
	record := persistence.workers[worker.ID()]
	persistence.mu.Unlock()

	if record.state != WorkerStateReady {
		t.Errorf("Expected persisted state to be ready, got: %v", record.state)
	}
}

func TestWorker_TransitionTo_RollbackOnError(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	defer worker.Stop(ctx)

	initialState := worker.State()

	// Set error for next state update
	persistence.updateStateErr = errors.New("update failed")

	err = worker.TransitionTo(ctx, WorkerStateReady)
	if err == nil {
		t.Fatal("Expected error when state update fails")
	}

	// State should be rolled back
	if worker.State() != initialState {
		t.Errorf("Expected state to be rolled back to %v, got: %v", initialState, worker.State())
	}
}

func TestWorker_HeartbeatLoop_UpdatesPeriodically(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		HeartbeatInterval:  50 * time.Millisecond,
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}

	// Wait for several heartbeats
	time.Sleep(200 * time.Millisecond)

	worker.Stop(ctx)

	persistence.mu.Lock()
	callCount := persistence.heartbeatCallCount
	persistence.mu.Unlock()

	// Should have been called at least 3 times (200ms / 50ms = 4, minus some margin)
	if callCount < 2 {
		t.Errorf("Expected at least 2 heartbeat calls, got: %d", callCount)
	}
}

func TestWorker_ObserveGeneration_UpdatesGeneration(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}
	defer worker.Stop(ctx)

	initialGen := worker.GenerationSeen()

	// Update generation in persistence
	persistence.mu.Lock()
	persistence.currentGeneration = 5
	persistence.mu.Unlock()

	err = worker.ObserveGeneration(ctx)
	if err != nil {
		t.Fatalf("Failed to observe generation: %v", err)
	}

	newGen := worker.GenerationSeen()
	if newGen == initialGen {
		t.Errorf("Expected generation to be updated, still at: %d", newGen)
	}

	if newGen != 5 {
		t.Errorf("Expected generation to be 5, got: %d", newGen)
	}
}

func TestWorker_Stop_StopsHeartbeat(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		HeartbeatInterval:  50 * time.Millisecond,
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}

	// Wait for a few heartbeats
	time.Sleep(150 * time.Millisecond)

	persistence.mu.Lock()
	countBeforeStop := persistence.heartbeatCallCount
	persistence.mu.Unlock()

	err = worker.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop worker: %v", err)
	}

	// Wait to ensure no more heartbeats happen
	time.Sleep(150 * time.Millisecond)

	persistence.mu.Lock()
	countAfterStop := persistence.heartbeatCallCount
	persistence.mu.Unlock()

	// Count should not have increased significantly after stop
	if countAfterStop > countBeforeStop+1 {
		t.Errorf("Expected heartbeat to stop, but count increased from %d to %d", countBeforeStop, countAfterStop)
	}

	if worker.State() != WorkerStateStopped {
		t.Errorf("Expected final state to be stopped, got: %v", worker.State())
	}
}

func TestWorker_StateTransitions(t *testing.T) {
	persistence := newMockWorkerPersistence()
	config := WorkerConfig{
		PersistenceAdapter: persistence,
	}

	worker, err := NewWorker(config)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	ctx := context.Background()
	err = worker.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}

	// Test valid state transitions
	transitions := []WorkerState{
		WorkerStateReady,
		WorkerStateRunning,
		WorkerStateDraining,
		WorkerStateStopped,
	}

	for _, state := range transitions {
		err := worker.TransitionTo(ctx, state)
		if err != nil {
			t.Errorf("Failed to transition to %v: %v", state, err)
		}

		if worker.State() != state {
			t.Errorf("Expected state %v, got: %v", state, worker.State())
		}
	}
}
