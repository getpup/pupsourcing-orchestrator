package store

import (
	"context"
	"sync"

	"github.com/getpup/pupsourcing-orchestrator"
)

// MockGenerationStore is a configurable mock implementation of GenerationStore
// for use in tests. It allows setting up expected return values, tracking method
// calls, and injecting errors for testing error paths.
type MockGenerationStore struct {
	mu sync.RWMutex

	// GetActiveGenerationFunc is called by GetActiveGeneration if set.
	GetActiveGenerationFunc func(ctx context.Context, replicaSet orchestrator.ReplicaSetName) (orchestrator.Generation, error)

	// CreateGenerationFunc is called by CreateGeneration if set.
	CreateGenerationFunc func(ctx context.Context, replicaSet orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error)

	// RegisterWorkerFunc is called by RegisterWorker if set.
	RegisterWorkerFunc func(ctx context.Context, replicaSet orchestrator.ReplicaSetName, generationID string) (orchestrator.Worker, error)

	// AssignPartitionFunc is called by AssignPartition if set.
	AssignPartitionFunc func(ctx context.Context, workerID string, partitionKey int) error

	// UpdateWorkerStateFunc is called by UpdateWorkerState if set.
	UpdateWorkerStateFunc func(ctx context.Context, workerID string, state orchestrator.WorkerState) error

	// HeartbeatFunc is called by Heartbeat if set.
	HeartbeatFunc func(ctx context.Context, workerID string) error

	// GetWorkerFunc is called by GetWorker if set.
	GetWorkerFunc func(ctx context.Context, workerID string) (orchestrator.Worker, error)

	// GetWorkersByGenerationFunc is called by GetWorkersByGeneration if set.
	GetWorkersByGenerationFunc func(ctx context.Context, generationID string) ([]orchestrator.Worker, error)

	// GetActiveWorkersFunc is called by GetActiveWorkers if set.
	GetActiveWorkersFunc func(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error)

	// GetPendingWorkersFunc is called by GetPendingWorkers if set.
	GetPendingWorkersFunc func(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error)

	// MarkWorkerDeadFunc is called by MarkWorkerDead if set.
	MarkWorkerDeadFunc func(ctx context.Context, workerID string) error

	// Call tracking
	GetActiveGenerationCalls    []GetActiveGenerationCall
	CreateGenerationCalls       []CreateGenerationCall
	RegisterWorkerCalls         []RegisterWorkerCall
	AssignPartitionCalls        []AssignPartitionCall
	UpdateWorkerStateCalls      []UpdateWorkerStateCall
	HeartbeatCalls              []HeartbeatCall
	GetWorkerCalls              []GetWorkerCall
	GetWorkersByGenerationCalls []GetWorkersByGenerationCall
	GetActiveWorkersCalls       []GetActiveWorkersCall
	GetPendingWorkersCalls      []GetPendingWorkersCall
	MarkWorkerDeadCalls         []MarkWorkerDeadCall
}

// Call tracking structs
type GetActiveGenerationCall struct {
	ReplicaSet orchestrator.ReplicaSetName
}

type CreateGenerationCall struct {
	ReplicaSet      orchestrator.ReplicaSetName
	TotalPartitions int
}

type RegisterWorkerCall struct {
	ReplicaSet   orchestrator.ReplicaSetName
	GenerationID string
}

type AssignPartitionCall struct {
	WorkerID     string
	PartitionKey int
}

type UpdateWorkerStateCall struct {
	WorkerID string
	State    orchestrator.WorkerState
}

type HeartbeatCall struct {
	WorkerID string
}

type GetWorkerCall struct {
	WorkerID string
}

type GetWorkersByGenerationCall struct {
	GenerationID string
}

type GetActiveWorkersCall struct {
	ReplicaSet orchestrator.ReplicaSetName
}

type GetPendingWorkersCall struct {
	ReplicaSet orchestrator.ReplicaSetName
}

type MarkWorkerDeadCall struct {
	WorkerID string
}

// NewMockGenerationStore creates a new mock generation store.
func NewMockGenerationStore() *MockGenerationStore {
	return &MockGenerationStore{}
}

// GetActiveGeneration implements GenerationStore.
func (m *MockGenerationStore) GetActiveGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
	m.mu.Lock()
	m.GetActiveGenerationCalls = append(m.GetActiveGenerationCalls, GetActiveGenerationCall{
		ReplicaSet: replicaSet,
	})
	m.mu.Unlock()

	if m.GetActiveGenerationFunc != nil {
		return m.GetActiveGenerationFunc(ctx, replicaSet)
	}

	return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
}

// CreateGeneration implements GenerationStore.
func (m *MockGenerationStore) CreateGeneration(ctx context.Context, replicaSet orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
	m.mu.Lock()
	m.CreateGenerationCalls = append(m.CreateGenerationCalls, CreateGenerationCall{
		ReplicaSet:      replicaSet,
		TotalPartitions: totalPartitions,
	})
	m.mu.Unlock()

	if m.CreateGenerationFunc != nil {
		return m.CreateGenerationFunc(ctx, replicaSet, totalPartitions)
	}

	return orchestrator.Generation{}, nil
}

// RegisterWorker implements GenerationStore.
func (m *MockGenerationStore) RegisterWorker(ctx context.Context, replicaSet orchestrator.ReplicaSetName, generationID string) (orchestrator.Worker, error) {
	m.mu.Lock()
	m.RegisterWorkerCalls = append(m.RegisterWorkerCalls, RegisterWorkerCall{
		ReplicaSet:   replicaSet,
		GenerationID: generationID,
	})
	m.mu.Unlock()

	if m.RegisterWorkerFunc != nil {
		return m.RegisterWorkerFunc(ctx, replicaSet, generationID)
	}

	return orchestrator.Worker{}, nil
}

// AssignPartition implements GenerationStore.
func (m *MockGenerationStore) AssignPartition(ctx context.Context, workerID string, partitionKey int) error {
	m.mu.Lock()
	m.AssignPartitionCalls = append(m.AssignPartitionCalls, AssignPartitionCall{
		WorkerID:     workerID,
		PartitionKey: partitionKey,
	})
	m.mu.Unlock()

	if m.AssignPartitionFunc != nil {
		return m.AssignPartitionFunc(ctx, workerID, partitionKey)
	}

	return nil
}

// UpdateWorkerState implements GenerationStore.
func (m *MockGenerationStore) UpdateWorkerState(ctx context.Context, workerID string, state orchestrator.WorkerState) error {
	m.mu.Lock()
	m.UpdateWorkerStateCalls = append(m.UpdateWorkerStateCalls, UpdateWorkerStateCall{
		WorkerID: workerID,
		State:    state,
	})
	m.mu.Unlock()

	if m.UpdateWorkerStateFunc != nil {
		return m.UpdateWorkerStateFunc(ctx, workerID, state)
	}

	return nil
}

// Heartbeat implements GenerationStore.
func (m *MockGenerationStore) Heartbeat(ctx context.Context, workerID string) error {
	m.mu.Lock()
	m.HeartbeatCalls = append(m.HeartbeatCalls, HeartbeatCall{
		WorkerID: workerID,
	})
	m.mu.Unlock()

	if m.HeartbeatFunc != nil {
		return m.HeartbeatFunc(ctx, workerID)
	}

	return nil
}

// GetWorker implements GenerationStore.
func (m *MockGenerationStore) GetWorker(ctx context.Context, workerID string) (orchestrator.Worker, error) {
	m.mu.Lock()
	m.GetWorkerCalls = append(m.GetWorkerCalls, GetWorkerCall{
		WorkerID: workerID,
	})
	m.mu.Unlock()

	if m.GetWorkerFunc != nil {
		return m.GetWorkerFunc(ctx, workerID)
	}

	return orchestrator.Worker{}, ErrWorkerNotFound
}

// GetWorkersByGeneration implements GenerationStore.
func (m *MockGenerationStore) GetWorkersByGeneration(ctx context.Context, generationID string) ([]orchestrator.Worker, error) {
	m.mu.Lock()
	m.GetWorkersByGenerationCalls = append(m.GetWorkersByGenerationCalls, GetWorkersByGenerationCall{
		GenerationID: generationID,
	})
	m.mu.Unlock()

	if m.GetWorkersByGenerationFunc != nil {
		return m.GetWorkersByGenerationFunc(ctx, generationID)
	}

	return []orchestrator.Worker{}, nil
}

// GetActiveWorkers implements GenerationStore.
func (m *MockGenerationStore) GetActiveWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
	m.mu.Lock()
	m.GetActiveWorkersCalls = append(m.GetActiveWorkersCalls, GetActiveWorkersCall{
		ReplicaSet: replicaSet,
	})
	m.mu.Unlock()

	if m.GetActiveWorkersFunc != nil {
		return m.GetActiveWorkersFunc(ctx, replicaSet)
	}

	return []orchestrator.Worker{}, nil
}

// GetPendingWorkers implements GenerationStore.
func (m *MockGenerationStore) GetPendingWorkers(ctx context.Context, replicaSet orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
	m.mu.Lock()
	m.GetPendingWorkersCalls = append(m.GetPendingWorkersCalls, GetPendingWorkersCall{
		ReplicaSet: replicaSet,
	})
	m.mu.Unlock()

	if m.GetPendingWorkersFunc != nil {
		return m.GetPendingWorkersFunc(ctx, replicaSet)
	}

	return []orchestrator.Worker{}, nil
}

// MarkWorkerDead implements GenerationStore.
func (m *MockGenerationStore) MarkWorkerDead(ctx context.Context, workerID string) error {
	m.mu.Lock()
	m.MarkWorkerDeadCalls = append(m.MarkWorkerDeadCalls, MarkWorkerDeadCall{
		WorkerID: workerID,
	})
	m.mu.Unlock()

	if m.MarkWorkerDeadFunc != nil {
		return m.MarkWorkerDeadFunc(ctx, workerID)
	}

	return nil
}

// Reset clears all call tracking data.
func (m *MockGenerationStore) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.GetActiveGenerationCalls = nil
	m.CreateGenerationCalls = nil
	m.RegisterWorkerCalls = nil
	m.AssignPartitionCalls = nil
	m.UpdateWorkerStateCalls = nil
	m.HeartbeatCalls = nil
	m.GetWorkerCalls = nil
	m.GetWorkersByGenerationCalls = nil
	m.GetActiveWorkersCalls = nil
	m.GetPendingWorkersCalls = nil
	m.MarkWorkerDeadCalls = nil
}
