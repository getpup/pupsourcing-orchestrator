package lifecycle

import (
	"context"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegister_CreatesWorkerInPendingState(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-123"
	workerID := "worker-456"

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, gid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           workerID,
			ReplicaSet:   rs,
			GenerationID: gid,
			State:        orchestrator.WorkerStatePending,
			PartitionKey: -1,
		}, nil
	}

	manager := New(Config{Store: mockStore})
	ctx := context.Background()

	returnedID, err := manager.Register(ctx, replicaSet, generationID)

	require.NoError(t, err)
	assert.Equal(t, workerID, returnedID)
	assert.Equal(t, workerID, manager.WorkerID())
	assert.Len(t, mockStore.RegisterWorkerCalls, 1)
	assert.Equal(t, replicaSet, mockStore.RegisterWorkerCalls[0].ReplicaSet)
	assert.Equal(t, generationID, mockStore.RegisterWorkerCalls[0].GenerationID)
}

func TestHeartbeat_CalledAtConfiguredInterval(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	workerID := "worker-123"

	heartbeatCount := 0
	mockStore.HeartbeatFunc = func(ctx context.Context, wid string) error {
		heartbeatCount++
		return nil
	}

	manager := New(Config{
		Store:             mockStore,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	manager.workerID = workerID

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := manager.StartHeartbeat(ctx)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, heartbeatCount, 2, "expected at least 2 heartbeats in 150ms with 50ms interval")
	assert.LessOrEqual(t, heartbeatCount, 4, "expected at most 4 heartbeats in 150ms with 50ms interval")
}

func TestContextCancellation_StopsHeartbeat(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	workerID := "worker-123"

	mockStore.HeartbeatFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	manager := New(Config{
		Store:             mockStore,
		HeartbeatInterval: 1 * time.Second,
	})
	manager.workerID = workerID

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- manager.StartHeartbeat(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("StartHeartbeat did not return promptly after context cancellation")
	}
}

func TestStateUpdates_ArePersisted(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	workerID := "worker-789"
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-123"

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, gid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           workerID,
			ReplicaSet:   rs,
			GenerationID: gid,
			State:        orchestrator.WorkerStatePending,
		}, nil
	}

	mockStore.UpdateWorkerStateFunc = func(ctx context.Context, wid string, state orchestrator.WorkerState) error {
		return nil
	}

	manager := New(Config{Store: mockStore})
	ctx := context.Background()

	_, err := manager.Register(ctx, replicaSet, generationID)
	require.NoError(t, err)

	err = manager.UpdateState(ctx, orchestrator.WorkerStateReady)
	require.NoError(t, err)

	assert.Len(t, mockStore.UpdateWorkerStateCalls, 1)
	assert.Equal(t, workerID, mockStore.UpdateWorkerStateCalls[0].WorkerID)
	assert.Equal(t, orchestrator.WorkerStateReady, mockStore.UpdateWorkerStateCalls[0].State)
}

func TestNilLogger_DoesntPanic(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	workerID := "worker-123"
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-123"

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, gid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           workerID,
			ReplicaSet:   rs,
			GenerationID: gid,
			State:        orchestrator.WorkerStatePending,
		}, nil
	}

	mockStore.UpdateWorkerStateFunc = func(ctx context.Context, wid string, state orchestrator.WorkerState) error {
		return nil
	}

	mockStore.HeartbeatFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	manager := New(Config{
		Store:             mockStore,
		Logger:            nil,
		HeartbeatInterval: 50 * time.Millisecond,
	})
	ctx := context.Background()

	_, err := manager.Register(ctx, replicaSet, generationID)
	require.NoError(t, err)

	err = manager.UpdateState(ctx, orchestrator.WorkerStateReady)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = manager.StartHeartbeat(ctx)
	require.NoError(t, err)
}

func TestGetWorker_ReturnsCurrentWorker(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	workerID := "worker-123"
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-123"

	expectedWorker := orchestrator.Worker{
		ID:           workerID,
		ReplicaSet:   replicaSet,
		GenerationID: generationID,
		State:        orchestrator.WorkerStateReady,
		PartitionKey: 0,
	}

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, gid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           workerID,
			ReplicaSet:   rs,
			GenerationID: gid,
			State:        orchestrator.WorkerStatePending,
		}, nil
	}

	mockStore.GetWorkerFunc = func(ctx context.Context, wid string) (orchestrator.Worker, error) {
		return expectedWorker, nil
	}

	manager := New(Config{Store: mockStore})
	ctx := context.Background()

	_, err := manager.Register(ctx, replicaSet, generationID)
	require.NoError(t, err)

	worker, err := manager.GetWorker(ctx)
	require.NoError(t, err)

	assert.Equal(t, expectedWorker.ID, worker.ID)
	assert.Equal(t, expectedWorker.State, worker.State)
	assert.Equal(t, expectedWorker.PartitionKey, worker.PartitionKey)
	assert.Len(t, mockStore.GetWorkerCalls, 1)
	assert.Equal(t, workerID, mockStore.GetWorkerCalls[0].WorkerID)
}

func TestWorkerID_ReturnsCorrectID(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	workerID := "worker-abc-123"
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-456"

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, gid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           workerID,
			ReplicaSet:   rs,
			GenerationID: gid,
			State:        orchestrator.WorkerStatePending,
		}, nil
	}

	manager := New(Config{Store: mockStore})
	ctx := context.Background()

	returnedID, err := manager.Register(ctx, replicaSet, generationID)
	require.NoError(t, err)

	assert.Equal(t, workerID, returnedID)
	assert.Equal(t, workerID, manager.WorkerID())
}
