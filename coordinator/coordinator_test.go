package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoinOrCreate_FirstWorkerCreatesGeneration(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-1",
			ReplicaSet:      rs,
			TotalPartitions: totalPartitions,
			CreatedAt:       time.Now(),
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	gen, err := coordinator.JoinOrCreate(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-1", gen.ID)
	assert.Equal(t, 1, gen.TotalPartitions)
	assert.Len(t, mockStore.GetActiveGenerationCalls, 1)
	assert.Len(t, mockStore.CreateGenerationCalls, 1)
	assert.Equal(t, replicaSet, mockStore.CreateGenerationCalls[0].ReplicaSet)
	assert.Equal(t, 1, mockStore.CreateGenerationCalls[0].TotalPartitions)
}

func TestJoinOrCreate_SecondWorkerTriggersNewGeneration(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	existingGen := orchestrator.Generation{
		ID:              "gen-1",
		ReplicaSet:      replicaSet,
		TotalPartitions: 1,
		CreatedAt:       time.Now(),
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return existingGen, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:           "worker-2",
				ReplicaSet:   rs,
				GenerationID: "gen-1",
				State:        orchestrator.WorkerStatePending,
				PartitionKey: -1,
			},
		}, nil
	}

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				ReplicaSet:    rs,
				GenerationID:  "gen-1",
				State:         orchestrator.WorkerStateRunning,
				PartitionKey:  0,
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-2",
			ReplicaSet:      rs,
			TotalPartitions: totalPartitions,
			CreatedAt:       time.Now(),
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	gen, err := coordinator.JoinOrCreate(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-2", gen.ID)
	assert.Equal(t, 2, gen.TotalPartitions)
	assert.Len(t, mockStore.CreateGenerationCalls, 1)
	assert.Equal(t, 2, mockStore.CreateGenerationCalls[0].TotalPartitions)
}

func TestWaitForPartitionAssignment_ReturnsWhenAssigned(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	workerID := "worker-1"

	callCount := 0
	mockStore.GetWorkerFunc = func(ctx context.Context, wid string) (orchestrator.Worker, error) {
		callCount++
		if callCount == 1 {
			return orchestrator.Worker{
				ID:           wid,
				GenerationID: "gen-1",
				PartitionKey: -1,
				State:        orchestrator.WorkerStatePending,
			}, nil
		}
		return orchestrator.Worker{
			ID:           wid,
			GenerationID: "gen-1",
			PartitionKey: 0,
			State:        orchestrator.WorkerStateReady,
		}, nil
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-1",
			ReplicaSet:      rs,
			TotalPartitions: 2,
			CreatedAt:       time.Now(),
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	assignment, err := coordinator.WaitForPartitionAssignment(ctx, workerID)

	require.NoError(t, err)
	assert.Equal(t, 0, assignment.PartitionKey)
	assert.Equal(t, 2, assignment.TotalPartitions)
	assert.Equal(t, "gen-1", assignment.GenerationID)
	assert.GreaterOrEqual(t, callCount, 2)
}

func TestWaitForPartitionAssignment_TimesOut(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	workerID := "worker-1"

	mockStore.GetWorkerFunc = func(ctx context.Context, wid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           wid,
			GenerationID: "gen-1",
			PartitionKey: -1,
			State:        orchestrator.WorkerStatePending,
		}, nil
	}

	coordinator := New(Config{
		Store:               mockStore,
		PollInterval:        10 * time.Millisecond,
		CoordinationTimeout: 50 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	_, err := coordinator.WaitForPartitionAssignment(ctx, workerID)

	require.Error(t, err)
	assert.Equal(t, orchestrator.ErrCoordinationTimeout, err)
}

func TestWaitForAllReady_ReturnsWhenAllWorkersReady(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	callCount := 0
	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		callCount++
		if callCount == 1 {
			return []orchestrator.Worker{
				{ID: "worker-1", State: orchestrator.WorkerStatePending},
				{ID: "worker-2", State: orchestrator.WorkerStatePending},
			}, nil
		}
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStateReady},
			{ID: "worker-2", State: orchestrator.WorkerStateReady},
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForAllReady(ctx, generationID)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2)
}

func TestWaitForAllReady_TimesOut(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending},
			{ID: "worker-2", State: orchestrator.WorkerStatePending},
		}, nil
	}

	coordinator := New(Config{
		Store:               mockStore,
		PollInterval:        10 * time.Millisecond,
		CoordinationTimeout: 50 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForAllReady(ctx, generationID)

	require.Error(t, err)
	assert.Equal(t, orchestrator.ErrCoordinationTimeout, err)
}

func TestWatchGeneration_ReturnsErrGenerationSuperseded(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	callCount := 0
	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		callCount++
		if callCount <= 2 {
			return orchestrator.Generation{
				ID:         generationID,
				ReplicaSet: rs,
			}, nil
		}
		return orchestrator.Generation{
			ID:         "gen-2",
			ReplicaSet: rs,
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WatchGeneration(ctx, generationID)

	require.Error(t, err)
	assert.Equal(t, orchestrator.ErrGenerationSuperseded, err)
	assert.GreaterOrEqual(t, callCount, 3)
}

func TestWatchGeneration_ReturnsNilOnContextCancellation(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:         generationID,
			ReplicaSet: rs,
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- coordinator.WatchGeneration(ctx, generationID)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("WatchGeneration did not return promptly after context cancellation")
	}
}

func TestCleanupStaleWorkers_MarksOldWorkersDead(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	staleTime := time.Now().Add(-2 * time.Minute)
	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				LastHeartbeat: staleTime,
			},
			{
				ID:            "worker-2",
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	mockStore.MarkWorkerDeadFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	coordinator := New(Config{
		Store:              mockStore,
		StaleWorkerTimeout: 1 * time.Minute,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.CleanupStaleWorkers(ctx)

	require.NoError(t, err)
	assert.Len(t, mockStore.MarkWorkerDeadCalls, 1)
	assert.Equal(t, "worker-1", mockStore.MarkWorkerDeadCalls[0].WorkerID)
}

func TestCleanupStaleWorkers_IgnoresFreshWorkers(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				LastHeartbeat: time.Now().Add(-10 * time.Second),
			},
			{
				ID:            "worker-2",
				LastHeartbeat: time.Now().Add(-5 * time.Second),
			},
		}, nil
	}

	mockStore.MarkWorkerDeadFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	coordinator := New(Config{
		Store:              mockStore,
		StaleWorkerTimeout: 1 * time.Minute,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.CleanupStaleWorkers(ctx)

	require.NoError(t, err)
	assert.Len(t, mockStore.MarkWorkerDeadCalls, 0)
}

func TestCoordinationTimeout_IsRespected(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending},
		}, nil
	}

	coordinator := New(Config{
		Store:               mockStore,
		PollInterval:        5 * time.Millisecond,
		CoordinationTimeout: 20 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	start := time.Now()
	err := coordinator.WaitForAllReady(ctx, "gen-1")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Equal(t, orchestrator.ErrCoordinationTimeout, err)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	assert.Less(t, elapsed, 100*time.Millisecond, "timeout should be relatively precise")
}

func TestNew_AppliesDefaults(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	coordinator := New(Config{Store: mockStore}, replicaSet)

	assert.Equal(t, 30*time.Second, coordinator.config.StaleWorkerTimeout)
	assert.Equal(t, 1*time.Second, coordinator.config.PollInterval)
	assert.Equal(t, 60*time.Second, coordinator.config.CoordinationTimeout)
}

func TestWaitForPartitionAssignment_ContextCancellation(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	workerID := "worker-1"

	mockStore.GetWorkerFunc = func(ctx context.Context, wid string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:           wid,
			GenerationID: "gen-1",
			PartitionKey: -1,
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := coordinator.WaitForPartitionAssignment(ctx, workerID)

	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestWaitForAllReady_ContextCancellation(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending},
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := coordinator.WaitForAllReady(ctx, "gen-1")

	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestJoinOrCreate_ReturnsExistingGenerationWhenNoChanges(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	existingGen := orchestrator.Generation{
		ID:              "gen-1",
		ReplicaSet:      replicaSet,
		TotalPartitions: 2,
		CreatedAt:       time.Now(),
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return existingGen, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				LastHeartbeat: time.Now(),
			},
			{
				ID:            "worker-2",
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	gen, err := coordinator.JoinOrCreate(ctx)

	require.NoError(t, err)
	assert.Equal(t, existingGen.ID, gen.ID)
	assert.Equal(t, existingGen.TotalPartitions, gen.TotalPartitions)
	assert.Len(t, mockStore.CreateGenerationCalls, 0, "should not create new generation")
}
