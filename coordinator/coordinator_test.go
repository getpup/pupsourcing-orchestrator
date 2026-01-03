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

func TestGetOrCreateGeneration_FirstWorkerCreatesGeneration(t *testing.T) {
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

	gen, err := coordinator.GetOrCreateGeneration(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-1", gen.ID)
	assert.Equal(t, 1, gen.TotalPartitions)
	assert.Len(t, mockStore.GetActiveGenerationCalls, 1)
	assert.Len(t, mockStore.CreateGenerationCalls, 1)
	assert.Equal(t, replicaSet, mockStore.CreateGenerationCalls[0].ReplicaSet)
	assert.Equal(t, 1, mockStore.CreateGenerationCalls[0].TotalPartitions)
}

func TestGetOrCreateGeneration_SecondWorkerReturnsExistingGeneration(t *testing.T) {
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

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	gen, err := coordinator.GetOrCreateGeneration(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-1", gen.ID)
	assert.Equal(t, 1, gen.TotalPartitions)
	assert.Len(t, mockStore.GetActiveGenerationCalls, 1)
	assert.Len(t, mockStore.CreateGenerationCalls, 0, "should not create new generation")
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

func TestCountExpectedWorkers_CountsActiveNonStaleWorkers(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	staleTime := time.Now().Add(-2 * time.Minute)

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				GenerationID:  gid,
				State:         orchestrator.WorkerStateRunning,
				LastHeartbeat: time.Now(),
			},
			{
				ID:            "worker-2",
				GenerationID:  gid,
				State:         orchestrator.WorkerStatePending,
				LastHeartbeat: time.Now(),
			},
			{
				ID:            "worker-3",
				GenerationID:  gid,
				State:         orchestrator.WorkerStateStopped,
				LastHeartbeat: time.Now(),
			},
			{
				ID:            "worker-4",
				GenerationID:  gid,
				State:         orchestrator.WorkerStateRunning,
				LastHeartbeat: staleTime,
			},
		}, nil
	}

	coordinator := New(Config{
		Store:              mockStore,
		StaleWorkerTimeout: 1 * time.Minute,
	}, replicaSet)
	ctx := context.Background()

	count, err := coordinator.CountExpectedWorkers(ctx, generationID)

	require.NoError(t, err)
	assert.Equal(t, 2, count, "should count only non-stopped, non-stale workers")
}

func TestShouldTriggerReconfiguration_WithPendingWorkers(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

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

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	shouldTrigger, err := coordinator.ShouldTriggerReconfiguration(ctx)

	require.NoError(t, err)
	assert.True(t, shouldTrigger)
}

func TestShouldTriggerReconfiguration_WithStaleWorkers(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	staleTime := time.Now().Add(-2 * time.Minute)

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

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

	coordinator := New(Config{
		Store:              mockStore,
		StaleWorkerTimeout: 1 * time.Minute,
	}, replicaSet)
	ctx := context.Background()

	shouldTrigger, err := coordinator.ShouldTriggerReconfiguration(ctx)

	require.NoError(t, err)
	assert.True(t, shouldTrigger)
}

func TestShouldTriggerReconfiguration_NoReconfigurationNeeded(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

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

	shouldTrigger, err := coordinator.ShouldTriggerReconfiguration(ctx)

	require.NoError(t, err)
	assert.False(t, shouldTrigger)
}

func TestTriggerReconfiguration_CreatesGenerationWithCorrectPartitions(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 2,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				GenerationID:  generationID,
				LastHeartbeat: time.Now(),
				State:         orchestrator.WorkerStateRunning,
			},
			{
				ID:            "worker-2",
				GenerationID:  generationID,
				LastHeartbeat: time.Now(),
				State:         orchestrator.WorkerStatePending,
			},
			{
				ID:            "worker-3",
				GenerationID:  generationID,
				LastHeartbeat: time.Now(),
				State:         orchestrator.WorkerStatePending,
			},
		}, nil
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
			{
				ID:            "worker-3",
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	mockStore.MarkWorkerDeadFunc = func(ctx context.Context, wid string) error {
		return nil
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

	gen, err := coordinator.TriggerReconfiguration(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-2", gen.ID)
	assert.Equal(t, 3, gen.TotalPartitions)
	assert.Len(t, mockStore.CreateGenerationCalls, 1)
	assert.Equal(t, 3, mockStore.CreateGenerationCalls[0].TotalPartitions)
}

func TestTriggerReconfiguration_CleansUpStaleWorkersFirst(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	staleTime := time.Now().Add(-2 * time.Minute)
	markedDead := []string{}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		// After cleanup, only worker-2 remains
		if len(markedDead) > 0 {
			return []orchestrator.Worker{
				{
					ID:            "worker-2",
					GenerationID:  generationID,
					LastHeartbeat: time.Now(),
					State:         orchestrator.WorkerStateRunning,
				},
			}, nil
		}
		// Before cleanup, both workers exist
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				GenerationID:  generationID,
				LastHeartbeat: staleTime,
				State:         orchestrator.WorkerStateRunning,
			},
			{
				ID:            "worker-2",
				GenerationID:  generationID,
				LastHeartbeat: time.Now(),
				State:         orchestrator.WorkerStateRunning,
			},
		}, nil
	}

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
		markedDead = append(markedDead, wid)
		return nil
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-2",
			ReplicaSet:      rs,
			TotalPartitions: totalPartitions,
			CreatedAt:       time.Now(),
		}, nil
	}

	coordinator := New(Config{
		Store:              mockStore,
		StaleWorkerTimeout: 1 * time.Minute,
	}, replicaSet)
	ctx := context.Background()

	gen, err := coordinator.TriggerReconfiguration(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-2", gen.ID)
	assert.Contains(t, markedDead, "worker-1", "stale worker should be marked dead")
}

func TestTriggerReconfiguration_WithNoActiveWorkersCreatesOnePartition(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.MarkWorkerDeadFunc = func(ctx context.Context, wid string) error {
		return nil
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

	gen, err := coordinator.TriggerReconfiguration(ctx)

	require.NoError(t, err)
	assert.Equal(t, "gen-2", gen.ID)
	assert.Equal(t, 1, gen.TotalPartitions)
	assert.Len(t, mockStore.CreateGenerationCalls, 1)
	assert.Equal(t, 1, mockStore.CreateGenerationCalls[0].TotalPartitions)
}

func TestIsLeader_WorkerWithSmallestIDIsLeader(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-3", GenerationID: gid},
			{ID: "worker-1", GenerationID: gid},
			{ID: "worker-2", GenerationID: gid},
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	isLeader, err := coordinator.IsLeader(ctx, generationID, "worker-1")

	require.NoError(t, err)
	assert.True(t, isLeader)
}

func TestIsLeader_OtherWorkersAreNotLeaders(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", GenerationID: gid},
			{ID: "worker-2", GenerationID: gid},
			{ID: "worker-3", GenerationID: gid},
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	isLeader2, err := coordinator.IsLeader(ctx, generationID, "worker-2")
	require.NoError(t, err)
	assert.False(t, isLeader2)

	isLeader3, err := coordinator.IsLeader(ctx, generationID, "worker-3")
	require.NoError(t, err)
	assert.False(t, isLeader3)
}

func TestIsLeader_SingleWorkerIsLeader(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", GenerationID: gid},
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	isLeader, err := coordinator.IsLeader(ctx, generationID, "worker-1")

	require.NoError(t, err)
	assert.True(t, isLeader)
}

func TestIsLeader_EmptyWorkerListReturnsFalse(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	isLeader, err := coordinator.IsLeader(ctx, generationID, "worker-1")

	require.NoError(t, err)
	assert.False(t, isLeader)
}

func TestIsLeader_WithUUIDStyleIDs(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "f47ac10b-58cc-4372-a567-0e02b2c3d479", GenerationID: gid},
			{ID: "550e8400-e29b-41d4-a716-446655440000", GenerationID: gid},
			{ID: "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d", GenerationID: gid},
			{ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", GenerationID: gid},
		}, nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	// The lexicographically smallest UUID should be the leader
	isLeader1, err := coordinator.IsLeader(ctx, generationID, "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	assert.True(t, isLeader1)

	// Others should not be leaders
	isLeader2, err := coordinator.IsLeader(ctx, generationID, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	assert.False(t, isLeader2)

	isLeader3, err := coordinator.IsLeader(ctx, generationID, "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d")
	require.NoError(t, err)
	assert.False(t, isLeader3)

	isLeader4, err := coordinator.IsLeader(ctx, generationID, "f47ac10b-58cc-4372-a567-0e02b2c3d479")
	require.NoError(t, err)
	assert.False(t, isLeader4)
}

func TestAssignPartitionsIfLeader_LeaderAssignsPartitions(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", GenerationID: gid},
			{ID: "worker-2", GenerationID: gid},
			{ID: "worker-3", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	assigned, err := coordinator.AssignPartitionsIfLeader(ctx, generationID, "worker-1")

	require.NoError(t, err)
	assert.True(t, assigned)
	assert.Len(t, mockStore.AssignPartitionCalls, 3, "leader should assign partitions to all workers")
}

func TestAssignPartitionsIfLeader_NonLeaderSkipsAssignment(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", GenerationID: gid},
			{ID: "worker-2", GenerationID: gid},
			{ID: "worker-3", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	coordinator := New(Config{Store: mockStore}, replicaSet)
	ctx := context.Background()

	assigned, err := coordinator.AssignPartitionsIfLeader(ctx, generationID, "worker-2")

	require.NoError(t, err)
	assert.False(t, assigned)
	assert.Len(t, mockStore.AssignPartitionCalls, 0, "non-leader should not assign partitions")
}

func TestWaitForExpectedWorkers_ReturnsWhenExpectedWorkersRegistered(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 3,
			CreatedAt:       time.Now(),
		}, nil
	}

	callCount := 0
	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		callCount++
		if callCount == 1 {
			return []orchestrator.Worker{
				{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			}, nil
		}
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			{ID: "worker-2", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			{ID: "worker-3", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForExpectedWorkers(ctx, generationID)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, callCount, 2)
}

func TestWaitForExpectedWorkers_TimesOutGracefully(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 3,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
		}, nil
	}

	coordinator := New(Config{
		Store:               mockStore,
		PollInterval:        10 * time.Millisecond,
		CoordinationTimeout: 50 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForExpectedWorkers(ctx, generationID)

	require.NoError(t, err, "should return nil on timeout for graceful degradation")
}

func TestWaitForExpectedWorkers_SingleWorkerCase(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForExpectedWorkers(ctx, generationID)

	require.NoError(t, err)
}

func TestWaitForExpectedWorkers_ContextCancellation(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 3,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- coordinator.WaitForExpectedWorkers(ctx, generationID)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("WaitForExpectedWorkers did not return promptly after context cancellation")
	}
}

func TestWaitForExpectedWorkers_IgnoresStoppedWorkers(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 2,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			{ID: "worker-2", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			{ID: "worker-3", State: orchestrator.WorkerStateStopped, LastHeartbeat: time.Now()},
		}, nil
	}

	coordinator := New(Config{
		Store:        mockStore,
		PollInterval: 10 * time.Millisecond,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForExpectedWorkers(ctx, generationID)

	require.NoError(t, err)
}

func TestWaitForExpectedWorkers_IgnoresStaleWorkers(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 2,
			CreatedAt:       time.Now(),
		}, nil
	}

	staleTime := time.Now().Add(-2 * time.Minute)
	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			{ID: "worker-2", State: orchestrator.WorkerStatePending, LastHeartbeat: time.Now()},
			{ID: "worker-3", State: orchestrator.WorkerStatePending, LastHeartbeat: staleTime},
		}, nil
	}

	coordinator := New(Config{
		Store:              mockStore,
		PollInterval:       10 * time.Millisecond,
		StaleWorkerTimeout: 1 * time.Minute,
	}, replicaSet)
	ctx := context.Background()

	err := coordinator.WaitForExpectedWorkers(ctx, generationID)

	require.NoError(t, err)
}
