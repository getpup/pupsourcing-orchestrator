package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateGeneration_NewReplicaSet(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")
	totalPartitions := 3

	beforeCreate := time.Now()
	gen, err := s.CreateGeneration(ctx, replicaSet, totalPartitions)
	afterCreate := time.Now()

	require.NoError(t, err)
	assert.NotEmpty(t, gen.ID, "Generation ID should not be empty")
	assert.Equal(t, replicaSet, gen.ReplicaSet)
	assert.Equal(t, totalPartitions, gen.TotalPartitions)
	assert.True(t, gen.CreatedAt.After(beforeCreate) || gen.CreatedAt.Equal(beforeCreate))
	assert.True(t, gen.CreatedAt.Before(afterCreate) || gen.CreatedAt.Equal(afterCreate))
}

func TestCreateGeneration_UpdatesActiveGeneration(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen1, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	gen2, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	activeGen, err := s.GetActiveGeneration(ctx, replicaSet)
	require.NoError(t, err)
	assert.Equal(t, gen2.ID, activeGen.ID)
	assert.NotEqual(t, gen1.ID, activeGen.ID)
}

func TestRegisterWorker_PendingState(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	beforeRegister := time.Now()
	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	afterRegister := time.Now()

	require.NoError(t, err)
	assert.NotEmpty(t, worker.ID, "Worker ID should not be empty")
	assert.Equal(t, gen.ID, worker.GenerationID)
	assert.Equal(t, replicaSet, worker.ReplicaSet)
	assert.Equal(t, orchestrator.WorkerStatePending, worker.State)
	assert.Equal(t, -1, worker.PartitionKey)
	assert.True(t, worker.StartedAt.After(beforeRegister) || worker.StartedAt.Equal(beforeRegister))
	assert.True(t, worker.StartedAt.Before(afterRegister) || worker.StartedAt.Equal(afterRegister))
	assert.True(t, worker.LastHeartbeat.After(beforeRegister) || worker.LastHeartbeat.Equal(beforeRegister))
	assert.True(t, worker.LastHeartbeat.Before(afterRegister) || worker.LastHeartbeat.Equal(afterRegister))
}

func TestAssignPartition(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	err = s.AssignPartition(ctx, worker.ID, 0)
	require.NoError(t, err)

	updatedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, updatedWorker.PartitionKey)
}

func TestUpdateWorkerState_Transitions(t *testing.T) {
	t.Run("Pending to Ready", func(t *testing.T) {
		s := New()
		ctx := context.Background()
		replicaSet := orchestrator.ReplicaSetName("main-projections")

		gen, err := s.CreateGeneration(ctx, replicaSet, 1)
		require.NoError(t, err)

		worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateReady)
		require.NoError(t, err)

		updatedWorker, err := s.GetWorker(ctx, worker.ID)
		require.NoError(t, err)
		assert.Equal(t, orchestrator.WorkerStateReady, updatedWorker.State)
	})

	t.Run("Ready to Running", func(t *testing.T) {
		s := New()
		ctx := context.Background()
		replicaSet := orchestrator.ReplicaSetName("main-projections")

		gen, err := s.CreateGeneration(ctx, replicaSet, 1)
		require.NoError(t, err)

		worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateReady)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateRunning)
		require.NoError(t, err)

		updatedWorker, err := s.GetWorker(ctx, worker.ID)
		require.NoError(t, err)
		assert.Equal(t, orchestrator.WorkerStateRunning, updatedWorker.State)
	})

	t.Run("Running to Stopping", func(t *testing.T) {
		s := New()
		ctx := context.Background()
		replicaSet := orchestrator.ReplicaSetName("main-projections")

		gen, err := s.CreateGeneration(ctx, replicaSet, 1)
		require.NoError(t, err)

		worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateReady)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateRunning)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateStopping)
		require.NoError(t, err)

		updatedWorker, err := s.GetWorker(ctx, worker.ID)
		require.NoError(t, err)
		assert.Equal(t, orchestrator.WorkerStateStopping, updatedWorker.State)
	})

	t.Run("Stopping to Stopped", func(t *testing.T) {
		s := New()
		ctx := context.Background()
		replicaSet := orchestrator.ReplicaSetName("main-projections")

		gen, err := s.CreateGeneration(ctx, replicaSet, 1)
		require.NoError(t, err)

		worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateReady)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateRunning)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateStopping)
		require.NoError(t, err)

		err = s.UpdateWorkerState(ctx, worker.ID, orchestrator.WorkerStateStopped)
		require.NoError(t, err)

		updatedWorker, err := s.GetWorker(ctx, worker.ID)
		require.NoError(t, err)
		assert.Equal(t, orchestrator.WorkerStateStopped, updatedWorker.State)
	})
}

func TestHeartbeat_UpdatesTimestamp(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	initialHeartbeat := worker.LastHeartbeat

	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)

	err = s.Heartbeat(ctx, worker.ID)
	require.NoError(t, err)

	updatedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)

	assert.True(t, updatedWorker.LastHeartbeat.After(initialHeartbeat),
		"LastHeartbeat should be updated to a later time")
}

func TestGetWorkersByGeneration(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen1, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	gen2, err := s.CreateGeneration(ctx, replicaSet, 2)
	require.NoError(t, err)

	worker1a, err := s.RegisterWorker(ctx, replicaSet, gen1.ID)
	require.NoError(t, err)

	worker1b, err := s.RegisterWorker(ctx, replicaSet, gen1.ID)
	require.NoError(t, err)

	worker2, err := s.RegisterWorker(ctx, replicaSet, gen2.ID)
	require.NoError(t, err)

	// Get workers for gen1
	gen1Workers, err := s.GetWorkersByGeneration(ctx, gen1.ID)
	require.NoError(t, err)
	assert.Len(t, gen1Workers, 2)

	gen1WorkerIDs := make(map[string]bool)
	for _, w := range gen1Workers {
		gen1WorkerIDs[w.ID] = true
	}
	assert.True(t, gen1WorkerIDs[worker1a.ID])
	assert.True(t, gen1WorkerIDs[worker1b.ID])
	assert.False(t, gen1WorkerIDs[worker2.ID])

	// Get workers for gen2
	gen2Workers, err := s.GetWorkersByGeneration(ctx, gen2.ID)
	require.NoError(t, err)
	assert.Len(t, gen2Workers, 1)
	assert.Equal(t, worker2.ID, gen2Workers[0].ID)
}

func TestGetActiveWorkers_ExcludesStopped(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 3)
	require.NoError(t, err)

	worker1, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker2, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker3, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	// Update worker2 to Running
	err = s.UpdateWorkerState(ctx, worker2.ID, orchestrator.WorkerStateRunning)
	require.NoError(t, err)

	// Mark worker3 as Stopped
	err = s.MarkWorkerDead(ctx, worker3.ID)
	require.NoError(t, err)

	// Get active workers
	activeWorkers, err := s.GetActiveWorkers(ctx, replicaSet)
	require.NoError(t, err)
	assert.Len(t, activeWorkers, 2)

	activeWorkerIDs := make(map[string]bool)
	for _, w := range activeWorkers {
		activeWorkerIDs[w.ID] = true
	}
	assert.True(t, activeWorkerIDs[worker1.ID])
	assert.True(t, activeWorkerIDs[worker2.ID])
	assert.False(t, activeWorkerIDs[worker3.ID], "Stopped worker should not be included")
}

func TestGetPendingWorkers(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 3)
	require.NoError(t, err)

	worker1, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker2, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	worker3, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	// Update worker2 to Ready
	err = s.UpdateWorkerState(ctx, worker2.ID, orchestrator.WorkerStateReady)
	require.NoError(t, err)

	// Update worker3 to Running
	err = s.UpdateWorkerState(ctx, worker3.ID, orchestrator.WorkerStateRunning)
	require.NoError(t, err)

	// Get pending workers
	pendingWorkers, err := s.GetPendingWorkers(ctx, replicaSet)
	require.NoError(t, err)
	assert.Len(t, pendingWorkers, 1)
	assert.Equal(t, worker1.ID, pendingWorkers[0].ID)
	assert.Equal(t, orchestrator.WorkerStatePending, pendingWorkers[0].State)
}

func TestMarkWorkerDead(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 1)
	require.NoError(t, err)

	worker, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
	require.NoError(t, err)

	// Mark worker as dead
	err = s.MarkWorkerDead(ctx, worker.ID)
	require.NoError(t, err)

	// Verify state is stopped
	updatedWorker, err := s.GetWorker(ctx, worker.ID)
	require.NoError(t, err)
	assert.Equal(t, orchestrator.WorkerStateStopped, updatedWorker.State)
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	gen, err := s.CreateGeneration(ctx, replicaSet, 10)
	require.NoError(t, err)

	var wg sync.WaitGroup
	numWorkers := 10

	// Register workers concurrently
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.RegisterWorker(ctx, replicaSet, gen.ID)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Verify all workers were registered
	workers, err := s.GetWorkersByGeneration(ctx, gen.ID)
	require.NoError(t, err)
	assert.Len(t, workers, numWorkers)

	// Perform concurrent operations on workers
	for _, worker := range workers {
		wg.Add(3)
		workerID := worker.ID

		// Concurrent heartbeat
		go func() {
			defer wg.Done()
			err := s.Heartbeat(ctx, workerID)
			assert.NoError(t, err)
		}()

		// Concurrent state update
		go func() {
			defer wg.Done()
			err := s.UpdateWorkerState(ctx, workerID, orchestrator.WorkerStateReady)
			assert.NoError(t, err)
		}()

		// Concurrent partition assignment
		go func() {
			defer wg.Done()
			err := s.AssignPartition(ctx, workerID, 0)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Verify store is in a consistent state
	activeWorkers, err := s.GetActiveWorkers(ctx, replicaSet)
	require.NoError(t, err)
	assert.Len(t, activeWorkers, numWorkers)
}

func TestErrorCases(t *testing.T) {
	t.Run("GetWorker with non-existent ID returns ErrWorkerNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()

		_, err := s.GetWorker(ctx, "non-existent-id")
		assert.ErrorIs(t, err, store.ErrWorkerNotFound)
	})

	t.Run("AssignPartition with non-existent worker returns ErrWorkerNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()

		err := s.AssignPartition(ctx, "non-existent-id", 0)
		assert.ErrorIs(t, err, store.ErrWorkerNotFound)
	})

	t.Run("UpdateWorkerState with non-existent worker returns ErrWorkerNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()

		err := s.UpdateWorkerState(ctx, "non-existent-id", orchestrator.WorkerStateReady)
		assert.ErrorIs(t, err, store.ErrWorkerNotFound)
	})

	t.Run("Heartbeat with non-existent worker returns ErrWorkerNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()

		err := s.Heartbeat(ctx, "non-existent-id")
		assert.ErrorIs(t, err, store.ErrWorkerNotFound)
	})

	t.Run("MarkWorkerDead with non-existent worker returns ErrWorkerNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()

		err := s.MarkWorkerDead(ctx, "non-existent-id")
		assert.ErrorIs(t, err, store.ErrWorkerNotFound)
	})

	t.Run("GetActiveGeneration with no generations returns ErrReplicaSetNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()
		replicaSet := orchestrator.ReplicaSetName("non-existent-replica-set")

		_, err := s.GetActiveGeneration(ctx, replicaSet)
		assert.ErrorIs(t, err, orchestrator.ErrReplicaSetNotFound)
	})

	t.Run("RegisterWorker with non-existent generation returns ErrGenerationNotFound", func(t *testing.T) {
		s := New()
		ctx := context.Background()
		replicaSet := orchestrator.ReplicaSetName("main-projections")

		_, err := s.RegisterWorker(ctx, replicaSet, "non-existent-generation-id")
		assert.ErrorIs(t, err, store.ErrGenerationNotFound)
	})
}

func TestRegisterWorker_InvalidGenerationID(t *testing.T) {
	s := New()
	ctx := context.Background()
	replicaSet := orchestrator.ReplicaSetName("main-projections")

	_, err := s.RegisterWorker(ctx, replicaSet, "non-existent-generation")

	require.Error(t, err)
	assert.ErrorIs(t, err, store.ErrGenerationNotFound)
}
