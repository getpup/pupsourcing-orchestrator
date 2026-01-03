package coordinator

import (
	"context"
	"errors"
	"testing"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssignPartitions_SingleWorkerGetsPartitionZero(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	err := assigner.AssignPartitions(ctx, generationID)

	require.NoError(t, err)
	assert.Len(t, mockStore.AssignPartitionCalls, 1)
	assert.Equal(t, "worker-1", mockStore.AssignPartitionCalls[0].WorkerID)
	assert.Equal(t, 0, mockStore.AssignPartitionCalls[0].PartitionKey)
}

func TestAssignPartitions_MultipleWorkersGetSequentialPartitions(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
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

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	err := assigner.AssignPartitions(ctx, generationID)

	require.NoError(t, err)
	assert.Len(t, mockStore.AssignPartitionCalls, 3)
	assert.Equal(t, "worker-1", mockStore.AssignPartitionCalls[0].WorkerID)
	assert.Equal(t, 0, mockStore.AssignPartitionCalls[0].PartitionKey)
	assert.Equal(t, "worker-2", mockStore.AssignPartitionCalls[1].WorkerID)
	assert.Equal(t, 1, mockStore.AssignPartitionCalls[1].PartitionKey)
	assert.Equal(t, "worker-3", mockStore.AssignPartitionCalls[2].WorkerID)
	assert.Equal(t, 2, mockStore.AssignPartitionCalls[2].PartitionKey)
}

func TestAssignPartitions_WorkerOrderIsDeterministic(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "ccc", GenerationID: gid},
			{ID: "aaa", GenerationID: gid},
			{ID: "bbb", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	err := assigner.AssignPartitions(ctx, generationID)

	require.NoError(t, err)
	assert.Len(t, mockStore.AssignPartitionCalls, 3)
	assert.Equal(t, "aaa", mockStore.AssignPartitionCalls[0].WorkerID)
	assert.Equal(t, 0, mockStore.AssignPartitionCalls[0].PartitionKey)
	assert.Equal(t, "bbb", mockStore.AssignPartitionCalls[1].WorkerID)
	assert.Equal(t, 1, mockStore.AssignPartitionCalls[1].PartitionKey)
	assert.Equal(t, "ccc", mockStore.AssignPartitionCalls[2].WorkerID)
	assert.Equal(t, 2, mockStore.AssignPartitionCalls[2].PartitionKey)
}

func TestAssignPartitions_SameWorkersAlwaysGetSamePartitions(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-z", GenerationID: gid},
			{ID: "worker-a", GenerationID: gid},
			{ID: "worker-m", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	// First call
	err := assigner.AssignPartitions(ctx, generationID)
	require.NoError(t, err)
	firstCallAssignments := make([]store.AssignPartitionCall, len(mockStore.AssignPartitionCalls))
	copy(firstCallAssignments, mockStore.AssignPartitionCalls)

	// Reset and call again
	mockStore.Reset()
	err = assigner.AssignPartitions(ctx, generationID)
	require.NoError(t, err)
	secondCallAssignments := mockStore.AssignPartitionCalls

	// Verify same assignments both times
	assert.Equal(t, len(firstCallAssignments), len(secondCallAssignments))
	for i := range firstCallAssignments {
		assert.Equal(t, firstCallAssignments[i].WorkerID, secondCallAssignments[i].WorkerID)
		assert.Equal(t, firstCallAssignments[i].PartitionKey, secondCallAssignments[i].PartitionKey)
	}

	// Verify expected assignments
	assert.Equal(t, "worker-a", firstCallAssignments[0].WorkerID)
	assert.Equal(t, 0, firstCallAssignments[0].PartitionKey)
	assert.Equal(t, "worker-m", firstCallAssignments[1].WorkerID)
	assert.Equal(t, 1, firstCallAssignments[1].PartitionKey)
	assert.Equal(t, "worker-z", firstCallAssignments[2].WorkerID)
	assert.Equal(t, 2, firstCallAssignments[2].PartitionKey)
}

func TestAssignPartitions_EmptyWorkerList(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	err := assigner.AssignPartitions(ctx, generationID)

	require.NoError(t, err)
	assert.Len(t, mockStore.AssignPartitionCalls, 0)
}

func TestAssignPartitions_ErrorPropagation(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	generationID := "gen-1"
	expectedErr := errors.New("assignment failed")

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "worker-1", GenerationID: gid},
			{ID: "worker-2", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		if workerID == "worker-2" {
			return expectedErr
		}
		return nil
	}

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	err := assigner.AssignPartitions(ctx, generationID)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to assign partition")
	assert.Contains(t, err.Error(), "worker-2")
	assert.ErrorIs(t, err, expectedErr)
}

func TestAssignPartitions_WithUUIDStyleIDs(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	generationID := "gen-1"

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, gid string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{ID: "f47ac10b-58cc-4372-a567-0e02b2c3d479", GenerationID: gid},
			{ID: "550e8400-e29b-41d4-a716-446655440000", GenerationID: gid},
			{ID: "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d", GenerationID: gid},
			{ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", GenerationID: gid},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, workerID string, partitionKey int) error {
		return nil
	}

	assigner := NewAssigner(mockStore)
	ctx := context.Background()

	err := assigner.AssignPartitions(ctx, generationID)

	require.NoError(t, err)
	assert.Len(t, mockStore.AssignPartitionCalls, 4)

	// Verify deterministic ordering by UUID string comparison
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", mockStore.AssignPartitionCalls[0].WorkerID)
	assert.Equal(t, 0, mockStore.AssignPartitionCalls[0].PartitionKey)
	assert.Equal(t, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", mockStore.AssignPartitionCalls[1].WorkerID)
	assert.Equal(t, 1, mockStore.AssignPartitionCalls[1].PartitionKey)
	assert.Equal(t, "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d", mockStore.AssignPartitionCalls[2].WorkerID)
	assert.Equal(t, 2, mockStore.AssignPartitionCalls[2].PartitionKey)
	assert.Equal(t, "f47ac10b-58cc-4372-a567-0e02b2c3d479", mockStore.AssignPartitionCalls[3].WorkerID)
	assert.Equal(t, 3, mockStore.AssignPartitionCalls[3].PartitionKey)
}
