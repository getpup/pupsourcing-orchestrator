package recreate

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProjection is a test projection
type mockProjection struct {
	name string
}

func newMockProjection(name string) *mockProjection {
	return &mockProjection{
		name: name,
	}
}

func (m *mockProjection) Name() string {
	return m.name
}

func (m *mockProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	return nil
}

func TestNew_AppliesDefaultValues(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	cfg := Config{
		DB:         &sql.DB{},
		EventStore: &postgres.Store{},
		GenStore:   mockStore,
		ReplicaSet: "test-replica-set",
	}

	orch := New(cfg)

	assert.Equal(t, 5*time.Second, orch.config.HeartbeatInterval)
	assert.Equal(t, 30*time.Second, orch.config.StaleWorkerTimeout)
	assert.Equal(t, 60*time.Second, orch.config.CoordinationTimeout)
	assert.Equal(t, 1*time.Second, orch.config.PollInterval)
	assert.Equal(t, 100, orch.config.BatchSize)
	assert.Equal(t, 5*time.Second, orch.config.RegistrationWaitTime)
}

func TestNew_PreservesNonZeroValues(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	cfg := Config{
		DB:                   &sql.DB{},
		EventStore:           &postgres.Store{},
		GenStore:             mockStore,
		ReplicaSet:           "test-replica-set",
		HeartbeatInterval:    10 * time.Second,
		StaleWorkerTimeout:   60 * time.Second,
		CoordinationTimeout:  120 * time.Second,
		PollInterval:         2 * time.Second,
		BatchSize:            50,
		RegistrationWaitTime: 3 * time.Second,
	}

	orch := New(cfg)

	assert.Equal(t, 10*time.Second, orch.config.HeartbeatInterval)
	assert.Equal(t, 60*time.Second, orch.config.StaleWorkerTimeout)
	assert.Equal(t, 120*time.Second, orch.config.CoordinationTimeout)
	assert.Equal(t, 2*time.Second, orch.config.PollInterval)
	assert.Equal(t, 50, orch.config.BatchSize)
	assert.Equal(t, 3*time.Second, orch.config.RegistrationWaitTime)
}

func TestRun_ContextCancellationShutsDownCleanly(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	cfg := Config{
		DB:         &sql.DB{},
		EventStore: &postgres.Store{},
		GenStore:   mockStore,
		ReplicaSet: replicaSet,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := orch.Run(ctx, []projection.Projection{proj})

	assert.ErrorIs(t, err, context.Canceled)
}

func TestRun_StaleWorkerCleanupOnStart(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		// Return a stale worker
		return []orchestrator.Worker{
			{
				ID:            "stale-worker",
				ReplicaSet:    rs,
				State:         orchestrator.WorkerStateRunning,
				LastHeartbeat: time.Now().Add(-5 * time.Minute),
			},
		}, nil
	}

	mockStore.MarkWorkerDeadFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-1",
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, genID string) (orchestrator.Worker, error) {
		return orchestrator.Worker{}, errors.New("stop here")
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	cfg := Config{
		DB:                 &sql.DB{},
		EventStore:         &postgres.Store{},
		GenStore:           mockStore,
		ReplicaSet:         replicaSet,
		StaleWorkerTimeout: 1 * time.Minute,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx := context.Background()
	_ = orch.Run(ctx, []projection.Projection{proj})

	// Verify stale worker was marked as dead
	assert.Greater(t, len(mockStore.MarkWorkerDeadCalls), 0, "should have cleaned up stale workers")
}

func TestRun_ErrorPropagationFromRegistration(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	expectedErr := errors.New("registration failed")

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-1",
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, genID string) (orchestrator.Worker, error) {
		return orchestrator.Worker{}, expectedErr
	}

	cfg := Config{
		DB:         &sql.DB{},
		EventStore: &postgres.Store{},
		GenStore:   mockStore,
		ReplicaSet: replicaSet,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx := context.Background()
	err := orch.Run(ctx, []projection.Projection{proj})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to register worker")
	assert.ErrorIs(t, err, expectedErr)
}

func TestNew_CreatesAllComponents(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	cfg := Config{
		DB:         &sql.DB{},
		EventStore: &postgres.Store{},
		GenStore:   mockStore,
		ReplicaSet: "test-replica-set",
	}

	orch := New(cfg)

	assert.NotNil(t, orch.lifecycle)
	assert.NotNil(t, orch.coordinator)
	assert.NotNil(t, orch.executor)
	assert.Equal(t, cfg.ReplicaSet, orch.config.ReplicaSet)
}

func TestRun_ErrorPropagationFromCoordination(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	expectedErr := errors.New("coordination failed")

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, expectedErr
	}

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	cfg := Config{
		DB:         &sql.DB{},
		EventStore: &postgres.Store{},
		GenStore:   mockStore,
		ReplicaSet: replicaSet,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx := context.Background()
	err := orch.Run(ctx, []projection.Projection{proj})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to join generation")
	assert.ErrorIs(t, err, expectedErr)
}

func TestConfig_AllFieldsPreserved(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	db := &sql.DB{}
	eventStore := &postgres.Store{}
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")

	cfg := Config{
		DB:                   db,
		EventStore:           eventStore,
		GenStore:             mockStore,
		ReplicaSet:           replicaSet,
		HeartbeatInterval:    3 * time.Second,
		StaleWorkerTimeout:   45 * time.Second,
		CoordinationTimeout:  90 * time.Second,
		PollInterval:         500 * time.Millisecond,
		BatchSize:            200,
		RegistrationWaitTime: 2 * time.Second,
	}

	orch := New(cfg)

	assert.Equal(t, replicaSet, orch.config.ReplicaSet)
	assert.Equal(t, 3*time.Second, orch.config.HeartbeatInterval)
	assert.Equal(t, 45*time.Second, orch.config.StaleWorkerTimeout)
	assert.Equal(t, 90*time.Second, orch.config.CoordinationTimeout)
	assert.Equal(t, 500*time.Millisecond, orch.config.PollInterval)
	assert.Equal(t, 200, orch.config.BatchSize)
	assert.Equal(t, 2*time.Second, orch.config.RegistrationWaitTime)
}

func TestRun_SingleWorkerGetsPartitionAssigned(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"
	workerID := "worker-1"

	assignPartitionCalled := false
	partitionAssigned := false

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, genID string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:            workerID,
			ReplicaSet:    rs,
			GenerationID:  genID,
			State:         orchestrator.WorkerStatePending,
			PartitionKey:  -1,
			LastHeartbeat: time.Now(),
		}, nil
	}

	mockStore.HeartbeatFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, genID string) ([]orchestrator.Worker, error) {
		pk := -1
		if partitionAssigned {
			pk = 0
		}
		return []orchestrator.Worker{
			{
				ID:            workerID,
				ReplicaSet:    replicaSet,
				GenerationID:  genID,
				State:         orchestrator.WorkerStatePending,
				PartitionKey:  pk,
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, wid string, partitionKey int) error {
		assignPartitionCalled = true
		partitionAssigned = true
		return nil
	}

	mockStore.GetWorkerFunc = func(ctx context.Context, wid string) (orchestrator.Worker, error) {
		pk := -1
		if partitionAssigned {
			pk = 0
		}
		return orchestrator.Worker{
			ID:            workerID,
			ReplicaSet:    replicaSet,
			GenerationID:  generationID,
			State:         orchestrator.WorkerStatePending,
			PartitionKey:  pk,
			LastHeartbeat: time.Now(),
		}, nil
	}

	mockStore.UpdateWorkerStateFunc = func(ctx context.Context, wid string, state orchestrator.WorkerState) error {
		return nil
	}

	cfg := Config{
		DB:                   &sql.DB{},
		EventStore:           &postgres.Store{},
		GenStore:             mockStore,
		ReplicaSet:           replicaSet,
		RegistrationWaitTime: 100 * time.Millisecond, // Use short wait for test
		CoordinationTimeout:  2 * time.Second,
		PollInterval:         50 * time.Millisecond,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Run in goroutine since it will block
	done := make(chan error, 1)
	go func() {
		done <- orch.Run(ctx, []projection.Projection{proj})
	}()

	// Wait a bit for the flow to proceed
	time.Sleep(500 * time.Millisecond)

	// Cancel to stop the test
	cancel()
	<-done

	// Verify partition assignment was called
	assert.True(t, assignPartitionCalled, "partition assignment should have been called")
}

func TestRun_LeaderAssignsPartitionsAfterRegistrationWait(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	generationID := "gen-1"
	workerID := "worker-1"

	assignPartitionCalled := false
	var assignmentTime time.Time
	startTime := time.Now()

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              generationID,
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, genID string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:            workerID,
			ReplicaSet:    rs,
			GenerationID:  genID,
			State:         orchestrator.WorkerStatePending,
			PartitionKey:  -1,
			LastHeartbeat: time.Now(),
		}, nil
	}

	mockStore.HeartbeatFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, genID string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            workerID,
				ReplicaSet:    replicaSet,
				GenerationID:  genID,
				State:         orchestrator.WorkerStatePending,
				PartitionKey:  -1,
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, wid string, partitionKey int) error {
		assignPartitionCalled = true
		assignmentTime = time.Now()
		return nil
	}

	cfg := Config{
		DB:                   &sql.DB{},
		EventStore:           &postgres.Store{},
		GenStore:             mockStore,
		ReplicaSet:           replicaSet,
		RegistrationWaitTime: 200 * time.Millisecond,
		CoordinationTimeout:  1 * time.Second,
		PollInterval:         50 * time.Millisecond,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run in goroutine
	done := make(chan error, 1)
	go func() {
		done <- orch.Run(ctx, []projection.Projection{proj})
	}()

	// Wait for assignment to be called
	time.Sleep(500 * time.Millisecond)

	cancel()
	<-done

	assert.True(t, assignPartitionCalled, "assignment should be called")
	// Verify the assignment happened at least after the registration wait time
	// Use a small tolerance for timing variations
	expectedMinTime := 200 * time.Millisecond
	tolerance := 50 * time.Millisecond
	actualTime := assignmentTime.Sub(startTime)
	assert.True(t, actualTime >= expectedMinTime-tolerance,
		"assignment should happen after registration wait time (expected >= %v, got %v)",
		expectedMinTime, actualTime)
}

func TestRun_PartitionAssignmentErrorHandled(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	replicaSet := orchestrator.ReplicaSetName("test-replica-set")
	expectedErr := errors.New("assignment failed")

	mockStore.GetActiveWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetPendingWorkersFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{}, nil
	}

	mockStore.GetActiveGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName) (orchestrator.Generation, error) {
		return orchestrator.Generation{}, orchestrator.ErrReplicaSetNotFound
	}

	mockStore.CreateGenerationFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, totalPartitions int) (orchestrator.Generation, error) {
		return orchestrator.Generation{
			ID:              "gen-1",
			ReplicaSet:      rs,
			TotalPartitions: 1,
			CreatedAt:       time.Now(),
		}, nil
	}

	mockStore.RegisterWorkerFunc = func(ctx context.Context, rs orchestrator.ReplicaSetName, genID string) (orchestrator.Worker, error) {
		return orchestrator.Worker{
			ID:            "worker-1",
			ReplicaSet:    rs,
			GenerationID:  genID,
			State:         orchestrator.WorkerStatePending,
			PartitionKey:  -1,
			LastHeartbeat: time.Now(),
		}, nil
	}

	mockStore.HeartbeatFunc = func(ctx context.Context, wid string) error {
		return nil
	}

	mockStore.GetWorkersByGenerationFunc = func(ctx context.Context, genID string) ([]orchestrator.Worker, error) {
		return []orchestrator.Worker{
			{
				ID:            "worker-1",
				ReplicaSet:    replicaSet,
				GenerationID:  genID,
				State:         orchestrator.WorkerStatePending,
				PartitionKey:  -1,
				LastHeartbeat: time.Now(),
			},
		}, nil
	}

	mockStore.AssignPartitionFunc = func(ctx context.Context, wid string, partitionKey int) error {
		return expectedErr
	}

	cfg := Config{
		DB:                   &sql.DB{},
		EventStore:           &postgres.Store{},
		GenStore:             mockStore,
		ReplicaSet:           replicaSet,
		RegistrationWaitTime: 50 * time.Millisecond,
	}

	orch := New(cfg)
	proj := newMockProjection("test-projection")

	ctx := context.Background()
	err := orch.Run(ctx, []projection.Projection{proj})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to assign partitions")
	assert.ErrorIs(t, err, expectedErr)
}
