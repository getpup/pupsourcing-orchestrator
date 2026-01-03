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
}

func TestNew_PreservesNonZeroValues(t *testing.T) {
	mockStore := store.NewMockGenerationStore()
	cfg := Config{
		DB:                  &sql.DB{},
		EventStore:          &postgres.Store{},
		GenStore:            mockStore,
		ReplicaSet:          "test-replica-set",
		HeartbeatInterval:   10 * time.Second,
		StaleWorkerTimeout:  60 * time.Second,
		CoordinationTimeout: 120 * time.Second,
		PollInterval:        2 * time.Second,
		BatchSize:           50,
	}

	orch := New(cfg)

	assert.Equal(t, 10*time.Second, orch.config.HeartbeatInterval)
	assert.Equal(t, 60*time.Second, orch.config.StaleWorkerTimeout)
	assert.Equal(t, 120*time.Second, orch.config.CoordinationTimeout)
	assert.Equal(t, 2*time.Second, orch.config.PollInterval)
	assert.Equal(t, 50, orch.config.BatchSize)
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
		DB:                  db,
		EventStore:          eventStore,
		GenStore:            mockStore,
		ReplicaSet:          replicaSet,
		HeartbeatInterval:   3 * time.Second,
		StaleWorkerTimeout:  45 * time.Second,
		CoordinationTimeout: 90 * time.Second,
		PollInterval:        500 * time.Millisecond,
		BatchSize:           200,
	}

	orch := New(cfg)

	assert.Equal(t, replicaSet, orch.config.ReplicaSet)
	assert.Equal(t, 3*time.Second, orch.config.HeartbeatInterval)
	assert.Equal(t, 45*time.Second, orch.config.StaleWorkerTimeout)
	assert.Equal(t, 90*time.Second, orch.config.CoordinationTimeout)
	assert.Equal(t, 500*time.Millisecond, orch.config.PollInterval)
	assert.Equal(t, 200, orch.config.BatchSize)
}
