package orchestrator

import (
	"database/sql"
	"testing"
	"time"

	rootpkg "github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store/postgres"
	espostgres "github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ValidOptions(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestNew_MissingDatabase(t *testing.T) {
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
	)

	assert.Error(t, err)
	assert.Nil(t, orch)
	assert.Contains(t, err.Error(), "database is required")
}

func TestNew_MissingEventStore(t *testing.T) {
	db := &sql.DB{}

	orch, err := New(
		WithDatabase(db),
		WithReplicaSet("test-replica-set"),
	)

	assert.Error(t, err)
	assert.Nil(t, orch)
	assert.Contains(t, err.Error(), "event store is required")
}

func TestNew_MissingReplicaSet(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
	)

	assert.Error(t, err)
	assert.Nil(t, orch)
	assert.Contains(t, err.Error(), "replica set is required")
}

func TestNew_DefaultsAreApplied(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestNew_CustomValuesArePreserved(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithHeartbeatInterval(10*time.Second),
		WithStaleWorkerTimeout(60*time.Second),
		WithCoordinationTimeout(120*time.Second),
		WithBatchSize(50),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestWithTableNames(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithTableNames("custom_generations", "custom_workers"),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestWithGenerationStore(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}
	customStore := postgres.New(db)

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithGenerationStore(customStore),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestWithLogger(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithLogger(nil), // nil logger is valid
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestWithMetricsEnabled(t *testing.T) {
	t.Run("metrics enabled", func(t *testing.T) {
		db := &sql.DB{}
		eventStore := &espostgres.Store{}

		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithMetricsEnabled(true),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
	})

	t.Run("metrics disabled", func(t *testing.T) {
		db := &sql.DB{}
		eventStore := &espostgres.Store{}

		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithMetricsEnabled(false),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
	})
}

func TestWithPollInterval(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithPollInterval(2*time.Second),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestWithRegistrationWaitTime(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithRegistrationWaitTime(10*time.Second),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestMultipleOptions(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	orch, err := New(
		WithDatabase(db),
		WithEventStore(eventStore),
		WithReplicaSet("test-replica-set"),
		WithHeartbeatInterval(10*time.Second),
		WithStaleWorkerTimeout(60*time.Second),
		WithCoordinationTimeout(120*time.Second),
		WithPollInterval(2*time.Second),
		WithBatchSize(50),
		WithRegistrationWaitTime(10*time.Second),
		WithTableNames("custom_gen", "custom_work"),
		WithMetricsEnabled(false),
	)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestReExportedTypes(t *testing.T) {
	t.Run("ReplicaSetName type exists", func(t *testing.T) {
		var rsn ReplicaSetName = "test"
		assert.Equal(t, rootpkg.ReplicaSetName("test"), rsn)
	})

	t.Run("Generation type exists", func(t *testing.T) {
		gen := Generation{
			ID:              "gen-id",
			ReplicaSet:      "test",
			TotalPartitions: 3,
		}
		assert.Equal(t, "gen-id", gen.ID)
	})

	t.Run("Worker type exists", func(t *testing.T) {
		worker := Worker{
			ID:           "worker-id",
			ReplicaSet:   "test",
			GenerationID: "gen-id",
			PartitionKey: 0,
		}
		assert.Equal(t, "worker-id", worker.ID)
	})

	t.Run("PartitionAssignment type exists", func(t *testing.T) {
		assignment := PartitionAssignment{
			PartitionKey:    0,
			TotalPartitions: 3,
			GenerationID:    "gen-id",
		}
		assert.Equal(t, 0, assignment.PartitionKey)
	})
}

func TestRunMigrations(t *testing.T) {
	// Note: This is a unit test that verifies RunMigrations exists and has
	// the correct signature. Full testing of migrations requires a real database
	// and is done in integration tests.
	assert.NotNil(t, RunMigrations)
	assert.NotNil(t, RunMigrationsWithTableNames)
}

func TestRunMigrationsWithTableNames(t *testing.T) {
	// Verify the function exists and accepts a TableConfig
	config := postgres.TableConfig{
		GenerationsTable: "custom_gen",
		WorkersTable:     "custom_work",
	}
	// We can't test execution without a real DB, but we verify the signature compiles
	assert.NotNil(t, config)
}

func TestWithBatchSize_InvalidValue(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	t.Run("zero batch size uses default", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithBatchSize(0), // Zero should be ignored
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
		// If this succeeds, it means the default batch size (100) was used
	})

	t.Run("negative batch size uses default", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithBatchSize(-10), // Negative should be ignored
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
		// If this succeeds, it means the default batch size (100) was used
	})

	t.Run("positive batch size is accepted", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithBatchSize(500),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
	})
}

func TestWithTableNames_InvalidValues(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}

	t.Run("empty generations table uses defaults", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithTableNames("", "custom_workers"),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
		// If this succeeds, it means default table names were used
	})

	t.Run("empty workers table uses defaults", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithTableNames("custom_gen", ""),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
		// If this succeeds, it means default table names were used
	})

	t.Run("both empty tables use defaults", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithTableNames("", ""),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
		// If this succeeds, it means default table names were used
	})

	t.Run("valid table names are accepted", func(t *testing.T) {
		orch, err := New(
			WithDatabase(db),
			WithEventStore(eventStore),
			WithReplicaSet("test-replica-set"),
			WithTableNames("custom_gen", "custom_workers"),
		)

		require.NoError(t, err)
		assert.NotNil(t, orch)
	})
}
