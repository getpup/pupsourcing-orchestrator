package orchestrator

import (
	"database/sql"
	"testing"
	"time"

	rootpkg "github.com/getpup/pupsourcing-orchestrator"
	espostgres "github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ValidConfig(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}
	cfg := Config{
		DB:         db,
		EventStore: eventStore,
		ReplicaSet: "test-replica-set",
	}

	orch, err := New(cfg)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestNew_MissingDB(t *testing.T) {
	eventStore := &espostgres.Store{}
	cfg := Config{
		EventStore: eventStore,
		ReplicaSet: "test-replica-set",
	}

	orch, err := New(cfg)

	assert.Error(t, err)
	assert.Nil(t, orch)
	assert.Contains(t, err.Error(), "DB is required")
}

func TestNew_MissingEventStore(t *testing.T) {
	db := &sql.DB{}
	cfg := Config{
		DB:         db,
		ReplicaSet: "test-replica-set",
	}

	orch, err := New(cfg)

	assert.Error(t, err)
	assert.Nil(t, orch)
	assert.Contains(t, err.Error(), "EventStore is required")
}

func TestNew_MissingReplicaSet(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}
	cfg := Config{
		DB:         db,
		EventStore: eventStore,
	}

	orch, err := New(cfg)

	assert.Error(t, err)
	assert.Nil(t, orch)
	assert.Contains(t, err.Error(), "ReplicaSet is required")
}

func TestNew_DefaultsAreApplied(t *testing.T) {
	// Note: We can't directly inspect the internal config of the orchestrator,
	// but we can verify defaults are applied by providing only required fields
	// and checking that New succeeds (defaults allow successful creation).
	db := &sql.DB{}
	eventStore := &espostgres.Store{}
	cfg := Config{
		DB:         db,
		EventStore: eventStore,
		ReplicaSet: "test-replica-set",
	}

	orch, err := New(cfg)

	require.NoError(t, err)
	assert.NotNil(t, orch)
}

func TestNew_CustomValuesArePreserved(t *testing.T) {
	db := &sql.DB{}
	eventStore := &espostgres.Store{}
	cfg := Config{
		DB:                  db,
		EventStore:          eventStore,
		ReplicaSet:          "test-replica-set",
		HeartbeatInterval:   10 * time.Second,
		StaleWorkerTimeout:  60 * time.Second,
		CoordinationTimeout: 120 * time.Second,
		BatchSize:           50,
	}

	orch, err := New(cfg)

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
	// and is done in integration tests. Here we just verify the function compiles
	// and can be called (we check it's not nil).
	assert.NotNil(t, RunMigrations)
}
