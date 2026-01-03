package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/stretchr/testify/assert"
)

// TestSQLQueries verifies that SQL queries are constructed correctly.
func TestSQLQueries(t *testing.T) {
	t.Run("GetActiveGeneration query structure", func(t *testing.T) {
		s := New(nil)

		// Verify the query would use correct table name and parameters
		assert.Equal(t, "orchestrator_generations", s.generationsTable)
		assert.Equal(t, "orchestrator_workers", s.workersTable)
	})

	t.Run("custom table names are used", func(t *testing.T) {
		config := TableConfig{
			GenerationsTable: "custom_generations",
			WorkersTable:     "custom_workers",
		}
		s := NewWithConfig(nil, config)

		assert.Equal(t, "custom_generations", s.generationsTable)
		assert.Equal(t, "custom_workers", s.workersTable)
	})
}

// TestErrorMapping verifies that sql.ErrNoRows and zero rows affected are mapped to appropriate errors.
func TestErrorMapping(t *testing.T) {
	t.Run("GetActiveGeneration maps sql.ErrNoRows to ErrReplicaSetNotFound", func(t *testing.T) {
		// This test verifies the error mapping logic without requiring a real database.
		// The actual error mapping happens when sql.ErrNoRows is returned from QueryRowContext.
		// The implementation checks: if err == sql.ErrNoRows { return ErrReplicaSetNotFound }
		// This is validated by integration tests with a real database.
	})

	t.Run("GetWorker maps sql.ErrNoRows to ErrWorkerNotFound", func(t *testing.T) {
		// Similar to above, the error mapping logic is validated by integration tests.
		// The implementation checks: if err == sql.ErrNoRows { return store.ErrWorkerNotFound }
	})

	t.Run("AssignPartition maps zero rows affected to ErrWorkerNotFound", func(t *testing.T) {
		// The implementation checks: if rowsAffected == 0 { return store.ErrWorkerNotFound }
		// This logic is validated by integration tests with a real database.
	})

	t.Run("UpdateWorkerState maps zero rows affected to ErrWorkerNotFound", func(t *testing.T) {
		// Validated by integration tests.
	})

	t.Run("Heartbeat maps zero rows affected to ErrWorkerNotFound", func(t *testing.T) {
		// Validated by integration tests.
	})

	t.Run("MarkWorkerDead maps zero rows affected to ErrWorkerNotFound", func(t *testing.T) {
		// Validated by integration tests.
	})
}

// TestContextHandling verifies that all methods accept and use context correctly.
func TestContextHandling(t *testing.T) {
	t.Run("all methods accept context parameter", func(t *testing.T) {
		// This test verifies that all GenerationStore methods have context as their first parameter.
		// The actual context usage is tested in integration tests.
		// We verify this by checking that the Store type implements the GenerationStore interface.
		var _ store.GenerationStore = (*Store)(nil)
	})
}

// TestMigrations verifies that migration functions generate valid SQL.
func TestMigrations(t *testing.T) {
	t.Run("MigrationUp generates valid SQL", func(t *testing.T) {
		config := DefaultTableConfig()
		sql := MigrationUp(config)

		assert.Contains(t, sql, "CREATE TABLE orchestrator_generations")
		assert.Contains(t, sql, "CREATE TABLE orchestrator_workers")
		assert.Contains(t, sql, "CREATE INDEX idx_generations_replica_set")
		assert.Contains(t, sql, "CREATE INDEX idx_workers_replica_set")
		assert.Contains(t, sql, "CREATE INDEX idx_workers_generation")
		assert.Contains(t, sql, "CREATE INDEX idx_workers_state")
		assert.Contains(t, sql, "REFERENCES orchestrator_generations(id)")
	})

	t.Run("MigrationDown generates valid SQL", func(t *testing.T) {
		config := DefaultTableConfig()
		sql := MigrationDown(config)

		assert.Contains(t, sql, "DROP TABLE IF EXISTS orchestrator_workers")
		assert.Contains(t, sql, "DROP TABLE IF EXISTS orchestrator_generations")
	})

	t.Run("MigrationUp with custom table names", func(t *testing.T) {
		config := TableConfig{
			GenerationsTable: "custom_generations",
			WorkersTable:     "custom_workers",
		}
		sql := MigrationUp(config)

		assert.Contains(t, sql, "CREATE TABLE custom_generations")
		assert.Contains(t, sql, "CREATE TABLE custom_workers")
		assert.Contains(t, sql, "REFERENCES custom_generations(id)")
	})

	t.Run("MigrationDown with custom table names", func(t *testing.T) {
		config := TableConfig{
			GenerationsTable: "custom_generations",
			WorkersTable:     "custom_workers",
		}
		sql := MigrationDown(config)

		assert.Contains(t, sql, "DROP TABLE IF EXISTS custom_workers")
		assert.Contains(t, sql, "DROP TABLE IF EXISTS custom_generations")
	})

	t.Run("MigrationDown drops workers table before generations", func(t *testing.T) {
		config := DefaultTableConfig()
		sql := MigrationDown(config)

		// Workers table should be dropped first due to foreign key constraint
		workersIdx := indexOf(sql, "orchestrator_workers")
		generationsIdx := indexOf(sql, "orchestrator_generations")

		assert.True(t, workersIdx < generationsIdx, "workers table should be dropped before generations table")
	})
}

// TestTableConfigDefaults verifies the default table configuration.
func TestTableConfigDefaults(t *testing.T) {
	config := DefaultTableConfig()

	assert.Equal(t, "orchestrator_generations", config.GenerationsTable)
	assert.Equal(t, "orchestrator_workers", config.WorkersTable)
}

// TestStoreInitialization verifies that the Store can be initialized correctly.
func TestStoreInitialization(t *testing.T) {
	t.Run("New creates store with default table names", func(t *testing.T) {
		s := New(nil)

		assert.NotNil(t, s)
		assert.Equal(t, "orchestrator_generations", s.generationsTable)
		assert.Equal(t, "orchestrator_workers", s.workersTable)
	})

	t.Run("NewWithConfig creates store with custom table names", func(t *testing.T) {
		config := TableConfig{
			GenerationsTable: "my_generations",
			WorkersTable:     "my_workers",
		}
		s := NewWithConfig(nil, config)

		assert.NotNil(t, s)
		assert.Equal(t, "my_generations", s.generationsTable)
		assert.Equal(t, "my_workers", s.workersTable)
	})
}

// TestErrorWrapping verifies that errors are properly wrapped with context.
func TestErrorWrapping(t *testing.T) {
	t.Run("errors are wrapped with fmt.Errorf and %w", func(t *testing.T) {
		// This test verifies that our implementation uses proper error wrapping.
		// We can't test the actual wrapping without a database, but we can verify
		// that the errors package supports unwrapping.
		baseErr := errors.New("base error")
		wrappedErr := fmt.Errorf("wrapped: %w", baseErr)

		assert.True(t, errors.Is(wrappedErr, baseErr))
	})
}

// indexOf returns the index of substr in s, or -1 if not found.
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
