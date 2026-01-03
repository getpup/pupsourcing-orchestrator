package postgres

import "fmt"

// TableConfig configures the table names used by the orchestrator.
type TableConfig struct {
	// GenerationsTable is the name of the table storing generation metadata.
	GenerationsTable string

	// WorkersTable is the name of the table storing worker metadata.
	WorkersTable string
}

// DefaultTableConfig returns the default table configuration.
func DefaultTableConfig() TableConfig {
	return TableConfig{
		GenerationsTable: "orchestrator_generations",
		WorkersTable:     "orchestrator_workers",
	}
}

// MigrationUp returns the SQL to create orchestrator tables.
// It creates the generations table with an index on replica_set and created_at,
// and the workers table with indexes on replica_set, generation_id, and state.
func MigrationUp(config TableConfig) string {
	return fmt.Sprintf(`-- Create orchestrator_generations table
CREATE TABLE %s (
    id UUID PRIMARY KEY,
    replica_set TEXT NOT NULL,
    total_partitions INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for finding the most recent generation for a replica set
CREATE INDEX idx_generations_replica_set ON %s(replica_set, created_at DESC);

-- Create orchestrator_workers table
CREATE TABLE %s (
    id UUID PRIMARY KEY,
    replica_set TEXT NOT NULL,
    generation_id UUID NOT NULL REFERENCES %s(id),
    partition_key INTEGER NOT NULL DEFAULT -1,
    state TEXT NOT NULL DEFAULT 'pending',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for finding workers by replica set
CREATE INDEX idx_workers_replica_set ON %s(replica_set);

-- Index for finding workers by generation
CREATE INDEX idx_workers_generation ON %s(generation_id);

-- Index for finding workers by state
CREATE INDEX idx_workers_state ON %s(state);
`, config.GenerationsTable, config.GenerationsTable, config.WorkersTable, config.GenerationsTable, config.WorkersTable, config.WorkersTable, config.WorkersTable)
}

// MigrationDown returns the SQL to drop orchestrator tables.
// It drops the workers table first due to the foreign key constraint,
// then drops the generations table.
func MigrationDown(config TableConfig) string {
	return fmt.Sprintf(`-- Drop orchestrator_workers table (must be dropped first due to foreign key)
DROP TABLE IF EXISTS %s;

-- Drop orchestrator_generations table
DROP TABLE IF EXISTS %s;
`, config.WorkersTable, config.GenerationsTable)
}
