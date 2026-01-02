package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var identifierRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

// validateIdentifier ensures an identifier contains only safe characters for SQL.
// Returns an error if the identifier contains characters that could be used for SQL injection.
func validateIdentifier(name, fieldName string) error {
	if name == "" {
		return fmt.Errorf("%s cannot be empty", fieldName)
	}
	if !identifierRegex.MatchString(name) {
		return fmt.Errorf("%s must start with a letter and contain only letters, numbers, and underscores (got: %s)", fieldName, name)
	}
	return nil
}

// validateConfig validates all configuration values to prevent SQL injection.
func validateConfig(config *Config) error {
	if err := validateIdentifier(config.SchemaName, "SchemaName"); err != nil {
		return err
	}
	if err := validateIdentifier(config.ProjectionShardsTable, "ProjectionShardsTable"); err != nil {
		return err
	}
	if err := validateIdentifier(config.RecreateLockTable, "RecreateLockTable"); err != nil {
		return err
	}
	if err := validateIdentifier(config.WorkersTable, "WorkersTable"); err != nil {
		return err
	}
	return nil
}

// Config configures migration generation for orchestrator coordination tables.
type Config struct {
	// OutputFolder is the directory where the migration file will be written
	OutputFolder string

	// OutputFilename is the name of the migration file
	OutputFilename string

	// SchemaName is the database schema name (PostgreSQL) or database name prefix (MySQL)
	// For SQLite, table name prefixes are used instead of schemas (e.g., orchestrator_table_name)
	SchemaName string

	// ProjectionShardsTable is the name of the projection shards coordination table
	ProjectionShardsTable string

	// RecreateLockTable is the name of the recreate strategy lock table
	RecreateLockTable string

	// WorkersTable is the name of the workers heartbeat table
	WorkersTable string
}

// DefaultConfig returns the default configuration for orchestrator migrations.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")
	return Config{
		OutputFolder:          "migrations",
		OutputFilename:        fmt.Sprintf("%s_init_orchestrator_coordination.sql", timestamp),
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}
}

// GeneratePostgres generates a PostgreSQL migration file.
func GeneratePostgres(config *Config) error {
	// Validate configuration to prevent SQL injection
	if err := validateConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generatePostgresSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generatePostgresSQL(config *Config) string {
	return fmt.Sprintf(`-- Orchestrator Coordination Infrastructure Migration
-- Generated: %s
-- Database: PostgreSQL

-- Create orchestrator schema for coordination tables
CREATE SCHEMA IF NOT EXISTS %s;

-- Projection shards table manages shard ownership and coordination for projections
-- Ensures each shard of a projection is owned by at most one worker at a time
-- Supports horizontal scaling by partitioning projection workload across shards
CREATE TABLE IF NOT EXISTS %s.%s (
    projection_name TEXT NOT NULL,
    shard_id INT NOT NULL,
    shard_count INT NOT NULL,
    last_global_position BIGINT NOT NULL DEFAULT 0,
    owner_id TEXT,
    state TEXT NOT NULL DEFAULT 'idle' CHECK (state IN ('idle', 'assigned', 'draining')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (projection_name, shard_id),
    
    -- Ensure shard_id is within valid range for shard_count
    CHECK (shard_id >= 0 AND shard_id < shard_count)
);

-- Index for querying shards by state (e.g., finding idle shards to assign)
CREATE INDEX IF NOT EXISTS idx_%s_state 
    ON %s.%s (state, projection_name, shard_id);

-- Index for querying shards by owner (e.g., finding all shards owned by a worker)
CREATE INDEX IF NOT EXISTS idx_%s_owner 
    ON %s.%s (owner_id, projection_name) WHERE owner_id IS NOT NULL;

-- Index for observability and monitoring
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s.%s (updated_at DESC);

-- Recreate lock table coordinates recreate strategy deployments
-- Singleton table (single row) that tracks deployment generation and phase
-- Prevents parallel deployments and ensures consistent state transitions
-- No advisory locks needed - uses standard transactional operations
CREATE TABLE IF NOT EXISTS %s.%s (
    lock_id INT PRIMARY KEY DEFAULT 1 CHECK (lock_id = 1),
    generation BIGINT NOT NULL DEFAULT 0,
    phase TEXT NOT NULL DEFAULT 'idle' CHECK (phase IN ('idle', 'draining', 'assigning', 'running')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Initialize the singleton lock row
INSERT INTO %s.%s (lock_id, generation, phase, updated_at)
VALUES (1, 0, 'idle', NOW())
ON CONFLICT (lock_id) DO NOTHING;

-- Index for observability
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s.%s (updated_at DESC);

-- Workers table tracks worker heartbeats and status
-- Enables worker discovery, health monitoring, and failure detection
-- Workers must regularly update their heartbeat to remain active
CREATE TABLE IF NOT EXISTS %s.%s (
    worker_id TEXT PRIMARY KEY,
    generation_seen BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'starting' CHECK (status IN ('starting', 'ready', 'running', 'draining', 'stopped')),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for querying workers by status
CREATE INDEX IF NOT EXISTS idx_%s_status 
    ON %s.%s (status, worker_id);

-- Index for finding stale workers by heartbeat
CREATE INDEX IF NOT EXISTS idx_%s_heartbeat 
    ON %s.%s (last_heartbeat DESC);

-- Index for querying workers by generation
CREATE INDEX IF NOT EXISTS idx_%s_generation 
    ON %s.%s (generation_seen, worker_id);
`,
		time.Now().Format(time.RFC3339),
		config.SchemaName,
		config.SchemaName, config.ProjectionShardsTable,
		config.ProjectionShardsTable, config.SchemaName, config.ProjectionShardsTable,
		config.ProjectionShardsTable, config.SchemaName, config.ProjectionShardsTable,
		config.ProjectionShardsTable, config.SchemaName, config.ProjectionShardsTable,
		config.SchemaName, config.RecreateLockTable,
		config.SchemaName, config.RecreateLockTable,
		config.RecreateLockTable, config.SchemaName, config.RecreateLockTable,
		config.SchemaName, config.WorkersTable,
		config.WorkersTable, config.SchemaName, config.WorkersTable,
		config.WorkersTable, config.SchemaName, config.WorkersTable,
		config.WorkersTable, config.SchemaName, config.WorkersTable,
	)
}

// GenerateMySQL generates a MySQL/MariaDB migration file.
func GenerateMySQL(config *Config) error {
	// Validate configuration to prevent SQL injection
	if err := validateConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generateMySQLSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generateMySQLSQL(config *Config) string {
	return fmt.Sprintf(`-- Orchestrator Coordination Infrastructure Migration
-- Generated: %s
-- Database: MySQL/MariaDB

-- Create database for orchestrator coordination if it doesn't exist
-- In MySQL, we use a separate database instead of schema
CREATE DATABASE IF NOT EXISTS %s
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

-- Switch to orchestrator database
USE %s;

-- Projection shards table manages shard ownership and coordination for projections
-- Ensures each shard of a projection is owned by at most one worker at a time
-- Supports horizontal scaling by partitioning projection workload across shards
CREATE TABLE IF NOT EXISTS %s (
    projection_name VARCHAR(255) NOT NULL,
    shard_id INT NOT NULL,
    shard_count INT NOT NULL,
    last_global_position BIGINT NOT NULL DEFAULT 0,
    owner_id VARCHAR(255),
    state ENUM('idle', 'assigned', 'draining') NOT NULL DEFAULT 'idle',
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    PRIMARY KEY (projection_name, shard_id),
    
    -- Ensure shard_id is within valid range for shard_count
    CHECK (shard_id >= 0 AND shard_id < shard_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for querying shards by state
CREATE INDEX idx_%s_state 
    ON %s (state, projection_name, shard_id);

-- Index for querying shards by owner
CREATE INDEX idx_%s_owner 
    ON %s (owner_id, projection_name);

-- Index for observability and monitoring
CREATE INDEX idx_%s_updated 
    ON %s (updated_at DESC);

-- Recreate lock table coordinates recreate strategy deployments
-- Singleton table (single row) that tracks deployment generation and phase
-- Prevents parallel deployments and ensures consistent state transitions
-- No advisory locks needed - uses standard transactional operations
CREATE TABLE IF NOT EXISTS %s (
    lock_id INT PRIMARY KEY DEFAULT 1,
    generation BIGINT NOT NULL DEFAULT 0,
    phase ENUM('idle', 'draining', 'assigning', 'running') NOT NULL DEFAULT 'idle',
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    
    CHECK (lock_id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Initialize the singleton lock row
INSERT INTO %s (lock_id, generation, phase, updated_at)
VALUES (1, 0, 'idle', CURRENT_TIMESTAMP(6))
ON DUPLICATE KEY UPDATE lock_id = lock_id;

-- Index for observability
CREATE INDEX idx_%s_updated 
    ON %s (updated_at DESC);

-- Workers table tracks worker heartbeats and status
-- Enables worker discovery, health monitoring, and failure detection
-- Workers must regularly update their heartbeat to remain active
CREATE TABLE IF NOT EXISTS %s (
    worker_id VARCHAR(255) PRIMARY KEY,
    generation_seen BIGINT NOT NULL DEFAULT 0,
    status ENUM('starting', 'ready', 'running', 'draining', 'stopped') NOT NULL DEFAULT 'starting',
    last_heartbeat TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for querying workers by status
CREATE INDEX idx_%s_status 
    ON %s (status, worker_id);

-- Index for finding stale workers by heartbeat
CREATE INDEX idx_%s_heartbeat 
    ON %s (last_heartbeat DESC);

-- Index for querying workers by generation
CREATE INDEX idx_%s_generation 
    ON %s (generation_seen, worker_id);
`,
		time.Now().Format(time.RFC3339),
		config.SchemaName,
		config.SchemaName,
		config.ProjectionShardsTable,
		config.ProjectionShardsTable, config.ProjectionShardsTable,
		config.ProjectionShardsTable, config.ProjectionShardsTable,
		config.ProjectionShardsTable, config.ProjectionShardsTable,
		config.RecreateLockTable,
		config.RecreateLockTable,
		config.RecreateLockTable, config.RecreateLockTable,
		config.WorkersTable,
		config.WorkersTable, config.WorkersTable,
		config.WorkersTable, config.WorkersTable,
		config.WorkersTable, config.WorkersTable,
	)
}

// GenerateSQLite generates a SQLite migration file.
func GenerateSQLite(config *Config) error {
	// Validate configuration to prevent SQL injection
	if err := validateConfig(config); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generateSQLiteSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generateSQLiteSQL(config *Config) string {
	// SQLite doesn't support schemas, so we use table name prefixes instead
	projectionShardsTable := config.SchemaName + "_" + config.ProjectionShardsTable
	recreateLockTable := config.SchemaName + "_" + config.RecreateLockTable
	workersTable := config.SchemaName + "_" + config.WorkersTable

	return fmt.Sprintf(`-- Orchestrator Coordination Infrastructure Migration
-- Generated: %s
-- Database: SQLite

-- Projection shards table manages shard ownership and coordination for projections
-- Ensures each shard of a projection is owned by at most one worker at a time
-- Supports horizontal scaling by partitioning projection workload across shards
CREATE TABLE IF NOT EXISTS %s (
    projection_name TEXT NOT NULL,
    shard_id INTEGER NOT NULL,
    shard_count INTEGER NOT NULL,
    last_global_position INTEGER NOT NULL DEFAULT 0,
    owner_id TEXT,
    state TEXT NOT NULL DEFAULT 'idle' CHECK (state IN ('idle', 'assigned', 'draining')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    
    PRIMARY KEY (projection_name, shard_id),
    
    -- Ensure shard_id is within valid range for shard_count
    CHECK (shard_id >= 0 AND shard_id < shard_count)
);

-- Index for querying shards by state
CREATE INDEX IF NOT EXISTS idx_%s_state 
    ON %s (state, projection_name, shard_id);

-- Index for querying shards by owner
CREATE INDEX IF NOT EXISTS idx_%s_owner 
    ON %s (owner_id, projection_name) WHERE owner_id IS NOT NULL;

-- Index for observability and monitoring
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at DESC);

-- Recreate lock table coordinates recreate strategy deployments
-- Singleton table (single row) that tracks deployment generation and phase
-- Prevents parallel deployments and ensures consistent state transitions
-- No advisory locks needed - uses standard transactional operations
CREATE TABLE IF NOT EXISTS %s (
    lock_id INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock_id = 1),
    generation INTEGER NOT NULL DEFAULT 0,
    phase TEXT NOT NULL DEFAULT 'idle' CHECK (phase IN ('idle', 'draining', 'assigning', 'running')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Initialize the singleton lock row
INSERT OR IGNORE INTO %s (lock_id, generation, phase, updated_at)
VALUES (1, 0, 'idle', datetime('now'));

-- Index for observability
CREATE INDEX IF NOT EXISTS idx_%s_updated 
    ON %s (updated_at DESC);

-- Workers table tracks worker heartbeats and status
-- Enables worker discovery, health monitoring, and failure detection
-- Workers must regularly update their heartbeat to remain active
CREATE TABLE IF NOT EXISTS %s (
    worker_id TEXT PRIMARY KEY,
    generation_seen INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'starting' CHECK (status IN ('starting', 'ready', 'running', 'draining', 'stopped')),
    last_heartbeat TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Index for querying workers by status
CREATE INDEX IF NOT EXISTS idx_%s_status 
    ON %s (status, worker_id);

-- Index for finding stale workers by heartbeat
CREATE INDEX IF NOT EXISTS idx_%s_heartbeat 
    ON %s (last_heartbeat DESC);

-- Index for querying workers by generation
CREATE INDEX IF NOT EXISTS idx_%s_generation 
    ON %s (generation_seen, worker_id);
`,
		time.Now().Format(time.RFC3339),
		projectionShardsTable,
		projectionShardsTable, projectionShardsTable,
		projectionShardsTable, projectionShardsTable,
		projectionShardsTable, projectionShardsTable,
		recreateLockTable,
		recreateLockTable,
		recreateLockTable, recreateLockTable,
		workersTable,
		workersTable, workersTable,
		workersTable, workersTable,
		workersTable, workersTable,
	)
}
