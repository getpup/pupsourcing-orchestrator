//go:build integration

package migrations_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/getpup/pupsourcing-orchestrator/pkg/migrations"
)

// NOTE: Integration tests use string interpolation for SQL queries with validated
// configuration values. This is acceptable in test code as all config values are
// controlled by the test and have been validated by the migrations package.
// Production code should always use parameterized queries.

func TestIntegrationPostgres(t *testing.T) {
	// Skip if DATABASE_URL not set
	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		t.Skip("POSTGRES_URL not set, skipping PostgreSQL integration test")
	}

	tmpDir := t.TempDir()
	config := migrations.Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "postgres_integration.sql",
		SchemaName:            "orchestrator_test",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	// Generate migration
	err := migrations.GeneratePostgres(&config)
	if err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	// Read migration file
	migrationPath := filepath.Join(tmpDir, config.OutputFilename)
	migrationSQL, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("Failed to read migration file: %v", err)
	}

	// Connect to database
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Execute migration
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}

	// Verify schema exists
	var schemaExists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", config.SchemaName).Scan(&schemaExists)
	if err != nil {
		t.Fatalf("Failed to check schema existence: %v", err)
	}
	if !schemaExists {
		t.Errorf("Schema %s was not created", config.SchemaName)
	}

	// Verify projection_shards table
	var projectionShardsExists bool
	err = db.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')",
		config.SchemaName, config.ProjectionShardsTable)).Scan(&projectionShardsExists)
	if err != nil {
		t.Fatalf("Failed to check projection_shards table: %v", err)
	}
	if !projectionShardsExists {
		t.Error("projection_shards table was not created")
	}

	// Verify recreate_lock table and singleton row
	var recreateLockExists bool
	err = db.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')",
		config.SchemaName, config.RecreateLockTable)).Scan(&recreateLockExists)
	if err != nil {
		t.Fatalf("Failed to check recreate_lock table: %v", err)
	}
	if !recreateLockExists {
		t.Error("recreate_lock table was not created")
	}

	// Verify singleton row in recreate_lock
	var lockID int
	var generation int64
	var phase string
	err = db.QueryRow(fmt.Sprintf("SELECT lock_id, generation, phase FROM %s.%s WHERE lock_id = 1",
		config.SchemaName, config.RecreateLockTable)).Scan(&lockID, &generation, &phase)
	if err != nil {
		t.Fatalf("Failed to query recreate_lock singleton: %v", err)
	}
	if lockID != 1 || generation != 0 || phase != "idle" {
		t.Errorf("Singleton row not initialized correctly: lock_id=%d, generation=%d, phase=%s", lockID, generation, phase)
	}

	// Verify workers table
	var workersExists bool
	err = db.QueryRow(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s')",
		config.SchemaName, config.WorkersTable)).Scan(&workersExists)
	if err != nil {
		t.Fatalf("Failed to check workers table: %v", err)
	}
	if !workersExists {
		t.Error("workers table was not created")
	}

	// Test inserting data into projection_shards
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s.%s (projection_name, shard_id, shard_count, last_global_position, owner_id, state) VALUES ($1, $2, $3, $4, $5, $6)",
		config.SchemaName, config.ProjectionShardsTable), "test_projection", 0, 4, 0, "worker-1", "assigned")
	if err != nil {
		t.Fatalf("Failed to insert into projection_shards: %v", err)
	}

	// Test inserting data into workers
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s.%s (worker_id, generation_seen, status) VALUES ($1, $2, $3)",
		config.SchemaName, config.WorkersTable), "worker-1", 0, "running")
	if err != nil {
		t.Fatalf("Failed to insert into workers: %v", err)
	}

	// Clean up - drop schema
	_, err = db.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", config.SchemaName))
	if err != nil {
		t.Logf("Warning: Failed to clean up schema: %v", err)
	}
}

func TestIntegrationMySQL(t *testing.T) {
	// Skip if MYSQL_URL not set
	dbURL := os.Getenv("MYSQL_URL")
	if dbURL == "" {
		t.Skip("MYSQL_URL not set, skipping MySQL integration test")
	}

	tmpDir := t.TempDir()
	config := migrations.Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "mysql_integration.sql",
		SchemaName:            "orchestrator_test",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	// Generate migration
	err := migrations.GenerateMySQL(&config)
	if err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	// Read migration file
	migrationPath := filepath.Join(tmpDir, config.OutputFilename)
	migrationSQL, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("Failed to read migration file: %v", err)
	}

	// Connect to database
	db, err := sql.Open("mysql", dbURL+"?multiStatements=true")
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	// Execute migration
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}

	// Verify database exists
	var dbExists int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", config.SchemaName).Scan(&dbExists)
	if err != nil {
		t.Fatalf("Failed to check database existence: %v", err)
	}
	if dbExists == 0 {
		t.Errorf("Database %s was not created", config.SchemaName)
	}

	// Switch to the test database
	_, err = db.Exec(fmt.Sprintf("USE %s", config.SchemaName))
	if err != nil {
		t.Fatalf("Failed to switch to test database: %v", err)
	}

	// Verify projection_shards table
	var projectionShardsExists int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		config.SchemaName, config.ProjectionShardsTable).Scan(&projectionShardsExists)
	if err != nil {
		t.Fatalf("Failed to check projection_shards table: %v", err)
	}
	if projectionShardsExists == 0 {
		t.Error("projection_shards table was not created")
	}

	// Verify recreate_lock table and singleton row
	var recreateLockExists int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		config.SchemaName, config.RecreateLockTable).Scan(&recreateLockExists)
	if err != nil {
		t.Fatalf("Failed to check recreate_lock table: %v", err)
	}
	if recreateLockExists == 0 {
		t.Error("recreate_lock table was not created")
	}

	// Verify singleton row in recreate_lock
	var lockID int
	var generation int64
	var phase string
	err = db.QueryRow(fmt.Sprintf("SELECT lock_id, generation, phase FROM %s WHERE lock_id = 1",
		config.RecreateLockTable)).Scan(&lockID, &generation, &phase)
	if err != nil {
		t.Fatalf("Failed to query recreate_lock singleton: %v", err)
	}
	if lockID != 1 || generation != 0 || phase != "idle" {
		t.Errorf("Singleton row not initialized correctly: lock_id=%d, generation=%d, phase=%s", lockID, generation, phase)
	}

	// Verify workers table
	var workersExists int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		config.SchemaName, config.WorkersTable).Scan(&workersExists)
	if err != nil {
		t.Fatalf("Failed to check workers table: %v", err)
	}
	if workersExists == 0 {
		t.Error("workers table was not created")
	}

	// Test inserting data into projection_shards
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (projection_name, shard_id, shard_count, last_global_position, owner_id, state) VALUES (?, ?, ?, ?, ?, ?)",
		config.ProjectionShardsTable), "test_projection", 0, 4, 0, "worker-1", "assigned")
	if err != nil {
		t.Fatalf("Failed to insert into projection_shards: %v", err)
	}

	// Test inserting data into workers
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (worker_id, generation_seen, status) VALUES (?, ?, ?)",
		config.WorkersTable), "worker-1", 0, "running")
	if err != nil {
		t.Fatalf("Failed to insert into workers: %v", err)
	}

	// Clean up - drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", config.SchemaName))
	if err != nil {
		t.Logf("Warning: Failed to clean up database: %v", err)
	}
}

func TestIntegrationSQLite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := migrations.Config{
		OutputFolder:          tmpDir,
		OutputFilename:        "sqlite_integration.sql",
		SchemaName:            "orchestrator",
		ProjectionShardsTable: "projection_shards",
		RecreateLockTable:     "recreate_lock",
		WorkersTable:          "workers",
	}

	// Generate migration
	err := migrations.GenerateSQLite(&config)
	if err != nil {
		t.Fatalf("Failed to generate migration: %v", err)
	}

	// Read migration file
	migrationPath := filepath.Join(tmpDir, config.OutputFilename)
	migrationSQL, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("Failed to read migration file: %v", err)
	}

	// Connect to database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to connect to SQLite: %v", err)
	}
	defer db.Close()

	// Execute migration
	_, err = db.Exec(string(migrationSQL))
	if err != nil {
		t.Fatalf("Failed to execute migration: %v", err)
	}

	// SQLite uses table name prefixes instead of schemas
	projectionShardsTable := config.SchemaName + "_" + config.ProjectionShardsTable
	recreateLockTable := config.SchemaName + "_" + config.RecreateLockTable
	workersTable := config.SchemaName + "_" + config.WorkersTable

	// Verify projection_shards table
	var projectionShardsExists int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		projectionShardsTable).Scan(&projectionShardsExists)
	if err != nil {
		t.Fatalf("Failed to check projection_shards table: %v", err)
	}
	if projectionShardsExists == 0 {
		t.Error("projection_shards table was not created")
	}

	// Verify recreate_lock table and singleton row
	var recreateLockExists int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		recreateLockTable).Scan(&recreateLockExists)
	if err != nil {
		t.Fatalf("Failed to check recreate_lock table: %v", err)
	}
	if recreateLockExists == 0 {
		t.Error("recreate_lock table was not created")
	}

	// Verify singleton row in recreate_lock
	var lockID int
	var generation int64
	var phase string
	err = db.QueryRow(fmt.Sprintf("SELECT lock_id, generation, phase FROM %s WHERE lock_id = 1",
		recreateLockTable)).Scan(&lockID, &generation, &phase)
	if err != nil {
		t.Fatalf("Failed to query recreate_lock singleton: %v", err)
	}
	if lockID != 1 || generation != 0 || phase != "idle" {
		t.Errorf("Singleton row not initialized correctly: lock_id=%d, generation=%d, phase=%s", lockID, generation, phase)
	}

	// Verify workers table
	var workersExists int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		workersTable).Scan(&workersExists)
	if err != nil {
		t.Fatalf("Failed to check workers table: %v", err)
	}
	if workersExists == 0 {
		t.Error("workers table was not created")
	}

	// Test inserting data into projection_shards
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (projection_name, shard_id, shard_count, last_global_position, owner_id, state) VALUES (?, ?, ?, ?, ?, ?)",
		projectionShardsTable), "test_projection", 0, 4, 0, "worker-1", "assigned")
	if err != nil {
		t.Fatalf("Failed to insert into projection_shards: %v", err)
	}

	// Test inserting data into workers
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (worker_id, generation_seen, status) VALUES (?, ?, ?)",
		workersTable), "worker-1", 0, "running")
	if err != nil {
		t.Fatalf("Failed to insert into workers: %v", err)
	}
}
