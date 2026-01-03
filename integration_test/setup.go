//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"testing"

	pgstore "github.com/getpup/pupsourcing-orchestrator/store/postgres"
	_ "github.com/lib/pq"
)

// getTestDB returns a database connection for integration tests.
// It reads the DATABASE_URL environment variable and skips the test if not set.
func getTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	return db
}

// setupTables creates the orchestrator tables using the default configuration.
func setupTables(t *testing.T, db *sql.DB) {
	t.Helper()

	config := pgstore.DefaultTableConfig()
	migrationSQL := pgstore.MigrationUp(config)

	if _, err := db.Exec(migrationSQL); err != nil {
		t.Fatalf("failed to create tables: %v", err)
	}
}

// cleanupTables truncates the orchestrator tables to clean up test data.
// Errors are logged but don't fail the test (cleanup is best-effort).
func cleanupTables(t *testing.T, db *sql.DB) {
	t.Helper()

	config := pgstore.DefaultTableConfig()

	// TRUNCATE workers table first (has foreign key to generations)
	_, err := db.Exec("TRUNCATE " + config.WorkersTable + " CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate workers table: %v", err)
	}

	// TRUNCATE generations table
	_, err = db.Exec("TRUNCATE " + config.GenerationsTable + " CASCADE")
	if err != nil {
		t.Logf("warning: failed to truncate generations table: %v", err)
	}
}

// teardownTables drops the orchestrator tables using the default configuration.
// Errors are logged but don't fail the test.
func teardownTables(t *testing.T, db *sql.DB) {
	t.Helper()

	config := pgstore.DefaultTableConfig()
	migrationSQL := pgstore.MigrationDown(config)

	if _, err := db.Exec(migrationSQL); err != nil {
		t.Logf("warning: failed to drop tables: %v", err)
	}
}
