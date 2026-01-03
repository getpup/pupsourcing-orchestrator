//go:build integration

package integration_test

import (
	"testing"
)

// TestSetupHelpers validates that the integration test helper functions work correctly.
// This test requires a PostgreSQL database to be available via DATABASE_URL.
func TestSetupHelpers(t *testing.T) {
	// Get database connection
	db := getTestDB(t)
	defer db.Close()

	// Setup tables
	setupTables(t, db)

	// Verify tables were created by querying them
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM orchestrator_generations").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query generations table: %v", err)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM orchestrator_workers").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query workers table: %v", err)
	}

	// Cleanup tables
	cleanupTables(t, db)

	// Verify tables are empty after cleanup
	err = db.QueryRow("SELECT COUNT(*) FROM orchestrator_generations").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query generations table after cleanup: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows in generations table after cleanup, got %d", count)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM orchestrator_workers").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query workers table after cleanup: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 rows in workers table after cleanup, got %d", count)
	}

	// Teardown tables
	teardownTables(t, db)

	// Verify tables were dropped by trying to query them
	// This should fail since the tables no longer exist
	err = db.QueryRow("SELECT COUNT(*) FROM orchestrator_generations").Scan(&count)
	if err == nil {
		t.Error("expected error querying dropped generations table, but got none")
	}

	err = db.QueryRow("SELECT COUNT(*) FROM orchestrator_workers").Scan(&count)
	if err == nil {
		t.Error("expected error querying dropped workers table, but got none")
	}
}
