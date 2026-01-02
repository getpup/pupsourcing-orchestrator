// Package main demonstrates how to generate orchestrator database migrations.
//
// Run this example:
//
//	go run github.com/getpup/pupsourcing-orchestrator/examples/migrations
//
// This will generate migration files for all supported database adapters
// in the 'migrations' folder.
package main

import (
	"fmt"
	"log"

	"github.com/getpup/pupsourcing-orchestrator/pkg/migrations"
)

func main() {
	fmt.Println("Generating orchestrator database migrations...")

	config := migrations.DefaultConfig()
	config.OutputFolder = "migrations"

	// Generate PostgreSQL migration
	fmt.Println("\nGenerating PostgreSQL migration...")
	config.OutputFilename = "001_init_orchestrator_postgres.sql"
	if err := migrations.GeneratePostgres(&config); err != nil {
		log.Fatalf("Failed to generate PostgreSQL migration: %v", err)
	}
	fmt.Printf("✓ Generated: %s/%s\n", config.OutputFolder, config.OutputFilename)

	// Generate MySQL migration
	fmt.Println("\nGenerating MySQL migration...")
	config.OutputFilename = "001_init_orchestrator_mysql.sql"
	if err := migrations.GenerateMySQL(&config); err != nil {
		log.Fatalf("Failed to generate MySQL migration: %v", err)
	}
	fmt.Printf("✓ Generated: %s/%s\n", config.OutputFolder, config.OutputFilename)

	// Generate SQLite migration
	fmt.Println("\nGenerating SQLite migration...")
	config.OutputFilename = "001_init_orchestrator_sqlite.sql"
	if err := migrations.GenerateSQLite(&config); err != nil {
		log.Fatalf("Failed to generate SQLite migration: %v", err)
	}
	fmt.Printf("✓ Generated: %s/%s\n", config.OutputFolder, config.OutputFilename)

	fmt.Println("\n✓ All migrations generated successfully!")
	fmt.Printf("\nApply migrations to your database:\n")
	fmt.Printf("  PostgreSQL: psql -f %s/001_init_orchestrator_postgres.sql\n", config.OutputFolder)
	fmt.Printf("  MySQL:      mysql < %s/001_init_orchestrator_mysql.sql\n", config.OutputFolder)
	fmt.Printf("  SQLite:     sqlite3 mydatabase.db < %s/001_init_orchestrator_sqlite.sql\n", config.OutputFolder)
}
