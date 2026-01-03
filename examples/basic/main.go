package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
)

// UserProjection is an example projection
type UserProjection struct{}

func (p *UserProjection) Name() string {
	return "user_projection"
}

func (p *UserProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	log.Printf("Processing event: %s at position %d", event.EventType, event.GlobalPosition)
	return nil
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Run migrations (typically done once during deployment)
	if err := orchestrator.RunMigrations(db); err != nil {
		_ = db.Close()
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Create event store
	eventStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Define projections to run
	projections := []projection.Projection{
		&UserProjection{},
	}

	// Create orchestrator
	orch, err := orchestrator.New(db, eventStore, "main-projections")
	if err != nil {
		_ = db.Close()
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Ensure database is closed on exit
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Println("Shutdown signal received")
		cancel()
	}()

	// Run orchestrator
	log.Println("Starting orchestrator...")
	if err := orch.Run(ctx, projections); err != nil && err != context.Canceled {
		_ = db.Close()
		log.Fatalf("Orchestrator error: %v", err) //nolint:gocritic // exitAfterDefer is acceptable in example code
	}
	log.Println("Orchestrator stopped")
}
