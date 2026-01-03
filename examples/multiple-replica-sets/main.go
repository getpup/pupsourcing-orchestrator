package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
)

// UserProjection is an example projection for the main replica set
type UserProjection struct{}

func (p *UserProjection) Name() string {
	return "user_projection"
}

func (p *UserProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	log.Printf("[main-projections] Processing event: %s at position %d", event.EventType, event.GlobalPosition)
	return nil
}

// AnalyticsProjection is an example projection for the analytics replica set
type AnalyticsProjection struct{}

func (p *AnalyticsProjection) Name() string {
	return "analytics_projection"
}

func (p *AnalyticsProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	log.Printf("[analytics-projections] Processing event: %s at position %d", event.EventType, event.GlobalPosition)
	return nil
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Run migrations (only needs to be done once)
	if err := orchestrator.RunMigrations(db); err != nil {
		_ = db.Close()
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Create event store (shared across both replica sets)
	eventStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create first orchestrator for main projections
	mainOrch, err := orchestrator.New(db, eventStore, "main-projections")
	if err != nil {
		_ = db.Close()
		log.Fatalf("Failed to create main orchestrator: %v", err)
	}

	// Create second orchestrator for analytics projections
	analyticsOrch, err := orchestrator.New(db, eventStore, "analytics-projections")
	if err != nil {
		_ = db.Close()
		log.Fatalf("Failed to create analytics orchestrator: %v", err)
	}

	// Ensure database is closed on exit
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Define projections for each replica set
	mainProjections := []projection.Projection{
		&UserProjection{},
	}
	analyticsProjections := []projection.Projection{
		&AnalyticsProjection{},
	}

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Println("Shutdown signal received")
		cancel()
	}()

	// Run both orchestrators concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	// Run main projections
	go func() {
		defer wg.Done()
		log.Println("Starting main projections orchestrator...")
		if err := mainOrch.Run(ctx, mainProjections); err != nil && err != context.Canceled {
			log.Printf("Main orchestrator error: %v", err)
		}
		log.Println("Main orchestrator stopped")
	}()

	// Run analytics projections
	go func() {
		defer wg.Done()
		log.Println("Starting analytics projections orchestrator...")
		if err := analyticsOrch.Run(ctx, analyticsProjections); err != nil && err != context.Canceled {
			log.Printf("Analytics orchestrator error: %v", err)
		}
		log.Println("Analytics orchestrator stopped")
	}()

	// Wait for both to complete
	wg.Wait()
	log.Println("All orchestrators stopped")
}
