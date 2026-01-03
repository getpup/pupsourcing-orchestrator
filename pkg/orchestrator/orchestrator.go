package orchestrator

import (
	"database/sql"
	"fmt"
	"time"

	rootpkg "github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/recreate"
	"github.com/getpup/pupsourcing-orchestrator/store/postgres"
	"github.com/getpup/pupsourcing/es"
	espostgres "github.com/getpup/pupsourcing/es/adapters/postgres"
)

// Re-export core types from root package
type (
	// ReplicaSetName identifies a group of projections that scale together.
	ReplicaSetName = rootpkg.ReplicaSetName

	// Generation represents a specific partition configuration for a replica set.
	Generation = rootpkg.Generation

	// Worker represents an orchestrator instance for a replica set.
	Worker = rootpkg.Worker

	// PartitionAssignment contains the partition configuration for a worker.
	PartitionAssignment = rootpkg.PartitionAssignment
)

// Config holds configuration for creating an Orchestrator.
type Config struct {
	// DB is the database connection for storing generation state and projection processors (required).
	DB *sql.DB

	// EventStore is the event store for reading events (required).
	EventStore *espostgres.Store

	// ReplicaSet is the name of the replica set this orchestrator manages (required).
	ReplicaSet ReplicaSetName

	// HeartbeatInterval is the interval between heartbeats (default: 5s).
	HeartbeatInterval time.Duration

	// StaleWorkerTimeout is the duration after which a worker is considered dead (default: 30s).
	StaleWorkerTimeout time.Duration

	// CoordinationTimeout is the max time to wait for coordination (default: 60s).
	CoordinationTimeout time.Duration

	// BatchSize is the number of events to read per batch (default: 100).
	BatchSize int

	// Logger is for observability (optional).
	Logger es.Logger
}

// New creates a new Orchestrator with the given configuration.
// It validates required fields and applies default values for optional fields.
//
// Required fields:
//   - DB: database connection
//   - EventStore: event store for reading events
//   - ReplicaSet: name of the replica set
//
// Returns an error if any required field is missing.
func New(cfg Config) (rootpkg.Orchestrator, error) {
	// Validate required fields
	if cfg.DB == nil {
		return nil, fmt.Errorf("DB is required")
	}
	if cfg.EventStore == nil {
		return nil, fmt.Errorf("EventStore is required")
	}
	if cfg.ReplicaSet == "" {
		return nil, fmt.Errorf("ReplicaSet is required")
	}

	// Apply defaults
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.StaleWorkerTimeout == 0 {
		cfg.StaleWorkerTimeout = 30 * time.Second
	}
	if cfg.CoordinationTimeout == 0 {
		cfg.CoordinationTimeout = 60 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}

	// Create PostgreSQL generation store
	genStore := postgres.New(cfg.DB)

	// Create and return recreate orchestrator
	orch := recreate.New(recreate.Config{
		DB:                  cfg.DB,
		EventStore:          cfg.EventStore,
		GenStore:            genStore,
		ReplicaSet:          cfg.ReplicaSet,
		HeartbeatInterval:   cfg.HeartbeatInterval,
		StaleWorkerTimeout:  cfg.StaleWorkerTimeout,
		CoordinationTimeout: cfg.CoordinationTimeout,
		BatchSize:           cfg.BatchSize,
		Logger:              cfg.Logger,
	})

	return orch, nil
}

// RunMigrations executes the database migrations required for the orchestrator.
// It creates the necessary tables for generation and worker state tracking.
//
// This should typically be run once during application deployment or startup.
func RunMigrations(db *sql.DB) error {
	config := postgres.DefaultTableConfig()
	sql := postgres.MigrationUp(config)

	_, err := db.Exec(sql)
	if err != nil {
		return fmt.Errorf("failed to execute migrations: %w", err)
	}

	return nil
}
