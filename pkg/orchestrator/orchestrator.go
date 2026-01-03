package orchestrator

import (
	"database/sql"
	"fmt"
	"time"

	rootpkg "github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/executor"
	"github.com/getpup/pupsourcing-orchestrator/recreate"
	"github.com/getpup/pupsourcing-orchestrator/store"
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

// Option configures an Orchestrator.
type Option func(*config)

// config holds the internal configuration for creating an Orchestrator.
type config struct {
	genStore             store.GenerationStore
	executor             executor.Runner
	heartbeatInterval    time.Duration
	staleWorkerTimeout   time.Duration
	coordinationTimeout  time.Duration
	pollInterval         time.Duration
	batchSize            int
	registrationWaitTime time.Duration
	logger               es.Logger
	metricsEnabled       *bool
	tableConfig          postgres.TableConfig
}

// New creates a new Orchestrator with the given required parameters and optional configuration.
//
// Required parameters:
//   - db: database connection for storing generation state and projection processors
//   - eventStore: event store for reading events
//   - replicaSet: name of the replica set this orchestrator manages
//
// Optional configuration (with defaults):
//   - WithHeartbeatInterval: interval between heartbeats (default: 5s)
//   - WithStaleWorkerTimeout: duration after which a worker is considered dead (default: 30s)
//   - WithCoordinationTimeout: max time to wait for coordination (default: 60s)
//   - WithPollInterval: how often to check state (default: 1s)
//   - WithBatchSize: number of events to read per batch (default: 100)
//   - WithRegistrationWaitTime: time to wait for workers to register (default: 5s)
//   - WithLogger: logger for observability (default: nil)
//   - WithMetricsEnabled: enable Prometheus metrics (default: true)
//   - WithTableNames: custom table names for generation store (default: orchestrator_generations, orchestrator_workers)
//   - WithGenerationStore: custom generation store (default: PostgreSQL store with DB)
//   - WithExecutor: custom executor for running projections (default: executor.New)
//
// Example:
//
//	orch, err := orchestrator.New(
//	    db,
//	    eventStore,
//	    "main-projections",
//	    orchestrator.WithTableNames("custom_generations", "custom_workers"),
//	    orchestrator.WithBatchSize(500),
//	)
//
// Returns an error if any required parameter is nil or empty.
func New(db *sql.DB, eventStore *espostgres.Store, replicaSet ReplicaSetName, opts ...Option) (rootpkg.Orchestrator, error) {
	// Validate required parameters
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}
	if eventStore == nil {
		return nil, fmt.Errorf("event store is required")
	}
	if replicaSet == "" {
		return nil, fmt.Errorf("replica set is required")
	}

	// Apply defaults
	cfg := &config{
		heartbeatInterval:    5 * time.Second,
		staleWorkerTimeout:   30 * time.Second,
		coordinationTimeout:  60 * time.Second,
		pollInterval:         1 * time.Second,
		batchSize:            100,
		registrationWaitTime: 5 * time.Second,
		tableConfig:          postgres.DefaultTableConfig(),
	}

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	// Create generation store if not provided
	if cfg.genStore == nil {
		cfg.genStore = postgres.NewWithConfig(db, cfg.tableConfig)
	}

	// Create executor if not provided
	if cfg.executor == nil {
		cfg.executor = executor.New(executor.Config{
			DB:         db,
			EventStore: eventStore,
			BatchSize:  cfg.batchSize,
			Logger:     cfg.logger,
		})
	}

	// Create and return recreate orchestrator
	orch := recreate.New(recreate.Config{
		DB:                   db,
		EventStore:           eventStore,
		GenStore:             cfg.genStore,
		ReplicaSet:           replicaSet,
		Executor:             cfg.executor,
		HeartbeatInterval:    cfg.heartbeatInterval,
		StaleWorkerTimeout:   cfg.staleWorkerTimeout,
		CoordinationTimeout:  cfg.coordinationTimeout,
		PollInterval:         cfg.pollInterval,
		BatchSize:            cfg.batchSize,
		RegistrationWaitTime: cfg.registrationWaitTime,
		Logger:               cfg.logger,
		MetricsEnabled:       cfg.metricsEnabled,
	})

	return orch, nil
}

// WithHeartbeatInterval sets the interval between heartbeats.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(c *config) {
		c.heartbeatInterval = interval
	}
}

// WithStaleWorkerTimeout sets the duration after which a worker is considered dead.
func WithStaleWorkerTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.staleWorkerTimeout = timeout
	}
}

// WithCoordinationTimeout sets the max time to wait for coordination.
func WithCoordinationTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.coordinationTimeout = timeout
	}
}

// WithPollInterval sets how often to check state during coordination.
func WithPollInterval(interval time.Duration) Option {
	return func(c *config) {
		c.pollInterval = interval
	}
}

// WithBatchSize sets the number of events to read per batch.
// The batch size must be greater than zero.
func WithBatchSize(size int) Option {
	return func(c *config) {
		if size > 0 {
			c.batchSize = size
		}
	}
}

// WithRegistrationWaitTime sets how long to wait for workers to register before assigning partitions.
func WithRegistrationWaitTime(duration time.Duration) Option {
	return func(c *config) {
		c.registrationWaitTime = duration
	}
}

// WithLogger sets the logger for observability.
func WithLogger(logger es.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

// WithMetricsEnabled enables or disables Prometheus metrics collection.
func WithMetricsEnabled(enabled bool) Option {
	return func(c *config) {
		c.metricsEnabled = &enabled
	}
}

// WithTableNames sets custom table names for the generation store.
// This allows you to use custom table names instead of the defaults:
//   - generationsTable: default is "orchestrator_generations"
//   - workersTable: default is "orchestrator_workers"
//
// Both table names must be non-empty strings.
func WithTableNames(generationsTable, workersTable string) Option {
	return func(c *config) {
		if generationsTable != "" && workersTable != "" {
			c.tableConfig = postgres.TableConfig{
				GenerationsTable: generationsTable,
				WorkersTable:     workersTable,
			}
		}
	}
}

// WithGenerationStore sets a custom generation store.
// Use this if you want to provide your own implementation of store.GenerationStore.
func WithGenerationStore(genStore store.GenerationStore) Option {
	return func(c *config) {
		c.genStore = genStore
	}
}

// WithExecutor sets a custom executor for running projections.
// Use this if you want to provide your own implementation of executor.Runner.
func WithExecutor(exec executor.Runner) Option {
	return func(c *config) {
		c.executor = exec
	}
}

// RunMigrations executes the database migrations required for the orchestrator.
// It creates the necessary tables for generation and worker state tracking.
//
// This should typically be run once during application deployment or startup.
//
// To run migrations with custom table names, use RunMigrationsWithTableNames.
func RunMigrations(db *sql.DB) error {
	return RunMigrationsWithTableNames(db, postgres.DefaultTableConfig())
}

// RunMigrationsWithTableNames executes database migrations with custom table names.
// Use this if you specified custom table names via WithTableNames option.
//
// Example:
//
//	tableConfig := postgres.TableConfig{
//	    GenerationsTable: "custom_generations",
//	    WorkersTable:     "custom_workers",
//	}
//	if err := orchestrator.RunMigrationsWithTableNames(db, tableConfig); err != nil {
//	    log.Fatal(err)
//	}
func RunMigrationsWithTableNames(db *sql.DB, config postgres.TableConfig) error {
	sql := postgres.MigrationUp(config)

	_, err := db.Exec(sql)
	if err != nil {
		return fmt.Errorf("failed to execute migrations: %w", err)
	}

	return nil
}
