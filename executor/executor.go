package executor

import (
	"context"
	"database/sql"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/getpup/pupsourcing/es/projection/runner"
)

// Config configures the projection executor.
type Config struct {
	// DB is the database connection for projection processors (required).
	DB *sql.DB

	// EventStore is the event store for reading events (required).
	EventStore *postgres.Store

	// BatchSize is the number of events to read per batch (default: 100).
	BatchSize int

	// Logger is an optional logger for observability.
	Logger es.Logger
}

// Executor executes projections with the assigned partition configuration.
type Executor struct {
	config Config
}

// Compile-time check that Executor implements Runner.
var _ Runner = (*Executor)(nil)

// New creates a new Executor with the given configuration.
// It applies default values for BatchSize if zero.
func New(cfg Config) *Executor {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}

	return &Executor{
		config: cfg,
	}
}

// Run executes the given projections with the assigned partition configuration.
// It creates a ProcessorConfig from the assignment, creates a postgres.Processor
// for each projection, and uses runner.New().Run() to execute them concurrently.
// Returns when the context is cancelled or when any projection returns an error.
func (e *Executor) Run(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
	// Create ProcessorConfig from assignment
	processorConfig := &projection.ProcessorConfig{
		BatchSize:         e.config.BatchSize,
		PartitionKey:      assignment.PartitionKey,
		TotalPartitions:   assignment.TotalPartitions,
		PartitionStrategy: projection.HashPartitionStrategy{},
		Logger:            e.config.Logger,
		PollInterval:      100 * time.Millisecond,
	}

	// Create a processor runner for each projection
	runners := make([]runner.ProjectionRunner, len(projections))
	for i, proj := range projections {
		processor := postgres.NewProcessor(e.config.DB, e.config.EventStore, processorConfig)
		runners[i] = runner.ProjectionRunner{
			Projection: proj,
			Processor:  processor,
		}
	}

	// Run all projections concurrently
	return runner.New().Run(ctx, runners)
}
