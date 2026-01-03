package recreate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/coordinator"
	"github.com/getpup/pupsourcing-orchestrator/executor"
	"github.com/getpup/pupsourcing-orchestrator/lifecycle"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
)

// Config holds configuration for the Recreate orchestrator.
type Config struct {
	// DB is the database connection for projection processors (required).
	DB *sql.DB

	// EventStore is the event store for reading events (required).
	EventStore *postgres.Store

	// GenStore is the generation store for worker coordination (required).
	GenStore store.GenerationStore

	// ReplicaSet is the name of the replica set this orchestrator manages (required).
	ReplicaSet orchestrator.ReplicaSetName

	// HeartbeatInterval is the interval between heartbeats (default: 5s).
	HeartbeatInterval time.Duration

	// StaleWorkerTimeout is the duration after which a worker is considered dead (default: 30s).
	StaleWorkerTimeout time.Duration

	// CoordinationTimeout is the max time to wait for coordination (default: 60s).
	CoordinationTimeout time.Duration

	// PollInterval is how often to check state (default: 1s).
	PollInterval time.Duration

	// BatchSize is the number of events to read per batch (default: 100).
	BatchSize int

	// Logger is for observability (optional).
	Logger es.Logger
}

// Orchestrator coordinates the execution of projections across multiple workers.
type Orchestrator struct {
	config      Config
	lifecycle   *lifecycle.Manager
	coordinator *coordinator.Coordinator
	executor    *executor.Executor
}

// New creates a new Orchestrator with the given configuration.
// Applies default values for all duration/int fields if zero.
func New(cfg Config) *Orchestrator {
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
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}

	// Create lifecycle manager
	lifecycleManager := lifecycle.New(lifecycle.Config{
		Store:             cfg.GenStore,
		HeartbeatInterval: cfg.HeartbeatInterval,
		Logger:            cfg.Logger,
	})

	// Create coordinator
	coord := coordinator.New(coordinator.Config{
		Store:               cfg.GenStore,
		StaleWorkerTimeout:  cfg.StaleWorkerTimeout,
		PollInterval:        cfg.PollInterval,
		CoordinationTimeout: cfg.CoordinationTimeout,
		Logger:              cfg.Logger,
	}, cfg.ReplicaSet)

	// Create executor
	exec := executor.New(executor.Config{
		DB:         cfg.DB,
		EventStore: cfg.EventStore,
		BatchSize:  cfg.BatchSize,
		Logger:     cfg.Logger,
	})

	return &Orchestrator{
		config:      cfg,
		lifecycle:   lifecycleManager,
		coordinator: coord,
		executor:    exec,
	}
}

// Run starts the orchestrator for the given projections.
// It implements the Recreate strategy: when workers join or leave, all workers pause,
// reconfigure partition assignments, and restart together.
func (o *Orchestrator) Run(ctx context.Context, projections []projection.Projection) error {
	for {
		// Check if we should exit
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// 1. Cleanup any stale workers from previous runs
		if err := o.coordinator.CleanupStaleWorkers(ctx); err != nil {
			if o.config.Logger != nil {
				o.config.Logger.Error(ctx, "failed to cleanup stale workers", "error", err)
			}
		}

		// 2. Join or create generation
		generation, err := o.coordinator.JoinOrCreate(ctx)
		if err != nil {
			return fmt.Errorf("failed to join generation: %w", err)
		}

		// 3. Register this worker
		workerID, err := o.lifecycle.Register(ctx, o.config.ReplicaSet, generation.ID)
		if err != nil {
			return fmt.Errorf("failed to register worker: %w", err)
		}

		// 4. Start heartbeat in background
		heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
		heartbeatDone := make(chan error, 1)
		go func() {
			heartbeatDone <- o.lifecycle.StartHeartbeat(heartbeatCtx)
		}()

		// 5. Wait for partition assignment
		assignment, err := o.coordinator.WaitForPartitionAssignment(ctx, workerID)
		if err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to get partition assignment: %w", err)
		}

		// 6. Mark self as ready
		if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateReady); err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to update state to ready: %w", err)
		}

		// 7. Wait for all workers to be ready
		err = o.coordinator.WaitForAllReady(ctx, generation.ID)
		if err != nil {
			cancelHeartbeat()
			return fmt.Errorf("coordination failed: %w", err)
		}

		// 8. Mark self as running
		if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateRunning); err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to update state to running: %w", err)
		}

		// 9. Create execution context that can be cancelled on generation change
		execCtx, cancelExec := context.WithCancel(ctx)

		// 10. Watch for generation changes in background
		watchDone := make(chan error, 1)
		go func() {
			watchDone <- o.coordinator.WatchGeneration(execCtx, generation.ID)
		}()

		// 11. Run projections
		execDone := make(chan error, 1)
		go func() {
			execDone <- o.executor.Run(execCtx, projections, assignment)
		}()

		// 12. Wait for either execution to complete or generation to change
		var runErr error
		select {
		case runErr = <-execDone:
			// Execution finished (error or context cancelled)
			cancelExec()
		case watchErr := <-watchDone:
			// Generation changed
			cancelExec()
			if errors.Is(watchErr, orchestrator.ErrGenerationSuperseded) {
				// Log and continue to next iteration
				if o.config.Logger != nil {
					o.config.Logger.Info(ctx, "generation superseded, reconfiguring",
						"old_generation", generation.ID)
				}
				<-execDone // Wait for executor to stop
				cancelHeartbeat()
				if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateStopped); err != nil {
					if o.config.Logger != nil {
						o.config.Logger.Error(ctx, "failed to update state to stopped", "error", err)
					}
				}
				continue // Loop back to coordinate with new generation
			}
			runErr = watchErr
		case <-ctx.Done():
			cancelExec()
			cancelHeartbeat()
			return ctx.Err()
		}

		// 13. Cleanup
		cancelHeartbeat()
		if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateStopped); err != nil {
			if o.config.Logger != nil {
				o.config.Logger.Error(ctx, "failed to update state to stopped", "error", err)
			}
		}

		// 14. If parent context cancelled, exit
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// 15. If execution error (not from context cancellation), return it
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			return runErr
		}

		// Otherwise, loop back (this handles the generation superseded case)
	}
}
