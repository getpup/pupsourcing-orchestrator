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
	"github.com/getpup/pupsourcing-orchestrator/metrics"
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

	// Executor is an optional custom executor for running projections.
	// If nil, a default executor is created using DB and EventStore.
	Executor executor.Runner

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

	// RegistrationWaitTime is how long to wait for other workers to register
	// before the leader assigns partitions (default: 5s).
	RegistrationWaitTime time.Duration

	// Logger is for observability (optional).
	Logger es.Logger

	// MetricsEnabled enables Prometheus metrics collection (default: true).
	// Set to false explicitly to disable metrics.
	MetricsEnabled *bool
}

// Orchestrator coordinates the execution of projections across multiple workers.
type Orchestrator struct {
	config      Config
	lifecycle   *lifecycle.Manager
	coordinator *coordinator.Coordinator
	executor    executor.Runner
	collector   *metrics.Collector
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
	if cfg.RegistrationWaitTime == 0 {
		cfg.RegistrationWaitTime = 5 * time.Second
	}

	// Create metrics collector if enabled (default: true)
	var collector *metrics.Collector
	metricsEnabled := true
	if cfg.MetricsEnabled != nil {
		metricsEnabled = *cfg.MetricsEnabled
	}
	if metricsEnabled {
		collector = metrics.NewCollector(string(cfg.ReplicaSet))
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

	// Create or use provided executor
	var exec executor.Runner
	if cfg.Executor != nil {
		exec = cfg.Executor
	} else {
		exec = executor.New(executor.Config{
			DB:         cfg.DB,
			EventStore: cfg.EventStore,
			BatchSize:  cfg.BatchSize,
			Logger:     cfg.Logger,
		})
	}

	return &Orchestrator{
		config:      cfg,
		lifecycle:   lifecycleManager,
		coordinator: coord,
		executor:    exec,
		collector:   collector,
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

		// 2. Get or create generation (without triggering reconfiguration yet)
		generation, err := o.coordinator.JoinOrCreate(ctx)
		if err != nil {
			return fmt.Errorf("failed to join generation: %w", err)
		}

		// 3. Register this worker in the generation
		workerID, err := o.lifecycle.Register(ctx, o.config.ReplicaSet, generation.ID)
		if err != nil {
			return fmt.Errorf("failed to register worker: %w", err)
		}
		if o.collector != nil {
			o.collector.IncWorkersRegistered()
		}

		// 4. Check if this generation is now oversubscribed (more workers than partitions)
		shouldReconfig, err := o.coordinator.ShouldTriggerReconfigurationForGeneration(ctx, generation.ID)
		if err != nil {
			return fmt.Errorf("failed to check reconfiguration: %w", err)
		}

		if shouldReconfig {
			// Wait for RegistrationWaitTime to allow other workers starting simultaneously
			// to register before we count and trigger reconfiguration. This ensures accurate
			// partition counts when multiple workers start at the same time.
			time.Sleep(o.config.RegistrationWaitTime)
			
			// Trigger reconfiguration - this creates a new generation
			newGen, err := o.coordinator.TriggerReconfiguration(ctx)
			if err != nil {
				return fmt.Errorf("failed to trigger reconfiguration: %w", err)
			}
			
			if o.config.Logger != nil {
				o.config.Logger.Info(ctx, "reconfiguration triggered after registration",
					"oldGenerationID", generation.ID,
					"newGenerationID", newGen.ID)
			}
			
			// Mark self as stopped in old generation and loop back to join new one
			if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateStopped); err != nil {
				if o.config.Logger != nil {
					o.config.Logger.Error(ctx, "failed to update state to stopped", "error", err)
				}
			}
			continue
		}

		// 5. Start heartbeat in background
		heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
		heartbeatDone := make(chan error, 1)
		go func() {
			heartbeatDone <- o.lifecycle.StartHeartbeat(heartbeatCtx)
		}()

		// 6. Check if this worker is the leader
		isLeader, err := o.coordinator.IsLeader(ctx, generation.ID, workerID)
		if err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to check leader status: %w", err)
		}

		// 7. If leader, wait for expected workers then assign partitions
		if isLeader {
			// Wait for expected number of workers to register
			if err := o.coordinator.WaitForExpectedWorkers(ctx, generation.ID); err != nil {
				cancelHeartbeat()
				return fmt.Errorf("failed waiting for expected workers: %w", err)
			}

			// Assign partitions
			_, err = o.coordinator.AssignPartitionsIfLeader(ctx, generation.ID, workerID)
			if err != nil {
				cancelHeartbeat()
				return fmt.Errorf("failed to assign partitions: %w", err)
			}
			if o.collector != nil {
				o.collector.IncPartitionAssignments()
			}
		}

		// 8. Wait for partition assignment
		assignment, err := o.coordinator.WaitForPartitionAssignment(ctx, workerID)
		if err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to get partition assignment: %w", err)
		}

		// 9. Mark self as ready
		if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateReady); err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to update state to ready: %w", err)
		}

		// Track start of coordination
		coordinationStart := time.Now()

		// 10. Wait for all workers to be ready
		err = o.coordinator.WaitForAllReady(ctx, generation.ID)
		if err != nil {
			cancelHeartbeat()
			return fmt.Errorf("coordination failed: %w", err)
		}

		// Track coordination duration
		if o.collector != nil {
			o.collector.ObserveCoordinationDuration(time.Since(coordinationStart).Seconds())
			o.collector.SetCurrentGenerationPartitions(generation.TotalPartitions)
		}

		// 11. Mark self as running
		if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateRunning); err != nil {
			cancelHeartbeat()
			return fmt.Errorf("failed to update state to running: %w", err)
		}

		// 12. Create execution context that can be cancelled on generation change
		execCtx, cancelExec := context.WithCancel(ctx)

		// 13. Watch for generation changes in background
		watchDone := make(chan error, 1)
		go func() {
			watchDone <- o.coordinator.WatchGeneration(execCtx, generation.ID)
		}()

		// 14. Run projections
		execDone := make(chan error, 1)
		go func() {
			execDone <- o.executor.Run(execCtx, projections, assignment)
		}()

		// 15. Wait for either execution to complete or generation to change
		var runErr error
		select {
		case runErr = <-execDone:
			// Execution finished (error or context cancelled)
			cancelExec()
		case watchErr := <-watchDone:
			// Generation changed
			cancelExec()
			if errors.Is(watchErr, orchestrator.ErrGenerationSuperseded) {
				// Track reconfiguration
				if o.collector != nil {
					o.collector.IncReconfiguration()
				}
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

		// 15. Cleanup
		cancelHeartbeat()
		if err := o.lifecycle.UpdateState(ctx, orchestrator.WorkerStateStopped); err != nil {
			if o.config.Logger != nil {
				o.config.Logger.Error(ctx, "failed to update state to stopped", "error", err)
			}
		}

		// 16. If parent context cancelled, exit
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// 17. If execution error (not from context cancellation), return it
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			return runErr
		}

		// Otherwise, loop back (this handles the generation superseded case)
	}
}
