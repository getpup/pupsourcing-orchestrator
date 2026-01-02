package orchestrator

import (
	"context"
	"time"

	"github.com/getpup/pupsourcing/es"
)

// RecreateStrategy implements a simple orchestration strategy where all projections
// are stopped before starting new ones. This is similar to Kubernetes Recreate deployment.
//
// Characteristics:
// - Simple and predictable
// - Guaranteed consistency (no parallel execution of same projection)
// - Downtime during updates (all instances stopped before new ones start)
// - No rolling updates
// - Workers register themselves and send periodic heartbeats
// - Stale workers are cleaned up on strategy start
//
// This is the default and initially only supported strategy.
type RecreateStrategy struct {
	// WorkerConfig configures worker behavior (optional)
	// If not provided, worker management is disabled
	WorkerConfig WorkerConfig

	// StaleWorkerThreshold is the duration after which a worker is considered stale
	// Default is 30 seconds
	StaleWorkerThreshold time.Duration

	// Logger is an optional logger for observability
	// If nil, logging is disabled
	Logger es.Logger
}

// Run executes all projections sequentially until the context is canceled.
// In the Recreate strategy, projections run in a simple sequential manner.
// When the context is canceled, all projections are stopped gracefully.
func (s *RecreateStrategy) Run(ctx context.Context, projections []Projection) error {
	if s.Logger != nil {
		s.Logger.Info(ctx, "Starting Recreate strategy", "projection_count", len(projections))
	}

	var worker *Worker

	// Initialize worker if config is provided and has a valid adapter
	if s.WorkerConfig.PersistenceAdapter != nil {
		var err error
		worker, err = NewWorker(s.WorkerConfig)
		if err != nil {
			return err
		}

		// Start worker (registers and begins heartbeat)
		if err := worker.Start(ctx); err != nil {
			return err
		}

		if s.Logger != nil {
			s.Logger.Info(ctx, "Worker registered", "worker_id", worker.ID())
		}

		// Cleanup stale workers from previous deployments
		staleThreshold := s.StaleWorkerThreshold
		if staleThreshold == 0 {
			staleThreshold = DefaultStaleWorkerThreshold
		}

		if err := CleanupStaleWorkers(ctx, s.WorkerConfig.PersistenceAdapter, staleThreshold); err != nil {
			if s.Logger != nil {
				s.Logger.Error(ctx, "Failed to cleanup stale workers", "error", err)
			}
			// Don't fail the strategy if cleanup fails - log and continue
		}

		// Transition to ready state
		if err := worker.TransitionTo(ctx, WorkerStateReady); err != nil {
			return err
		}

		// Defer cleanup
		defer func() {
			if worker != nil {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), DefaultCleanupTimeout)
				defer cancel()
				if err := worker.Stop(cleanupCtx); err != nil {
					if s.Logger != nil {
						s.Logger.Error(cleanupCtx, "Failed to stop worker cleanly", "error", err)
					}
				}
			}
		}()
	}

	// For now, this is a placeholder implementation.
	// The actual projection execution will be implemented later when we integrate
	// with pupsourcing's projection.ProcessorRunner.
	//
	// The strategy's responsibility is to:
	// 1. Ensure only one instance of each projection runs at a time
	// 2. Manage graceful shutdown when context is canceled
	// 3. Handle errors and potentially restart projections
	//
	// Future implementation will coordinate with the database for distributed locking
	// and actually run the projections using the ProcessorRunner interface.

	for _, proj := range projections {
		if s.Logger != nil {
			s.Logger.Info(ctx, "Projection registered", "projection_name", proj.Name())
		}
	}

	// Transition to running state if worker is active
	if worker != nil {
		if err := worker.TransitionTo(ctx, WorkerStateRunning); err != nil {
			return err
		}
	}

	// Wait for context cancellation
	<-ctx.Done()

	// Transition to draining state if worker is active
	if worker != nil {
		drainingCtx, cancel := context.WithTimeout(context.Background(), DefaultCleanupTimeout)
		defer cancel()
		if err := worker.TransitionTo(drainingCtx, WorkerStateDraining); err != nil {
			if s.Logger != nil {
				s.Logger.Error(drainingCtx, "Failed to transition to draining state", "error", err)
			}
		}
	}

	if s.Logger != nil {
		s.Logger.Info(ctx, "Context canceled, stopping Recreate strategy")
	}

	return ctx.Err()
}

// Recreate is a convenience function that returns a new RecreateStrategy with default settings.
func Recreate() *RecreateStrategy {
	return &RecreateStrategy{}
}
