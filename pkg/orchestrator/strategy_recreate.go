package orchestrator

import (
	"context"
	"time"

	"github.com/getpup/pupsourcing/es"
)

// RecreatePersistenceAdapter defines the interface for persisting recreate state to the database.
// It manages the generation-based coordination ensuring only one generation runs at a time.
type RecreatePersistenceAdapter interface {
	// GetCurrentGeneration retrieves the current generation number
	GetCurrentGeneration(ctx context.Context) (int64, error)

	// AdvanceGeneration attempts to increment the generation using CAS-style update.
	// Returns the new generation number on success.
	// If expectedCurrent doesn't match the current generation, returns an error.
	AdvanceGeneration(ctx context.Context, expectedCurrent int64) (int64, error)
}

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
// - Operates on generations - only one generation active at a time
//
// This is the default and initially only supported strategy.
type RecreateStrategy struct {
	// WorkerConfig configures worker behavior (optional)
	// If not provided, worker management is disabled
	WorkerConfig WorkerConfig

	// RecreatePersistence handles generation coordination (optional)
	// If not provided, generation advancement is skipped
	RecreatePersistence RecreatePersistenceAdapter

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
	var currentGeneration int64

	// Initialize worker if persistence adapter is configured
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

		currentGeneration = worker.GenerationSeen()

		if s.Logger != nil {
			s.Logger.Info(ctx, "Worker registered", "worker_id", worker.ID(), "generation", currentGeneration)
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

	// Attempt to advance generation if recreate persistence is configured
	if s.RecreatePersistence != nil {
		// Observe current generation
		observedGen, err := s.RecreatePersistence.GetCurrentGeneration(ctx)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Error(ctx, "Failed to observe generation", "error", err)
			}
			return err
		}

		if s.Logger != nil {
			s.Logger.Info(ctx, "Observed current generation", "generation", observedGen)
		}

		// Attempt to advance generation (CAS-style)
		newGeneration, err := s.RecreatePersistence.AdvanceGeneration(ctx, observedGen)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Error(ctx, "Failed to advance generation", "expected", observedGen, "error", err)
			}
			// Another worker won the race - this is OK, we just exit
			return err
		}

		currentGeneration = newGeneration

		if s.Logger != nil {
			s.Logger.Info(ctx, "Advanced to new generation", "generation", newGeneration)
		}

		// Update worker's observed generation if worker is active
		if worker != nil {
			worker.mu.Lock()
			worker.generationSeen = newGeneration
			worker.mu.Unlock()
		}
	}

	// Transition to ready state if worker is active
	if worker != nil {
		if err := worker.TransitionTo(ctx, WorkerStateReady); err != nil {
			return err
		}
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
			s.Logger.Info(ctx, "Projection registered", "projection_name", proj.Name(), "generation", currentGeneration)
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
