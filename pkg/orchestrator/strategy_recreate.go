package orchestrator

import (
	"context"
	"log"
)

// RecreateStrategy implements a simple orchestration strategy where all projections
// are stopped before starting new ones. This is similar to Kubernetes Recreate deployment.
//
// Characteristics:
// - Simple and predictable
// - Guaranteed consistency (no parallel execution of same projection)
// - Downtime during updates (all instances stopped before new ones start)
// - No rolling updates
//
// This is the default and initially only supported strategy.
type RecreateStrategy struct {
	// Note: Logger integration will be added in future iterations when
	// the orchestrator integrates with pupsourcing's ProcessorRunner
}

// Run executes all projections sequentially until the context is canceled.
// In the Recreate strategy, projections run in a simple sequential manner.
// When the context is canceled, all projections are stopped gracefully.
func (s *RecreateStrategy) Run(ctx context.Context, projections []Projection) error {
	log.Printf("Starting Recreate strategy with %d projection(s)", len(projections))

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
		log.Printf("Projection registered: %s", proj.Name())
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Printf("Context canceled, stopping Recreate strategy")

	return ctx.Err()
}

// Recreate is a convenience function that returns a new RecreateStrategy with default settings.
func Recreate() *RecreateStrategy {
	return &RecreateStrategy{}
}
