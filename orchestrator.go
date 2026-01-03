package orchestrator

import (
	"context"

	"github.com/getpup/pupsourcing/es/projection"
)

// Orchestrator coordinates the execution of projections across multiple workers.
// It handles worker registration, partition assignment, and lifecycle management
// using the Recreate strategy: when workers join or leave, all workers pause,
// reconfigure partition assignments, and restart together.
type Orchestrator interface {
	// Run starts the orchestrator for the given projections.
	// It blocks until ctx is cancelled or a fatal error occurs.
	//
	// The orchestrator will:
	// 1. Register this worker with the replica set
	// 2. Coordinate with other workers to determine partition assignments
	// 3. Create processors with the assigned partition configuration
	// 4. Run projections until a generation change or context cancellation
	// 5. On generation change: stop, reconfigure, and restart
	//
	// Run returns an error if:
	// - Worker registration fails
	// - Coordination fails or times out
	// - A fatal error occurs during projection processing
	//
	// Run returns nil when ctx is cancelled and graceful shutdown completes.
	Run(ctx context.Context, projections []projection.Projection) error
}
