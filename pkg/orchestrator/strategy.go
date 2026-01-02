package orchestrator

import "context"

// Strategy defines how projections are deployed, managed, and scaled.
// Different strategies provide different tradeoffs between availability,
// consistency, and deployment complexity.
type Strategy interface {
	// Run executes the strategy for the given projections.
	// It blocks until the context is canceled or an error occurs.
	Run(ctx context.Context, projections []Projection) error
}
