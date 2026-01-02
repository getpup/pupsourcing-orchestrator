// Package orchestrator provides the public API for managing projection orchestration.
// It handles running, supervising, and scaling projections in an event-sourced system.
package orchestrator

import (
	"context"
	"fmt"

	"github.com/getpup/pupsourcing/es/projection"
)

// Projection is an alias for the projection interface from pupsourcing.
// This allows users to work with pupsourcing projections directly.
type Projection = projection.Projection

// Orchestrator coordinates multiple projection workers safely and deterministically.
// It manages lifecycle concerns such as startup, shutdown, restarts, and fault recovery.
type Orchestrator struct {
	strategy    Strategy
	projections []Projection
}

// New creates a new Orchestrator with the given options.
// At minimum, users must provide a strategy and projections.
func New(opts ...Option) (*Orchestrator, error) {
	o := &Orchestrator{}

	// Apply all options
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Validate required configuration
	if o.strategy == nil {
		return nil, fmt.Errorf("strategy is required (use WithStrategy)")
	}
	if len(o.projections) == 0 {
		return nil, fmt.Errorf("at least one projection is required (use WithProjections)")
	}

	return o, nil
}

// Run starts the orchestrator and blocks until the context is canceled.
// It coordinates the execution of all configured projections according to the strategy.
func (o *Orchestrator) Run(ctx context.Context) error {
	if err := o.strategy.Run(ctx, o.projections); err != nil {
		return fmt.Errorf("strategy execution failed: %w", err)
	}
	return nil
}

// Option is a functional option for configuring the Orchestrator.
type Option func(*Orchestrator) error

// WithStrategy sets the orchestration strategy.
// The strategy determines how projections are deployed and managed.
func WithStrategy(strategy Strategy) Option {
	return func(o *Orchestrator) error {
		if strategy == nil {
			return fmt.Errorf("strategy cannot be nil")
		}
		o.strategy = strategy
		return nil
	}
}

// WithProjections adds projections to be orchestrated.
// All provided projections will be managed by the orchestrator.
func WithProjections(projections ...Projection) Option {
	return func(o *Orchestrator) error {
		if len(projections) == 0 {
			return fmt.Errorf("at least one projection is required")
		}
		o.projections = append(o.projections, projections...)
		return nil
	}
}
