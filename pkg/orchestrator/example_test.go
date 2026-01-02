package orchestrator_test

import (
	"context"
	"log"
	"time"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
	"github.com/getpup/pupsourcing/es"
)

// ExampleProjection is a simple projection implementation for demonstration
type ExampleProjection struct {
	name string
}

func (p *ExampleProjection) Name() string {
	return p.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *ExampleProjection) Handle(_ context.Context, _ es.PersistedEvent) error {
	return nil
}

// Example_basic demonstrates the basic usage of the orchestrator
func Example_basic() {
	// Create some projections
	p1 := &ExampleProjection{name: "user_read_model"}
	p2 := &ExampleProjection{name: "order_analytics"}
	p3 := &ExampleProjection{name: "inventory_tracker"}

	// Create orchestrator with Recreate strategy and projections
	orch, err := orchestrator.New(
		orchestrator.WithStrategy(orchestrator.Recreate()),
		orchestrator.WithProjections(p1, p2, p3),
	)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Create context with timeout for this example
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Run the orchestrator (blocks until context is canceled)
	if err := orch.Run(ctx); err != nil && err != context.DeadlineExceeded {
		log.Printf("Orchestrator error: %v", err)
	}

	// Output will be logged but not captured in example output
}

// Example_withStrategy demonstrates creating a strategy
func Example_withStrategy() {
	// Create projection
	p := &ExampleProjection{name: "my_projection"}

	// Create strategy (currently no configuration options)
	strategy := orchestrator.Recreate()

	// Create orchestrator
	orch, err := orchestrator.New(
		orchestrator.WithStrategy(strategy),
		orchestrator.WithProjections(p),
	)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := orch.Run(ctx); err != nil && err != context.DeadlineExceeded {
		log.Printf("Orchestrator error: %v", err)
	}
}
