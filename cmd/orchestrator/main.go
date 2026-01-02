package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/pkg/version"
)

// DemoProjection is a simple demonstration projection
type DemoProjection struct {
	name string
}

func (p *DemoProjection) Name() string {
	return p.name
}

func main() {
	log.Printf("Starting pupsourcing-orchestrator v%s", version.Version)

	// Create some example projections
	// In a real application, these would be actual projection implementations
	// that integrate with pupsourcing's projection.Projection interface
	p1 := &DemoProjection{name: "user_read_model"}
	p2 := &DemoProjection{name: "order_analytics"}
	p3 := &DemoProjection{name: "inventory_tracker"}

	// Create orchestrator with the Recreate strategy
	orch, err := orchestrator.New(
		orchestrator.WithStrategy(orchestrator.Recreate()),
		orchestrator.WithProjections(p1, p2, p3),
	)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nReceived shutdown signal, stopping orchestrator...")
		cancel()
	}()

	// Run the orchestrator
	log.Println("Press Ctrl+C to stop")
	if err := orch.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Orchestrator error: %v", err)
	}

	fmt.Println("Orchestrator completed successfully")
}
