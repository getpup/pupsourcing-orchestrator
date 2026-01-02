// Package main demonstrates basic usage of pupsourcing-orchestrator.
//
// This example shows:
// 1. Creating scoped projections that filter by aggregate type and bounded context
// 2. Creating a global projection that receives all events
// 3. Configuring the orchestrator with the Recreate strategy
// 4. Running projections with graceful shutdown
//
// Note: This is a demonstration example with mock projections.
// In production, projections would:
// - Connect to actual databases or message brokers
// - Process events from a real event store
// - Handle errors and implement retry logic
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
	"github.com/getpup/pupsourcing/es"
)

// UserReadModelProjection is a scoped projection that only processes User events
// from the Identity bounded context. This builds a read model for user data.
type UserReadModelProjection struct {
	userCount int
}

func (p *UserReadModelProjection) Name() string {
	return "user_read_model"
}

// AggregateTypes filters events to only User aggregate
func (p *UserReadModelProjection) AggregateTypes() []string {
	return []string{"User"}
}

// BoundedContexts filters events to only Identity context
func (p *UserReadModelProjection) BoundedContexts() []string {
	return []string{"Identity"}
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *UserReadModelProjection) Handle(_ context.Context, event es.PersistedEvent) error {
	// In a real implementation, this would:
	// - Parse event payload
	// - Update read model in database
	// - Handle errors appropriately
	p.userCount++
	log.Printf("[UserReadModel] Processed User event: %s (total: %d)", event.EventType, p.userCount)
	return nil
}

// OrderAnalyticsProjection is a scoped projection that only processes Order events
// from the Sales bounded context. This maintains analytics and reporting data.
type OrderAnalyticsProjection struct {
	orderCount int
}

func (p *OrderAnalyticsProjection) Name() string {
	return "order_analytics"
}

// AggregateTypes filters events to only Order aggregate
func (p *OrderAnalyticsProjection) AggregateTypes() []string {
	return []string{"Order"}
}

// BoundedContexts filters events to only Sales context
func (p *OrderAnalyticsProjection) BoundedContexts() []string {
	return []string{"Sales"}
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *OrderAnalyticsProjection) Handle(_ context.Context, event es.PersistedEvent) error {
	// In a real implementation, this would:
	// - Calculate revenue metrics
	// - Update analytics tables
	// - Generate reports
	p.orderCount++
	log.Printf("[OrderAnalytics] Processed Order event: %s (total: %d)", event.EventType, p.orderCount)
	return nil
}

// IntegrationPublisher is a global projection that receives ALL events from
// all bounded contexts and aggregate types. This is useful for publishing
// events to external systems like message brokers (Kafka, RabbitMQ, etc.).
type IntegrationPublisher struct {
	publishedCount int
}

func (p *IntegrationPublisher) Name() string {
	// Convention: system.integration.{service}.v{version}
	return "system.integration.publisher.v1"
}

// No AggregateTypes or BoundedContexts methods = receives ALL events

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (p *IntegrationPublisher) Handle(_ context.Context, event es.PersistedEvent) error {
	// In a real implementation, this would:
	// - Marshal event to wire format
	// - Publish to message broker
	// - Handle delivery confirmations
	p.publishedCount++
	log.Printf("[IntegrationPublisher] Published event: %s/%s (total: %d)",
		event.BoundedContext, event.EventType, p.publishedCount)
	return nil
}

func main() {
	log.Printf("Starting pupsourcing-orchestrator example v%s", version.Version)

	// Create projections
	// Note: In a real application, these would be initialized with
	// database connections, message broker clients, etc.
	p1 := &UserReadModelProjection{}
	p2 := &OrderAnalyticsProjection{}
	p3 := &IntegrationPublisher{}

	log.Println("Projections:")
	log.Println("  - UserReadModel: SCOPED to User events in Identity context")
	log.Println("  - OrderAnalytics: SCOPED to Order events in Sales context")
	log.Println("  - IntegrationPublisher: GLOBAL, receives ALL events")

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
