# pupsourcing-orchestrator

Scalable orchestrator for projection processes

## Overview

pupsourcing-orchestrator is a companion library to [pupsourcing](https://github.com/getpup/pupsourcing) that is responsible for running, supervising, and scaling projections in an event-sourced system.

Its core responsibility is to coordinate multiple projection workers safely and deterministically, using a shared database for coordination. It manages lifecycle concerns such as startup, shutdown, restarts, and fault recovery, while ensuring each event is processed exactly once per projection.

The orchestrator enables horizontal scaling without requiring users to manually partition streams or assign shards. Multiple identical workers can be started, and the orchestrator ensures only one worker owns a given projection at a time.

pupsourcing-orchestrator is intentionally not a framework. It does not own domain logic, storage schemas, or event definitions. Instead, it focuses purely on orchestration, coordination, and operational correctness for projection execution.

## Features

- **Strategy-based orchestration** - Pluggable strategies for different deployment patterns
- **Recreate strategy** - Simple, predictable deployment with guaranteed consistency
- **Clean API** - Simple functional options pattern for configuration
- **Lifecycle management** - Graceful startup and shutdown handling
- **No vendor lock-in** - Works with any projection that implements the `Projection` interface

## Prerequisites

- Go 1.24 or later

## Getting Started

### Installation

```bash
go get github.com/getpup/pupsourcing-orchestrator
```

### Basic Usage

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/getpup/pupsourcing/es"
    "github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
)

// Define your projections (compatible with pupsourcing projections)
type MyProjection struct {}

func main() {
    // Create scoped projections that filter by aggregate type and bounded context
    p1 := &UserReadModelProjection{}    // Scoped to User events in Identity context
    p2 := &OrderAnalyticsProjection{}   // Scoped to Order events in Sales context  
    p3 := &IntegrationPublisher{}       // Global projection - receives all events

    // Create orchestrator with Recreate strategy
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
        log.Println("Shutting down...")
        cancel()
    }()

    // Run the orchestrator
    if err := orch.Run(ctx); err != nil && err != context.Canceled {
        log.Fatalf("Orchestrator error: %v", err)
    }
}

// UserReadModelProjection is a scoped projection that only processes User events
type UserReadModelProjection struct{}

func (p *UserReadModelProjection) Name() string {
    return "user_read_model"
}

func (p *UserReadModelProjection) AggregateTypes() []string {
    return []string{"User"}
}

func (p *UserReadModelProjection) BoundedContexts() []string {
    return []string{"Identity"}
}

func (p *UserReadModelProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
    // Build read model from User events
    return nil
}

// OrderAnalyticsProjection is a scoped projection for order analytics
type OrderAnalyticsProjection struct{}

func (p *OrderAnalyticsProjection) Name() string {
    return "order_analytics"
}

func (p *OrderAnalyticsProjection) AggregateTypes() []string {
    return []string{"Order"}
}

func (p *OrderAnalyticsProjection) BoundedContexts() []string {
    return []string{"Sales"}
}

func (p *OrderAnalyticsProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
    // Analyze order data
    return nil
}

// IntegrationPublisher is a global projection that publishes all events
type IntegrationPublisher struct{}

func (p *IntegrationPublisher) Name() string {
    return "system.integration.publisher.v1"
}

// No AggregateTypes or BoundedContexts methods = receives ALL events

func (p *IntegrationPublisher) Handle(ctx context.Context, event es.PersistedEvent) error {
    // Publish to message broker
    return nil
}
```

## API Reference

### Orchestrator

The main orchestrator type that coordinates projection execution.

```go
type Orchestrator struct {
    // internal fields
}
```

#### Creating an Orchestrator

```go
orch, err := orchestrator.New(opts ...Option)
```

Options:
- `WithStrategy(strategy Strategy)` - Sets the orchestration strategy (required)
- `WithProjections(projections ...Projection)` - Adds projections to orchestrate (required)

### Strategies

#### Recreate Strategy

The Recreate strategy is a simple orchestration approach where all projections are stopped before starting new ones. This is similar to Kubernetes Recreate deployment.

**Characteristics:**
- Simple and predictable
- Guaranteed consistency (no parallel execution of same projection)
- Downtime during updates (all instances stopped before new ones start)

**Usage:**

```go
// Use the convenience function
strategy := orchestrator.Recreate()

// Or create with custom logger
strategy := &orchestrator.RecreateStrategy{
    Logger: myCustomLogger,
}

orch, err := orchestrator.New(
    orchestrator.WithStrategy(strategy),
    orchestrator.WithProjections(p1, p2, p3),
)
```

### Projection Interface

Projections must implement the following interface:

```go
type Projection interface {
    // Name returns the unique name of this projection
    Name() string
}
```

This interface is compatible with `github.com/getpup/pupsourcing/es/projection.Projection`.

## Project Structure

```
.
├── cmd/                    # Main applications
│   └── orchestrator/       # Orchestrator application entry point
├── internal/               # Private application code
│   └── orchestrator/       # Orchestrator business logic (deprecated, use pkg/)
├── pkg/                    # Public library code
│   ├── orchestrator/       # Public orchestrator API
│   └── version/            # Version information
├── Makefile                # Build and CI automation
├── .golangci.yml           # Linter configuration
└── go.mod                  # Go module definition
```

## Design Principles

### No Partition/Shard Configuration

The orchestrator does not expose partition or shard configuration to users. This complexity is handled internally by the coordination layer. Users simply provide projections, and the orchestrator ensures they run correctly.

### No Event-Store-Specific Logic

The public API contains no event-store-specific logic. It works with any projection that implements the simple `Projection` interface, making it compatible with various event sourcing libraries and custom implementations.

### Strategy Pattern

Different orchestration strategies provide different tradeoffs:
- **Recreate**: Simple, predictable, with downtime during updates
- **Rolling** (future): Zero-downtime updates with gradual rollout

### Clean Architecture

The orchestrator follows clean architecture principles:
- Domain logic is separate from infrastructure concerns
- Dependencies point inward (infrastructure depends on domain, not vice versa)
- Public API is minimal and focused

## Build

```bash
make build
```

The binary will be built to `bin/orchestrator`.

### Run

```bash
./bin/orchestrator
```

### Development

#### Install Development Tools

```bash
make install-tools
```

This installs:
- golangci-lint (linter)

#### Format Code

```bash
make fmt
```

#### Run Linter

```bash
make lint
```

#### Run Go Vet

```bash
make vet
```

#### Run Tests

```bash
make test
```

#### Run All CI Checks

```bash
make ci
```

This runs: format, vet, lint, and test.

#### Clean Build Artifacts

```bash
make clean
```

## Makefile Targets

Run `make help` to see all available targets:

```bash
make help
```

