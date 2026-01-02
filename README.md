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
- Worker identity and lifecycle management
- Automatic cleanup of stale workers
- Heartbeat-based health monitoring

**Usage:**

```go
// Basic usage without worker management
strategy := orchestrator.Recreate()

// With worker lifecycle management
persistence := &MyWorkerPersistence{} // Implement WorkerPersistenceAdapter

workerConfig := orchestrator.WorkerConfig{
    HeartbeatInterval:  5 * time.Second,
    PersistenceAdapter: persistence,
}

strategy := &orchestrator.RecreateStrategy{
    WorkerConfig:         &workerConfig,
    StaleWorkerThreshold: 30 * time.Second,
}

orch, err := orchestrator.New(
    orchestrator.WithStrategy(strategy),
    orchestrator.WithProjections(p1, p2, p3),
)
```

**Worker Lifecycle:**

When worker management is enabled, each orchestrator instance:
1. Generates a stable worker ID (hostname + UUID)
2. Registers itself in the `orchestrator.workers` table
3. Sends periodic heartbeats to indicate it's alive
4. Observes the current recreate generation
5. Transitions through well-defined states:
   - `starting` - Worker is initializing
   - `ready` - Worker is connected and idle
   - `running` - Worker is processing projections
   - `draining` - Worker is finishing current batch before shutdown
   - `stopped` - Worker has stopped

**Crash Recovery:**

Workers may disappear without cleanup (e.g., process crash, network partition).
The system recovers via:
- Stale worker detection based on last heartbeat timestamp
- Automatic cleanup during Recreate strategy initialization
- Default stale threshold: 30 seconds (configurable)

**WorkerPersistenceAdapter Interface:**

To enable worker management, implement the `WorkerPersistenceAdapter` interface:

```go
type WorkerPersistenceAdapter interface {
    // RegisterWorker inserts or updates a worker record
    RegisterWorker(ctx context.Context, workerID string, generation int64, state WorkerState) error

    // UpdateWorkerHeartbeat updates the last_heartbeat timestamp
    UpdateWorkerHeartbeat(ctx context.Context, workerID string, state WorkerState) error

    // UpdateWorkerState updates the worker's state
    UpdateWorkerState(ctx context.Context, workerID string, state WorkerState, generation int64) error

    // GetCurrentGeneration retrieves the current recreate generation
    GetCurrentGeneration(ctx context.Context) (int64, error)

    // DeleteStaleWorkers removes workers with expired heartbeats
    DeleteStaleWorkers(ctx context.Context, staleThreshold time.Duration) error
}
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

## Database Schema Migration

The orchestrator requires database tables for coordination. Use the `migrate-gen` command to generate SQL migration files for your database.

### Generate Migrations

```bash
# Generate PostgreSQL migration
go run github.com/getpup/pupsourcing-orchestrator/cmd/migrate-gen \
  -adapter postgres \
  -output migrations

# Generate MySQL/MariaDB migration
go run github.com/getpup/pupsourcing-orchestrator/cmd/migrate-gen \
  -adapter mysql \
  -output migrations

# Generate SQLite migration
go run github.com/getpup/pupsourcing-orchestrator/cmd/migrate-gen \
  -adapter sqlite \
  -output migrations
```

Or use `go generate`:

```go
//go:generate go run github.com/getpup/pupsourcing-orchestrator/cmd/migrate-gen -adapter postgres -output migrations
```

Then run:

```bash
go generate ./...
```

### Migration Options

- `-adapter`: Database adapter (postgres, mysql, sqlite)
- `-output`: Output folder for migration files (default: migrations)
- `-filename`: Custom filename for the migration (default: timestamp-based)
- `-schema`: Schema name for PostgreSQL or database name for MySQL (default: orchestrator)
- `-projection-shards-table`: Custom name for projection shards table (default: projection_shards)
- `-recreate-lock-table`: Custom name for recreate lock table (default: recreate_lock)
- `-workers-table`: Custom name for workers table (default: workers)

### Generated Tables

The migration creates three coordination tables:

1. **projection_shards** - Manages shard ownership and coordination for projections
   - Ensures each shard is owned by at most one worker
   - Tracks global position for each shard
   - Supports horizontal scaling by partitioning workload

2. **recreate_lock** - Coordinates recreate strategy deployments
   - Singleton table tracking deployment generation and phase
   - Prevents parallel deployments using transactional operations
   - No advisory locks required

3. **workers** - Tracks worker heartbeats and status
   - Enables worker discovery and health monitoring
   - Detects worker failures through stale heartbeats
   - Manages worker lifecycle states

### Schema Requirements

- **PostgreSQL**: Creates a schema (default: `orchestrator`) and tables within it
- **MySQL/MariaDB**: Creates a database (default: `orchestrator`) and tables within it
- **SQLite**: Uses table name prefixes (e.g., `orchestrator_projection_shards`)

All operations use standard transactional SQL without requiring:
- Advisory locks
- SERIALIZABLE isolation level
- Database-specific extensions

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

