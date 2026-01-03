# pupsourcing-orchestrator

[![Go Version](https://img.shields.io/github/go-mod/go-version/getpup/pupsourcing-orchestrator)](https://go.dev/)
[![License](https://img.shields.io/github/license/getpup/pupsourcing-orchestrator)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/getpup/pupsourcing-orchestrator/ci.yml?branch=main)](https://github.com/getpup/pupsourcing-orchestrator/actions)

Scalable orchestration for projection processes in event-sourced systems.

## Overview

**pupsourcing-orchestrator** is a companion library to [pupsourcing](https://github.com/getpup/pupsourcing) that handles operational coordination for running projections in production. It enables horizontal scaling of projection processing across multiple workers while ensuring correctness through coordinated partition assignment.

### Key Features

- **Recreate Strategy**: When workers join or leave, all workers pause, reconfigure partition assignments, and restart together, ensuring consistent event processing
- **Automatic Partitioning**: Workers coordinate to automatically divide the event stream into partitions based on the number of active workers
- **Heartbeat Monitoring**: Workers continuously report health, and stale workers are automatically detected and removed
- **PostgreSQL-based Coordination**: Uses PostgreSQL for generation tracking and worker coordination, with no additional infrastructure dependencies
- **Multiple Replica Sets**: Run independent projection groups that scale independently

## Installation

```bash
go get github.com/getpup/pupsourcing-orchestrator
```

## Quick Start

Here's a minimal example to get started:

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/adapters/postgres"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
)

type UserProjection struct{}

func (p *UserProjection) Name() string {
	return "user_projection"
}

func (p *UserProjection) Handle(ctx context.Context, event es.PersistedEvent) error {
	log.Printf("Processing event: %s at position %d", event.EventType, event.GlobalPosition)
	return nil
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Run migrations (once during deployment)
	if err := orchestrator.RunMigrations(db); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	// Create event store
	eventStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Create orchestrator
	orch, err := orchestrator.New(orchestrator.Config{
		DB:         db,
		EventStore: eventStore,
		ReplicaSet: "main-projections",
	})
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()

	// Run projections
	projections := []projection.Projection{&UserProjection{}}
	if err := orch.Run(ctx, projections); err != nil && err != context.Canceled {
		log.Fatalf("Orchestrator error: %v", err)
	}
}
```

See the [examples/](examples/) directory for more complete examples.

## Concepts

### Replica Set

A **Replica Set** is a named group of projections that scale together. Each replica set:
- Has a unique name (e.g., `"main-projections"`, `"analytics-projections"`)
- Contains one or more projections
- Scales independently of other replica sets
- Coordinates workers via a shared generation store

You can run multiple replica sets in the same application to scale different projection groups independently.

### Generation

A **Generation** represents a specific partition configuration. When workers join or leave:
1. A new generation is created with updated `TotalPartitions`
2. Old generation workers stop processing
3. New generation workers start with assigned `PartitionKey` values
4. All workers process events with their assigned partition

### Worker

A **Worker** is an orchestrator instance running a replica set. Workers:
- Register with the generation store
- Receive a `PartitionKey` assignment (0-indexed)
- Heartbeat to prove liveness
- Stop when their generation is superseded

### Recreate Strategy

The orchestrator uses a **Recreate** strategy for handling worker changes:

```
Initial: No workers

Worker A starts:
  → A registers, becomes partition 0 of 1
  → A processes all events (partitioned by key % 1)

Worker B starts:
  → B registers and signals "pending"
  → Coordinator detects change, creates new generation (2 partitions)
  → A receives "generation superseded" signal, stops
  → A and B coordinate: A gets partition 0, B gets partition 1
  → Both start with TotalPartitions=2
  → A processes events where (key % 2 == 0)
  → B processes events where (key % 2 == 1)

Worker B crashes:
  → B's heartbeat stops
  → After timeout, coordinator marks B as dead
  → New generation created with 1 partition
  → A stops, reconfigures to partition 0 of 1, restarts
```

This approach ensures:
- **Correctness**: Events are never processed by multiple workers simultaneously
- **Consistency**: Partition assignments are always coordinated
- **Simplicity**: No complex rebalancing or state migration

## Configuration

The `orchestrator.Config` struct supports the following fields:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `DB` | `*sql.DB` | Yes | - | Database connection for generation state and processors |
| `EventStore` | `*postgres.Store` | Yes | - | Event store for reading events |
| `ReplicaSet` | `ReplicaSetName` | Yes | - | Name of the replica set this orchestrator manages |
| `HeartbeatInterval` | `time.Duration` | No | `5s` | Interval between heartbeats |
| `StaleWorkerTimeout` | `time.Duration` | No | `30s` | Duration after which a worker is considered dead |
| `CoordinationTimeout` | `time.Duration` | No | `60s` | Max time to wait for coordination |
| `BatchSize` | `int` | No | `100` | Number of events to read per batch |
| `Logger` | `es.Logger` | No | `nil` | Logger for observability |

### Tuning for Different Scenarios

**High-Throughput Systems** (processing many events per second):
```go
orchestrator.Config{
	// ... required fields ...
	BatchSize:           1000,  // Larger batches for efficiency
	HeartbeatInterval:   3 * time.Second,
	StaleWorkerTimeout:  15 * time.Second,
}
```

**Low-Latency Systems** (minimizing detection time for worker failures):
```go
orchestrator.Config{
	// ... required fields ...
	HeartbeatInterval:   2 * time.Second,
	StaleWorkerTimeout:  10 * time.Second,
}
```

**Stable Production Systems** (fewer workers, less churn):
```go
orchestrator.Config{
	// ... required fields ...
	HeartbeatInterval:   10 * time.Second,
	StaleWorkerTimeout:  60 * time.Second,
	CoordinationTimeout: 120 * time.Second,
}
```

## Multiple Replica Sets

You can run multiple independent replica sets in the same application. Each replica set scales independently:

```go
// Create orchestrators for different replica sets
mainOrch, _ := orchestrator.New(orchestrator.Config{
	DB:         db,
	EventStore: eventStore,
	ReplicaSet: "main-projections",
})

analyticsOrch, _ := orchestrator.New(orchestrator.Config{
	DB:         db,
	EventStore: eventStore,
	ReplicaSet: "analytics-projections",
})

// Run them concurrently
var wg sync.WaitGroup
wg.Add(2)

go func() {
	defer wg.Done()
	mainOrch.Run(ctx, mainProjections)
}()

go func() {
	defer wg.Done()
	analyticsOrch.Run(ctx, analyticsProjections)
}()

wg.Wait()
```

**Use cases for multiple replica sets:**
- Scale critical projections independently from analytics
- Different SLAs for different projection groups
- Isolate failures between projection groups

See [examples/multiple-replica-sets](examples/multiple-replica-sets/) for a complete example.

## Production Considerations

### Monitoring

Monitor these metrics for healthy operation:

- **Generation changes per hour**: Should be low in stable systems
- **Worker count per replica set**: Should match your deployment replica count
- **Heartbeat failures**: Should be rare
- **Coordination timeout errors**: Indicates issues with worker synchronization
- **Event processing lag**: Distance between current position and latest event

### Logging

Configure structured logging for observability:

```go
import "github.com/getpup/pupsourcing/es"

type myLogger struct{}

func (l *myLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	// Your logging implementation
}

func (l *myLogger) Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	// Your logging implementation
}

orch, _ := orchestrator.New(orchestrator.Config{
	// ... other fields ...
	Logger: &myLogger{},
})
```

### Kubernetes Deployment

For Kubernetes deployments:

1. **Use Deployments** with multiple replicas:
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: projections
   spec:
     replicas: 3
     template:
       spec:
         containers:
         - name: projections
           image: your-image:tag
   ```

2. **Configure graceful shutdown** with appropriate `terminationGracePeriodSeconds`:
   ```yaml
   spec:
     template:
       spec:
         terminationGracePeriodSeconds: 60
   ```

3. **Health checks** should verify database connectivity:
   ```yaml
   livenessProbe:
     exec:
       command:
       - /health
     initialDelaySeconds: 30
     periodSeconds: 10
   ```

### Horizontal Pod Autoscaler (HPA)

**Important**: The orchestrator uses the **Recreate** strategy, which means:
- When a new worker joins, all workers pause and reconfigure
- This causes temporary processing interruption
- Frequent scaling events can cause thrashing

**Recommendations:**
- Use HPA with **conservative scaling thresholds**
- Set longer `stabilizationWindowSeconds` to avoid rapid scale up/down
- Consider using custom metrics based on event processing lag instead of CPU/memory
- For most workloads, **manually setting replica count** is recommended over HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: projections-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: projections
  minReplicas: 2
  maxReplicas: 10
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 300  # Wait 5 minutes before scaling up
      policies:
      - type: Pods
        value: 1
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 600  # Wait 10 minutes before scaling down
      policies:
      - type: Pods
        value: 1
        periodSeconds: 180
```

## Migration from Manual Runners

If you're currently running projections manually, here's how to migrate:

### Before (Manual Runner)

```go
// Old approach: manually running a single projection processor
processor := projection.NewPostgresProcessor(projection.ProcessorConfig{
	DB:              db,
	EventStore:      eventStore,
	Projection:      &UserProjection{},
	BatchSize:       100,
	PartitionKey:    0,        // Hardcoded
	TotalPartitions: 3,        // Hardcoded
})

processor.Run(ctx)
```

**Issues:**
- Manual partition assignment
- No automatic scaling
- No coordination between workers
- No failure detection

### After (Orchestrator)

```go
// New approach: orchestrator handles everything
orch, _ := orchestrator.New(orchestrator.Config{
	DB:         db,
	EventStore: eventStore,
	ReplicaSet: "main-projections",
})

projections := []projection.Projection{
	&UserProjection{},
}

orch.Run(ctx, projections)
```

**Benefits:**
- Automatic partition assignment
- Horizontal scaling
- Coordinated worker management
- Automatic failure detection and recovery

### Migration Steps

1. **Run migrations**: Add orchestrator tables to your database
   ```go
   orchestrator.RunMigrations(db)
   ```

2. **Deploy orchestrator**: Replace manual runner with orchestrator

3. **Scale horizontally**: Add more replicas to your deployment

4. **Monitor**: Verify workers are coordinating correctly

## API Reference

For detailed API documentation, see:
- [pkg.go.dev documentation](https://pkg.go.dev/github.com/getpup/pupsourcing-orchestrator)
- [examples/](examples/) directory for working code examples

## Contributing

### Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/getpup/pupsourcing-orchestrator
   cd pupsourcing-orchestrator
   ```

2. Install development tools:
   ```bash
   make install-tools
   ```

### Running Tests

Run unit tests:
```bash
make test
```

Run integration tests (requires PostgreSQL):
```bash
make test-integration
```

Run all checks (format, vet, lint, test):
```bash
make ci
```

### Code Style

- Follow standard Go conventions
- Use `make fmt` to format code
- Use `make lint` to run linters
- Add tests for new functionality
- Update documentation for API changes

See [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) for architecture details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

