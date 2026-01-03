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
	orch, err := orchestrator.New(db, eventStore, "main-projections")
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

The orchestrator is configured by passing required parameters and optional configuration to `orchestrator.New()`.

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `db` | `*sql.DB` | Database connection for generation state and processors |
| `eventStore` | `*postgres.Store` | Event store for reading events |
| `replicaSet` | `ReplicaSetName` | Name of the replica set this orchestrator manages |

### Optional Configuration (with defaults)

| Option | Default | Description |
|--------|---------|-------------|
| `WithHeartbeatInterval(interval)` | `5s` | Interval between heartbeats |
| `WithStaleWorkerTimeout(timeout)` | `30s` | Duration after which a worker is considered dead |
| `WithCoordinationTimeout(timeout)` | `60s` | Max time to wait for coordination |
| `WithPollInterval(interval)` | `1s` | How often to check state during coordination |
| `WithBatchSize(size)` | `100` | Number of events to read per batch |
| `WithRegistrationWaitTime(duration)` | `5s` | Time to wait for workers to register before assigning partitions |
| `WithLogger(logger)` | `nil` | Logger for observability |
| `WithMetricsEnabled(enabled)` | `true` | Enable Prometheus metrics |
| `WithTableNames(genTable, workersTable)` | See below | Custom table names for generation store |
| `WithGenerationStore(store)` | PostgreSQL | Custom generation store implementation |
| `WithExecutor(executor)` | Default | Custom executor for running projections |

### Custom Table Names

By default, the orchestrator uses these table names:
- `orchestrator_generations` for generation metadata
- `orchestrator_workers` for worker metadata

You can customize these names:

```go
orch, err := orchestrator.New(
    db,
    eventStore,
    "main-projections",
    orchestrator.WithTableNames("my_generations", "my_workers"),
)
```

When using custom table names, you must also use them for migrations:

```go
tableConfig := postgres.TableConfig{
    GenerationsTable: "my_generations",
    WorkersTable:     "my_workers",
}
if err := orchestrator.RunMigrationsWithTableNames(db, tableConfig); err != nil {
    log.Fatal(err)
}
```

### Tuning for Different Scenarios

**High-Throughput Systems** (processing many events per second):
```go
orch, err := orchestrator.New(
	db,
	eventStore,
	"main-projections",
	orchestrator.WithBatchSize(1000),  // Larger batches for efficiency
	orchestrator.WithHeartbeatInterval(3 * time.Second),
	orchestrator.WithStaleWorkerTimeout(15 * time.Second),
)
```

**Low-Latency Systems** (minimizing detection time for worker failures):
```go
orch, err := orchestrator.New(
	db,
	eventStore,
	"main-projections",
	orchestrator.WithHeartbeatInterval(2 * time.Second),
	orchestrator.WithStaleWorkerTimeout(10 * time.Second),
)
```

**Stable Production Systems** (fewer workers, less churn):
```go
orch, err := orchestrator.New(
	db,
	eventStore,
	"main-projections",
	orchestrator.WithHeartbeatInterval(10 * time.Second),
	orchestrator.WithStaleWorkerTimeout(60 * time.Second),
	orchestrator.WithCoordinationTimeout(120 * time.Second),
)
```

## Multiple Replica Sets

You can run multiple independent replica sets in the same application. Each replica set scales independently:

```go
// Create orchestrators for different replica sets
mainOrch, _ := orchestrator.New(db, eventStore, "main-projections")

analyticsOrch, _ := orchestrator.New(db, eventStore, "analytics-projections")

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

## Metrics

The orchestrator exposes Prometheus metrics prefixed with `pupsourcing_orchestrator_`. These metrics are automatically registered to the Prometheus default registry when metrics are enabled (default).

### Integration with Existing Metrics

If your application already exposes a `/metrics` endpoint using the Prometheus default registry, orchestrator metrics are automatically included - no additional configuration needed.

For a complete example showing how to integrate orchestrator metrics with custom metrics and health endpoints, see [examples/custom-metrics](examples/custom-metrics/).

Example with existing metrics endpoint:

```go
import (
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/getpup/pupsourcing-orchestrator/recreate"
)

func main() {
	// Your existing metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":9090", nil)

	// Create orchestrator - metrics automatically appear at /metrics
	orch := recreate.New(recreate.Config{
		// ... config fields ...
		// MetricsEnabled defaults to true
	})

	orch.Run(ctx, projections)
}
```

### Standalone Metrics Server

If you need a dedicated metrics endpoint, use the optional metrics server:

```go
import "github.com/getpup/pupsourcing-orchestrator/metrics"

// Start metrics server on port 9090
metricsServer := metrics.NewServer(":9090")
metricsServer.Start()
defer metricsServer.Shutdown(ctx)
```

### Disabling Metrics

To disable metrics collection:

```go
metricsEnabled := false
orch := recreate.New(recreate.Config{
	// ... other fields ...
	MetricsEnabled: &metricsEnabled,
})
```

### Available Metrics

#### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `pupsourcing_orchestrator_generations_total` | `replica_set` | Total number of generations created |
| `pupsourcing_orchestrator_workers_registered_total` | `replica_set` | Total workers registered |
| `pupsourcing_orchestrator_partition_assignments_total` | `replica_set` | Total partition assignments |
| `pupsourcing_orchestrator_reconfiguration_total` | `replica_set` | Total reconfigurations triggered |
| `pupsourcing_orchestrator_stale_workers_cleaned_total` | `replica_set` | Total stale workers cleaned up |
| `pupsourcing_orchestrator_events_processed_total` | `replica_set`, `projection` | Total events processed |
| `pupsourcing_orchestrator_projection_errors_total` | `replica_set`, `projection` | Total projection errors |

#### Gauges

| Metric | Labels | Description |
|--------|--------|-------------|
| `pupsourcing_orchestrator_active_workers` | `replica_set` | Current active workers |
| `pupsourcing_orchestrator_current_generation_partitions` | `replica_set` | Current generation partition count |
| `pupsourcing_orchestrator_worker_state` | `replica_set`, `worker_id`, `state` | Worker state (1 for current state, 0 otherwise) |

#### Histograms

| Metric | Labels | Description |
|--------|--------|-------------|
| `pupsourcing_orchestrator_coordination_duration_seconds` | `replica_set` | Time spent in coordination phase |
| `pupsourcing_orchestrator_event_processing_duration_seconds` | `replica_set`, `projection` | Event processing latency |
| `pupsourcing_orchestrator_heartbeat_latency_seconds` | `replica_set` | Heartbeat round-trip latency |

### Example Queries

Monitor orchestrator health with these Prometheus queries:

```promql
# Rate of reconfigurations (should be low in stable systems)
rate(pupsourcing_orchestrator_reconfiguration_total[5m])

# Current number of active workers per replica set
pupsourcing_orchestrator_active_workers

# Average coordination duration
rate(pupsourcing_orchestrator_coordination_duration_seconds_sum[5m]) / 
rate(pupsourcing_orchestrator_coordination_duration_seconds_count[5m])

# Event processing rate per projection
rate(pupsourcing_orchestrator_events_processed_total[5m])

# Projection error rate
rate(pupsourcing_orchestrator_projection_errors_total[5m])
```

For a pre-configured Grafana dashboard with these and other queries, see [examples/grafana](examples/grafana/).

## Production Considerations

### Monitoring

The orchestrator provides comprehensive Prometheus metrics (see [Metrics](#metrics) section for details). Monitor these key indicators for healthy operation:

- **Reconfiguration rate** (`pupsourcing_orchestrator_reconfiguration_total`): Should be low in stable systems
- **Active workers** (`pupsourcing_orchestrator_active_workers`): Should match your deployment replica count
- **Coordination duration** (`pupsourcing_orchestrator_coordination_duration_seconds`): Time workers spend coordinating during reconfiguration
- **Event processing rate** (`pupsourcing_orchestrator_events_processed_total`): Throughput per projection
- **Projection errors** (`pupsourcing_orchestrator_projection_errors_total`): Should be rare or zero

Set up alerts for:
- High reconfiguration rate (indicates worker instability)
- Worker count mismatch (actual vs expected)
- High coordination duration (slow synchronization)
- High error rate (projection failures)

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

orch, _ := orchestrator.New(
	db,
	eventStore,
	"main-projections",
	orchestrator.WithLogger(&myLogger{}),
)
```

### Kubernetes Deployment

For production-ready Kubernetes deployment manifests, see [examples/kubernetes](examples/kubernetes/).

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
orch, _ := orchestrator.New(db, eventStore, "main-projections")

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

