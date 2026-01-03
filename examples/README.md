# Pupsourcing Orchestrator Examples

This directory contains examples demonstrating various deployment and usage patterns for pupsourcing-orchestrator.

## Examples

### [Basic](./basic/)
Minimal example showing how to get started with the orchestrator. This is the best place to start if you're new to pupsourcing-orchestrator.

**Shows:**
- Basic orchestrator setup
- Database connection and migrations
- Signal handling for graceful shutdown
- Running a simple projection

### [Multiple Replica Sets](./multiple-replica-sets/)
Demonstrates running multiple independent replica sets in the same application for scaling different projection groups independently.

**Shows:**
- Creating multiple orchestrators
- Running replica sets concurrently
- Independent scaling per replica set
- Use cases for replica set separation

### [Kubernetes](./kubernetes/)
Production-ready Kubernetes deployment manifests for running projection workers at scale.

**Includes:**
- Deployment manifest with multiple replicas
- Horizontal Pod Autoscaler (HPA) configuration
- Prometheus metrics annotations
- Health check configuration
- Resource limits and requests

**Shows:**
- Kubernetes deployment patterns
- Graceful shutdown configuration
- Scaling considerations with the Recreate strategy
- Best practices for production deployments

### [Custom Metrics](./custom-metrics/)
Example of integrating orchestrator metrics with your own custom Prometheus metrics.

**Shows:**
- Exposing orchestrator metrics alongside custom metrics
- Custom HTTP server setup
- Health check endpoints
- Integration with existing metrics infrastructure

### [Grafana](./grafana/)
Pre-configured Grafana dashboard for monitoring orchestrator operations.

**Includes:**
- Complete dashboard JSON
- Dashboard provisioning configuration
- Recommended alert rules
- Prometheus recording rules

**Shows:**
- Key metrics to monitor
- Dashboard setup and customization
- Alert configuration
- Troubleshooting tips

## Quick Start

To run any example:

```bash
# Set required environment variable
export DATABASE_URL='postgres://user:password@localhost:5432/database?sslmode=disable'

# Navigate to the example directory
cd examples/basic

# Run the example
go run main.go
```

## Prerequisites

All examples require:
- **Go 1.21+** - For running the examples
- **PostgreSQL** - For storing event data and orchestrator state
- **pupsourcing** - Event sourcing library (automatically installed via go.mod)

### Database Setup

Create a PostgreSQL database:

```sql
CREATE DATABASE projections;
```

Set the connection string:

```bash
export DATABASE_URL='postgres://user:password@localhost:5432/projections?sslmode=disable'
```

The examples will automatically run migrations on startup.

## Common Patterns

### Configuration

All examples follow a similar configuration pattern:

```go
orch, err := orchestrator.New(orchestrator.Config{
    DB:                  db,              // Required: PostgreSQL connection
    EventStore:          eventStore,      // Required: Event store
    ReplicaSet:          "my-replica",    // Required: Replica set name
    HeartbeatInterval:   5 * time.Second, // Optional: Default 5s
    StaleWorkerTimeout:  30 * time.Second, // Optional: Default 30s
    CoordinationTimeout: 60 * time.Second, // Optional: Default 60s
    BatchSize:           100,              // Optional: Default 100
    Logger:              logger,           // Optional: For observability
})
```

### Graceful Shutdown

All examples implement graceful shutdown:

```go
ctx, cancel := context.WithCancel(context.Background())
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
go func() {
    <-sigCh
    cancel()
}()

orch.Run(ctx, projections)
```

### Error Handling

Examples demonstrate proper error handling:

```go
if err := orch.Run(ctx, projections); err != nil && err != context.Canceled {
    log.Fatalf("Orchestrator error: %v", err)
}
```

## Development

### Running Tests

```bash
# Unit tests
make test

# Integration tests (requires PostgreSQL)
make test-integration
```

### Linting

```bash
make lint
```

## Production Deployment

For production deployments, see:
- [Kubernetes example](./kubernetes/) for container orchestration
- [Custom Metrics example](./custom-metrics/) for observability
- [Grafana example](./grafana/) for monitoring dashboards

Key production considerations:
1. **Use appropriate timeouts** - Tune `HeartbeatInterval`, `StaleWorkerTimeout`, and `CoordinationTimeout` for your workload
2. **Monitor metrics** - Set up Prometheus scraping and Grafana dashboards
3. **Configure graceful shutdown** - Set `terminationGracePeriodSeconds` in Kubernetes
4. **Use conservative HPA settings** - Avoid rapid scaling due to Recreate strategy
5. **Set resource limits** - Configure appropriate CPU and memory limits

## Architecture

The orchestrator uses a **Recreate strategy** for handling worker changes:
1. When a worker joins or leaves, all workers pause
2. A new generation is created with updated partition count
3. Workers coordinate to assign partitions
4. All workers resume processing with new partitions

This ensures:
- **Correctness**: No events processed by multiple workers
- **Consistency**: Coordinated partition assignments
- **Simplicity**: No complex rebalancing logic

See the [main README](../README.md) for detailed architecture information.

## Troubleshooting

### Connection Errors

If you see database connection errors:
```
Failed to connect to database: ...
```

Verify your `DATABASE_URL` is correct and PostgreSQL is running:
```bash
psql $DATABASE_URL -c "SELECT 1"
```

### Migration Errors

If migrations fail:
```
Failed to run migrations: ...
```

The orchestrator requires specific database tables. Check PostgreSQL logs and ensure the database user has CREATE TABLE permissions.

### Worker Coordination Issues

If workers aren't coordinating:
- Check that all workers use the same `ReplicaSet` name
- Verify all workers can connect to the same database
- Check PostgreSQL logs for connection issues
- Increase `CoordinationTimeout` if coordination is slow

## Contributing

To add a new example:

1. Create a new directory: `examples/my-example/`
2. Add `main.go` with a complete, runnable example
3. Add `README.md` explaining what the example demonstrates
4. Update this file to include your example
5. Test that the example runs successfully

Examples should be:
- **Complete** - Runnable without modifications
- **Focused** - Demonstrate one concept clearly
- **Documented** - Include comments explaining key parts
- **Realistic** - Show practical, real-world usage

## Resources

- [Main Documentation](../README.md)
- [Development Plan](../DEVELOPMENT_PLAN.md)
- [pkg.go.dev Documentation](https://pkg.go.dev/github.com/getpup/pupsourcing-orchestrator)
- [Pupsourcing Core Library](https://github.com/getpup/pupsourcing)

## Support

For questions or issues:
- Open an issue on [GitHub](https://github.com/getpup/pupsourcing-orchestrator/issues)
- Check existing [discussions](https://github.com/getpup/pupsourcing-orchestrator/discussions)
