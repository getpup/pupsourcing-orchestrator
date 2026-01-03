# Custom Metrics Integration Example

This example demonstrates how to integrate pupsourcing-orchestrator metrics with your own custom Prometheus metrics.

## Overview

The orchestrator automatically registers metrics with the Prometheus default registry. This example shows how to:
- Expose orchestrator metrics alongside your own custom metrics
- Add a custom `/health` endpoint
- Run a custom HTTP server for metrics

## Running the Example

```bash
# Set database URL
export DATABASE_URL='postgres://user:password@localhost:5432/database?sslmode=disable'

# Run the example
go run main.go
```

## Endpoints

The example exposes the following HTTP endpoints:

- `http://localhost:8080/metrics` - Orchestrator and custom metrics (default Prometheus registry)
- `http://localhost:8080/custom-metrics` - Only custom application metrics
- `http://localhost:8080/health` - Health check endpoint

## Metrics Integration

### Default Registry (Recommended)

The orchestrator metrics are automatically registered with the Prometheus default registry using `promauto`. This is the simplest approach:

```go
import (
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/getpup/pupsourcing-orchestrator/pkg/orchestrator"
)

// Orchestrator metrics are automatically registered
orch, _ := orchestrator.New(db, eventStore, "main-projections")

// Serve all metrics (including orchestrator metrics)
http.Handle("/metrics", promhttp.Handler())
```

### Custom Metrics

Add your own application metrics:

```go
import "github.com/prometheus/client_golang/prometheus"

// Create custom metric
myCounter := prometheus.NewCounter(prometheus.CounterOpts{
    Name: "myapp_requests_total",
    Help: "Total requests",
})

// Register with default registry to appear at /metrics
prometheus.MustRegister(myCounter)

// Use it
myCounter.Inc()
```

### Health Check

The example includes a health check endpoint that verifies database connectivity:

```go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    if err := db.PingContext(r.Context()); err != nil {
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("Database unhealthy"))
        return
    }
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
})
```

## Orchestrator Metrics

All orchestrator metrics are prefixed with `pupsourcing_orchestrator_`:

### Counters
- `pupsourcing_orchestrator_generations_total` - Total generations created
- `pupsourcing_orchestrator_workers_registered_total` - Total workers registered
- `pupsourcing_orchestrator_partition_assignments_total` - Total partition assignments
- `pupsourcing_orchestrator_reconfiguration_total` - Total reconfigurations
- `pupsourcing_orchestrator_stale_workers_cleaned_total` - Total stale workers cleaned
- `pupsourcing_orchestrator_events_processed_total` - Total events processed
- `pupsourcing_orchestrator_projection_errors_total` - Total projection errors

### Gauges
- `pupsourcing_orchestrator_active_workers` - Current active workers
- `pupsourcing_orchestrator_current_generation_partitions` - Current generation partitions
- `pupsourcing_orchestrator_worker_state` - Worker state (1 for current state, 0 otherwise)

### Histograms
- `pupsourcing_orchestrator_coordination_duration_seconds` - Coordination duration
- `pupsourcing_orchestrator_event_processing_duration_seconds` - Event processing latency
- `pupsourcing_orchestrator_heartbeat_latency_seconds` - Heartbeat latency

## Kubernetes Integration

In Kubernetes, add Prometheus annotations to your pod spec:

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/metrics"
```

Prometheus will automatically discover and scrape the metrics endpoint.

## Prometheus Configuration

If you're running Prometheus standalone, configure it to scrape your application:

```yaml
scrape_configs:
  - job_name: 'projection-workers'
    static_configs:
      - targets: ['localhost:8080']
```

## Go Metrics

The example also includes standard Go runtime metrics:

```go
registry := prometheus.NewRegistry()
registry.MustRegister(prometheus.NewGoCollector())
registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
```

These provide insights into:
- Goroutine count
- Memory usage
- GC statistics
- CPU usage

## Production Considerations

1. **Use structured logging** with the `Logger` config option for better observability
2. **Monitor health endpoint** with Kubernetes liveness/readiness probes
3. **Set appropriate resource limits** based on your workload
4. **Use TLS** for metrics endpoints in production environments
5. **Add authentication** if exposing metrics publicly
