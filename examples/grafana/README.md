# Grafana Dashboard for Pupsourcing Orchestrator

This directory contains a pre-configured Grafana dashboard for monitoring pupsourcing-orchestrator metrics.

## Dashboard Overview

The dashboard provides comprehensive visibility into orchestrator operations:

### Panels

1. **Active Workers by Replica Set** - Shows the current number of active workers per replica set
2. **Generation Creation Rate** - Rate of new generation creation over time (indicates worker join/leave events)
3. **Coordination Duration** - Heatmap showing distribution of coordination times
4. **Reconfiguration Rate** - Rate of reconfigurations triggered
5. **Workers Running** - Table showing which workers are currently running
6. **Event Processing Rate** - Events processed per second by projection
7. **Projection Error Rate** - Projection errors per second

## Installation

### Import Dashboard

1. Open Grafana UI
2. Navigate to **Dashboards** → **Import**
3. Upload `dashboard.json` or paste its contents
4. Select your Prometheus data source
5. Click **Import**

### Using Dashboard UID

The dashboard has a UID of `pupsourcing-orchestrator` which can be used for:
- Linking to the dashboard from other dashboards
- Provisioning the dashboard via Grafana configuration

## Data Source

The dashboard expects a Prometheus data source with metrics from pupsourcing-orchestrator.

### Required Metrics

All metrics are prefixed with `pupsourcing_orchestrator_`:

- `pupsourcing_orchestrator_active_workers`
- `pupsourcing_orchestrator_generations_total`
- `pupsourcing_orchestrator_coordination_duration_seconds_bucket`
- `pupsourcing_orchestrator_reconfiguration_total`
- `pupsourcing_orchestrator_worker_state`
- `pupsourcing_orchestrator_events_processed_total`
- `pupsourcing_orchestrator_projection_errors_total`

### Prometheus Configuration

Ensure Prometheus is scraping your projection workers. Example scrape config:

```yaml
scrape_configs:
  - job_name: 'projection-workers'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
```

## Dashboard Customization

### Variables

You can add template variables to filter by replica set:

1. Edit dashboard
2. Click **Settings** (gear icon)
3. Go to **Variables**
4. Add new variable:
   - **Name**: `replica_set`
   - **Type**: Query
   - **Query**: `label_values(pupsourcing_orchestrator_active_workers, replica_set)`
5. Update panel queries to use `replica_set="$replica_set"`

### Alerts

Set up alerts based on dashboard metrics:

#### High Reconfiguration Rate
```promql
rate(pupsourcing_orchestrator_reconfiguration_total[5m]) > 0.1
```
Indicates worker instability (more than 6 reconfigurations per minute).

#### Worker Count Mismatch
```promql
abs(pupsourcing_orchestrator_active_workers - <expected_count>) > 0
```
Indicates workers are not all running.

#### High Coordination Duration
```promql
histogram_quantile(0.95, rate(pupsourcing_orchestrator_coordination_duration_seconds_bucket[5m])) > 30
```
Indicates slow coordination (P95 > 30 seconds).

#### Projection Errors
```promql
rate(pupsourcing_orchestrator_projection_errors_total[5m]) > 0
```
Indicates projection failures.

## Provisioning

To automatically provision the dashboard in Grafana, add it to your provisioning configuration:

### Dashboard Provisioning Config

Create `/etc/grafana/provisioning/dashboards/orchestrator.yaml`:

```yaml
apiVersion: 1

providers:
  - name: 'Pupsourcing Orchestrator'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards/orchestrator
```

Copy `dashboard.json` to `/var/lib/grafana/dashboards/orchestrator/`.

### Kubernetes ConfigMap

For Kubernetes deployments:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orchestrator-dashboard
  namespace: monitoring
data:
  dashboard.json: |
    # Paste dashboard.json contents here
```

Mount in Grafana deployment:

```yaml
volumeMounts:
  - name: orchestrator-dashboard
    mountPath: /var/lib/grafana/dashboards/orchestrator
volumes:
  - name: orchestrator-dashboard
    configMap:
      name: orchestrator-dashboard
```

## Recommended Alerts

Based on the dashboard panels, here are recommended alert rules:

```yaml
groups:
  - name: orchestrator
    interval: 30s
    rules:
      - alert: OrchestratorHighReconfigurationRate
        expr: rate(pupsourcing_orchestrator_reconfiguration_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High reconfiguration rate for replica set {{ $labels.replica_set }}"
          description: "Replica set {{ $labels.replica_set }} is reconfiguring frequently ({{ $value }} times/sec)"

      - alert: OrchestratorWorkerMismatch
        expr: pupsourcing_orchestrator_active_workers != 3  # Change 3 to your expected replica count
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Worker count mismatch for {{ $labels.replica_set }}"
          description: "Expected 3 workers but found {{ $value }}"  # Update expected count to match expr

      - alert: OrchestratorSlowCoordination
        expr: histogram_quantile(0.95, rate(pupsourcing_orchestrator_coordination_duration_seconds_bucket[5m])) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Slow coordination for {{ $labels.replica_set }}"
          description: "P95 coordination duration is {{ $value }}s"

      - alert: OrchestratorProjectionErrors
        expr: rate(pupsourcing_orchestrator_projection_errors_total[5m]) > 0.01
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Projection errors in {{ $labels.replica_set }}"
          description: "Projection {{ $labels.projection }} is experiencing errors ({{ $value }} errors/sec)"
```

## Troubleshooting

### No Data in Dashboard

1. Verify Prometheus is scraping your application:
   ```promql
   up{job="projection-workers"}
   ```

2. Check if metrics are being exposed:
   ```bash
   curl http://<worker-pod>:9090/metrics | grep pupsourcing_orchestrator
   ```

3. Verify data source connection in Grafana

### Missing Panels

If some panels show "No data":
- Check that the metrics exist in Prometheus
- Verify the time range is appropriate
- Check for typos in metric names

### Performance Issues

If the dashboard loads slowly:
- Reduce the time range
- Increase the refresh interval
- Add variables to filter data
- Use recording rules for complex queries

## Example Recording Rules

To improve dashboard performance, create Prometheus recording rules:

```yaml
groups:
  - name: orchestrator_aggregations
    interval: 30s
    rules:
      - record: job:orchestrator_events_rate:5m
        expr: rate(pupsourcing_orchestrator_events_processed_total[5m])
      
      - record: job:orchestrator_errors_rate:5m
        expr: rate(pupsourcing_orchestrator_projection_errors_total[5m])
      
      - record: job:orchestrator_coordination_p95:5m
        expr: histogram_quantile(0.95, rate(pupsourcing_orchestrator_coordination_duration_seconds_bucket[5m]))
```

Update dashboard queries to use these pre-aggregated metrics for faster loading.
