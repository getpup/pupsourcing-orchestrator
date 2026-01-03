# Kubernetes Deployment Example

This example demonstrates how to deploy projection workers using Kubernetes with the pupsourcing-orchestrator.

## Files

- `deployment.yaml` - Kubernetes Deployment manifest for projection workers
- `hpa.yaml` - Horizontal Pod Autoscaler configuration

## Deployment

The deployment configuration includes:

- **Replicas**: Set to 3 by default. The orchestrator automatically handles partition redistribution when scaling.
- **Termination Grace Period**: 60 seconds to allow workers to gracefully shut down during reconfiguration.
- **Prometheus Annotations**: Enables automatic metrics scraping by Prometheus.
- **Liveness Probe**: Uses the `/metrics` endpoint to verify worker health.
- **Resource Limits**: Configurable CPU and memory limits.

### Deploy the application

```bash
# Create the deployment
kubectl apply -f deployment.yaml

# Verify pods are running
kubectl get pods -l app=projection-worker
```

### Scaling

Scale manually:
```bash
kubectl scale deployment projection-worker --replicas=5
```

The orchestrator will automatically:
1. Detect the new worker
2. Pause all existing workers
3. Create a new generation with 5 partitions
4. Assign each worker a partition (0-4)
5. Resume processing with new partition assignments

## Horizontal Pod Autoscaler (HPA)

The HPA configuration enables automatic scaling based on CPU utilization.

### Important Considerations

**The orchestrator uses the Recreate strategy**, which means:
- When a new worker joins, all workers pause and reconfigure
- This causes temporary processing interruption
- Frequent scaling events can cause thrashing

**Recommendations:**
- Use HPA with conservative scaling thresholds
- Set longer `stabilizationWindowSeconds` to avoid rapid scale up/down
- Consider using custom metrics based on event processing lag instead of CPU/memory
- For most workloads, manually setting replica count is recommended over HPA

### Deploy HPA

```bash
# Create the HPA
kubectl apply -f hpa.yaml

# Check HPA status
kubectl get hpa projection-worker-hpa
```

### Tuning HPA for Orchestrator

The provided `hpa.yaml` includes conservative scaling policies:

```yaml
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

These settings prevent rapid scaling that would cause frequent reconfigurations.

## Monitoring

The deployment exposes metrics on port 9090 at the `/metrics` endpoint. Prometheus can automatically discover and scrape these metrics using the pod annotations.

Key metrics to monitor:
- `pupsourcing_orchestrator_active_workers` - Current number of active workers
- `pupsourcing_orchestrator_reconfiguration_total` - Rate of reconfigurations
- `pupsourcing_orchestrator_coordination_duration_seconds` - Time spent coordinating
- `pupsourcing_orchestrator_events_processed_total` - Event processing throughput

## Secrets

The deployment references a secret named `postgres-credentials`:

```bash
kubectl create secret generic postgres-credentials \
  --from-literal=url='postgres://user:password@host:5432/database?sslmode=require'
```

## Complete Example with HPA

For a production deployment with HPA, you might want to adjust the scaling behavior:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: projection-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: projection-worker
  minReplicas: 2
  maxReplicas: 10
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 300  # Wait 5 minutes
      policies:
      - type: Pods
        value: 1
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 600  # Wait 10 minutes
      policies:
      - type: Pods
        value: 1
        periodSeconds: 180
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

This configuration ensures that the HPA waits 5 minutes before scaling up and 10 minutes before scaling down, and only adds/removes one pod at a time. This minimizes the impact of the Recreate strategy on event processing.
