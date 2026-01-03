package metrics

// Collector wraps metrics and provides helper methods with pre-filled labels.
type Collector struct {
	replicaSet string
}

// NewCollector creates a new Collector for the given replica set.
func NewCollector(replicaSet string) *Collector {
	return &Collector{replicaSet: replicaSet}
}

// IncGenerations increments the generations counter.
func (c *Collector) IncGenerations() {
	GenerationsTotal.WithLabelValues(c.replicaSet).Inc()
}

// IncWorkersRegistered increments the workers registered counter.
func (c *Collector) IncWorkersRegistered() {
	WorkersRegisteredTotal.WithLabelValues(c.replicaSet).Inc()
}

// IncPartitionAssignments increments the partition assignments counter.
func (c *Collector) IncPartitionAssignments() {
	PartitionAssignmentsTotal.WithLabelValues(c.replicaSet).Inc()
}

// IncReconfiguration increments the reconfiguration counter.
func (c *Collector) IncReconfiguration() {
	ReconfigurationTotal.WithLabelValues(c.replicaSet).Inc()
}

// IncStaleWorkersCleaned increments the stale workers cleaned counter.
func (c *Collector) IncStaleWorkersCleaned() {
	StaleWorkersCleanedTotal.WithLabelValues(c.replicaSet).Inc()
}

// IncEventsProcessed increments the events processed counter for a projection.
func (c *Collector) IncEventsProcessed(projection string) {
	EventsProcessedTotal.WithLabelValues(c.replicaSet, projection).Inc()
}

// IncProjectionErrors increments the projection errors counter for a projection.
func (c *Collector) IncProjectionErrors(projection string) {
	ProjectionErrorsTotal.WithLabelValues(c.replicaSet, projection).Inc()
}

// SetActiveWorkers sets the active workers gauge.
func (c *Collector) SetActiveWorkers(count int) {
	ActiveWorkers.WithLabelValues(c.replicaSet).Set(float64(count))
}

// SetCurrentGenerationPartitions sets the current generation partitions gauge.
func (c *Collector) SetCurrentGenerationPartitions(count int) {
	CurrentGenerationPartitions.WithLabelValues(c.replicaSet).Set(float64(count))
}

// SetWorkerState sets the worker state gauge. Sets value to 1 for the given state, 0 for others.
func (c *Collector) SetWorkerState(workerID string, state string) {
	states := []string{"pending", "ready", "running", "stopping", "stopped"}
	for _, s := range states {
		if s == state {
			WorkerState.WithLabelValues(c.replicaSet, workerID, s).Set(1)
		} else {
			WorkerState.WithLabelValues(c.replicaSet, workerID, s).Set(0)
		}
	}
}

// ObserveCoordinationDuration records a coordination duration observation.
func (c *Collector) ObserveCoordinationDuration(seconds float64) {
	CoordinationDuration.WithLabelValues(c.replicaSet).Observe(seconds)
}

// ObserveEventProcessingDuration records an event processing duration observation.
func (c *Collector) ObserveEventProcessingDuration(projection string, seconds float64) {
	EventProcessingDuration.WithLabelValues(c.replicaSet, projection).Observe(seconds)
}

// ObserveHeartbeatLatency records a heartbeat latency observation.
func (c *Collector) ObserveHeartbeatLatency(seconds float64) {
	HeartbeatLatency.WithLabelValues(c.replicaSet).Observe(seconds)
}
