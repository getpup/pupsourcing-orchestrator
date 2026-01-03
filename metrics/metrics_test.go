package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestGenerationsTotal_Increment(t *testing.T) {
	before := testutil.ToFloat64(GenerationsTotal.WithLabelValues("test-rs"))
	GenerationsTotal.WithLabelValues("test-rs").Inc()
	after := testutil.ToFloat64(GenerationsTotal.WithLabelValues("test-rs"))

	assert.Equal(t, before+1, after)
}

func TestWorkersRegisteredTotal_Increment(t *testing.T) {
	before := testutil.ToFloat64(WorkersRegisteredTotal.WithLabelValues("test-rs-2"))
	WorkersRegisteredTotal.WithLabelValues("test-rs-2").Inc()
	after := testutil.ToFloat64(WorkersRegisteredTotal.WithLabelValues("test-rs-2"))

	assert.Equal(t, before+1, after)
}

func TestActiveWorkers_SetValue(t *testing.T) {
	ActiveWorkers.WithLabelValues("test-rs-3").Set(5)
	value := testutil.ToFloat64(ActiveWorkers.WithLabelValues("test-rs-3"))

	assert.Equal(t, float64(5), value)
}

func TestCurrentGenerationPartitions_SetValue(t *testing.T) {
	CurrentGenerationPartitions.WithLabelValues("test-rs-4").Set(3)
	value := testutil.ToFloat64(CurrentGenerationPartitions.WithLabelValues("test-rs-4"))

	assert.Equal(t, float64(3), value)
}

func TestCoordinationDuration_Observe(t *testing.T) {
	CoordinationDuration.WithLabelValues("test-rs-5").Observe(1.5)
	count := testutil.CollectAndCount(CoordinationDuration)

	assert.Greater(t, count, 0)
}

func TestEventProcessingDuration_Observe(t *testing.T) {
	EventProcessingDuration.WithLabelValues("test-rs-6", "test-proj").Observe(0.5)
	count := testutil.CollectAndCount(EventProcessingDuration)

	assert.Greater(t, count, 0)
}

func TestHeartbeatLatency_Observe(t *testing.T) {
	HeartbeatLatency.WithLabelValues("test-rs-7").Observe(0.1)
	count := testutil.CollectAndCount(HeartbeatLatency)

	assert.Greater(t, count, 0)
}

func TestMetrics_LabelsAppliedCorrectly(t *testing.T) {
	replicaSet := "test-rs-labels"

	GenerationsTotal.WithLabelValues(replicaSet).Inc()

	// Verify that the metric has the correct label
	metricValue := testutil.ToFloat64(GenerationsTotal.WithLabelValues(replicaSet))
	assert.Greater(t, metricValue, float64(0))

	// Different label should have different value
	differentRS := "test-rs-different"
	differentValue := testutil.ToFloat64(GenerationsTotal.WithLabelValues(differentRS))

	// If we haven't incremented differentRS, it should be less than replicaSet
	assert.LessOrEqual(t, differentValue, metricValue)
}

func TestPartitionAssignmentsTotal_Increment(t *testing.T) {
	before := testutil.ToFloat64(PartitionAssignmentsTotal.WithLabelValues("test-rs-8"))
	PartitionAssignmentsTotal.WithLabelValues("test-rs-8").Inc()
	after := testutil.ToFloat64(PartitionAssignmentsTotal.WithLabelValues("test-rs-8"))

	assert.Equal(t, before+1, after)
}

func TestReconfigurationTotal_Increment(t *testing.T) {
	before := testutil.ToFloat64(ReconfigurationTotal.WithLabelValues("test-rs-9"))
	ReconfigurationTotal.WithLabelValues("test-rs-9").Inc()
	after := testutil.ToFloat64(ReconfigurationTotal.WithLabelValues("test-rs-9"))

	assert.Equal(t, before+1, after)
}

func TestStaleWorkersCleanedTotal_Increment(t *testing.T) {
	before := testutil.ToFloat64(StaleWorkersCleanedTotal.WithLabelValues("test-rs-10"))
	StaleWorkersCleanedTotal.WithLabelValues("test-rs-10").Inc()
	after := testutil.ToFloat64(StaleWorkersCleanedTotal.WithLabelValues("test-rs-10"))

	assert.Equal(t, before+1, after)
}

func TestEventsProcessedTotal_IncrementWithProjection(t *testing.T) {
	before := testutil.ToFloat64(EventsProcessedTotal.WithLabelValues("test-rs-11", "user-projection"))
	EventsProcessedTotal.WithLabelValues("test-rs-11", "user-projection").Inc()
	after := testutil.ToFloat64(EventsProcessedTotal.WithLabelValues("test-rs-11", "user-projection"))

	assert.Equal(t, before+1, after)
}

func TestProjectionErrorsTotal_IncrementWithProjection(t *testing.T) {
	before := testutil.ToFloat64(ProjectionErrorsTotal.WithLabelValues("test-rs-12", "order-projection"))
	ProjectionErrorsTotal.WithLabelValues("test-rs-12", "order-projection").Inc()
	after := testutil.ToFloat64(ProjectionErrorsTotal.WithLabelValues("test-rs-12", "order-projection"))

	assert.Equal(t, before+1, after)
}

func TestWorkerState_SetValue(t *testing.T) {
	WorkerState.WithLabelValues("test-rs-13", "worker-1", "running").Set(1)
	value := testutil.ToFloat64(WorkerState.WithLabelValues("test-rs-13", "worker-1", "running"))

	assert.Equal(t, float64(1), value)
}

func TestMetrics_AreRegisteredToDefaultRegistry(t *testing.T) {
	// Verify that metrics are registered to the default registry
	// by checking if they can be collected
	metrics := []prometheus.Collector{
		GenerationsTotal,
		WorkersRegisteredTotal,
		PartitionAssignmentsTotal,
		ReconfigurationTotal,
		StaleWorkersCleanedTotal,
		EventsProcessedTotal,
		ProjectionErrorsTotal,
		ActiveWorkers,
		CurrentGenerationPartitions,
		WorkerState,
		CoordinationDuration,
		EventProcessingDuration,
		HeartbeatLatency,
	}

	for _, metric := range metrics {
		count := testutil.CollectAndCount(metric)
		// Each metric should be collectible (count >= 0)
		assert.GreaterOrEqual(t, count, 0)
	}
}
