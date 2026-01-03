package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewCollector_CreatesCollectorWithReplicaSet(t *testing.T) {
	collector := NewCollector("test-replica-set")

	assert.NotNil(t, collector)
	assert.Equal(t, "test-replica-set", collector.replicaSet)
}

func TestCollector_IncGenerations(t *testing.T) {
	collector := NewCollector("test-rs-coll-1")

	before := testutil.ToFloat64(GenerationsTotal.WithLabelValues("test-rs-coll-1"))
	collector.IncGenerations()
	after := testutil.ToFloat64(GenerationsTotal.WithLabelValues("test-rs-coll-1"))

	assert.Equal(t, before+1, after)
}

func TestCollector_IncWorkersRegistered(t *testing.T) {
	collector := NewCollector("test-rs-coll-2")

	before := testutil.ToFloat64(WorkersRegisteredTotal.WithLabelValues("test-rs-coll-2"))
	collector.IncWorkersRegistered()
	after := testutil.ToFloat64(WorkersRegisteredTotal.WithLabelValues("test-rs-coll-2"))

	assert.Equal(t, before+1, after)
}

func TestCollector_IncPartitionAssignments(t *testing.T) {
	collector := NewCollector("test-rs-coll-3")

	before := testutil.ToFloat64(PartitionAssignmentsTotal.WithLabelValues("test-rs-coll-3"))
	collector.IncPartitionAssignments()
	after := testutil.ToFloat64(PartitionAssignmentsTotal.WithLabelValues("test-rs-coll-3"))

	assert.Equal(t, before+1, after)
}

func TestCollector_IncReconfiguration(t *testing.T) {
	collector := NewCollector("test-rs-coll-4")

	before := testutil.ToFloat64(ReconfigurationTotal.WithLabelValues("test-rs-coll-4"))
	collector.IncReconfiguration()
	after := testutil.ToFloat64(ReconfigurationTotal.WithLabelValues("test-rs-coll-4"))

	assert.Equal(t, before+1, after)
}

func TestCollector_IncStaleWorkersCleaned(t *testing.T) {
	collector := NewCollector("test-rs-coll-5")

	before := testutil.ToFloat64(StaleWorkersCleanedTotal.WithLabelValues("test-rs-coll-5"))
	collector.IncStaleWorkersCleaned()
	after := testutil.ToFloat64(StaleWorkersCleanedTotal.WithLabelValues("test-rs-coll-5"))

	assert.Equal(t, before+1, after)
}

func TestCollector_IncEventsProcessed(t *testing.T) {
	collector := NewCollector("test-rs-coll-6")

	before := testutil.ToFloat64(EventsProcessedTotal.WithLabelValues("test-rs-coll-6", "test-projection"))
	collector.IncEventsProcessed("test-projection")
	after := testutil.ToFloat64(EventsProcessedTotal.WithLabelValues("test-rs-coll-6", "test-projection"))

	assert.Equal(t, before+1, after)
}

func TestCollector_IncProjectionErrors(t *testing.T) {
	collector := NewCollector("test-rs-coll-7")

	before := testutil.ToFloat64(ProjectionErrorsTotal.WithLabelValues("test-rs-coll-7", "error-projection"))
	collector.IncProjectionErrors("error-projection")
	after := testutil.ToFloat64(ProjectionErrorsTotal.WithLabelValues("test-rs-coll-7", "error-projection"))

	assert.Equal(t, before+1, after)
}

func TestCollector_SetActiveWorkers(t *testing.T) {
	collector := NewCollector("test-rs-coll-8")

	collector.SetActiveWorkers(5)
	value := testutil.ToFloat64(ActiveWorkers.WithLabelValues("test-rs-coll-8"))

	assert.Equal(t, float64(5), value)
}

func TestCollector_SetCurrentGenerationPartitions(t *testing.T) {
	collector := NewCollector("test-rs-coll-9")

	collector.SetCurrentGenerationPartitions(3)
	value := testutil.ToFloat64(CurrentGenerationPartitions.WithLabelValues("test-rs-coll-9"))

	assert.Equal(t, float64(3), value)
}

func TestCollector_SetWorkerState(t *testing.T) {
	collector := NewCollector("test-rs-coll-10")

	collector.SetWorkerState("worker-1", "running")

	runningValue := testutil.ToFloat64(WorkerState.WithLabelValues("test-rs-coll-10", "worker-1", "running"))
	pendingValue := testutil.ToFloat64(WorkerState.WithLabelValues("test-rs-coll-10", "worker-1", "pending"))

	assert.Equal(t, float64(1), runningValue)
	assert.Equal(t, float64(0), pendingValue)
}

func TestCollector_ObserveCoordinationDuration(t *testing.T) {
	collector := NewCollector("test-rs-coll-11")

	collector.ObserveCoordinationDuration(1.5)

	// We can't easily test the exact value of histogram observations,
	// but we can verify that the metric exists and has been updated
	count := testutil.CollectAndCount(CoordinationDuration)
	assert.Greater(t, count, 0)
}

func TestCollector_ObserveEventProcessingDuration(t *testing.T) {
	collector := NewCollector("test-rs-coll-12")

	collector.ObserveEventProcessingDuration("test-proj", 0.5)

	count := testutil.CollectAndCount(EventProcessingDuration)
	assert.Greater(t, count, 0)
}

func TestCollector_ObserveHeartbeatLatency(t *testing.T) {
	collector := NewCollector("test-rs-coll-13")

	collector.ObserveHeartbeatLatency(0.1)

	count := testutil.CollectAndCount(HeartbeatLatency)
	assert.Greater(t, count, 0)
}

func TestCollector_MultipleOperations(t *testing.T) {
	collector := NewCollector("test-rs-coll-multi")

	collector.IncGenerations()
	collector.IncWorkersRegistered()
	collector.SetActiveWorkers(3)
	collector.ObserveCoordinationDuration(2.0)

	generationsValue := testutil.ToFloat64(GenerationsTotal.WithLabelValues("test-rs-coll-multi"))
	workersValue := testutil.ToFloat64(WorkersRegisteredTotal.WithLabelValues("test-rs-coll-multi"))
	activeWorkersValue := testutil.ToFloat64(ActiveWorkers.WithLabelValues("test-rs-coll-multi"))

	assert.Greater(t, generationsValue, float64(0))
	assert.Greater(t, workersValue, float64(0))
	assert.Equal(t, float64(3), activeWorkersValue)
}
