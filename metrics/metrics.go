package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// GenerationsTotal tracks the total number of generations created.
var GenerationsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_generations_total",
		Help: "Total number of generations created",
	},
	[]string{"replica_set"},
)

// WorkersRegisteredTotal tracks the total number of workers registered.
var WorkersRegisteredTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_workers_registered_total",
		Help: "Total workers registered",
	},
	[]string{"replica_set"},
)

// PartitionAssignmentsTotal tracks the total number of partition assignments.
var PartitionAssignmentsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_partition_assignments_total",
		Help: "Total partition assignments",
	},
	[]string{"replica_set"},
)

// ReconfigurationTotal tracks the total number of reconfigurations triggered.
var ReconfigurationTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_reconfiguration_total",
		Help: "Total reconfigurations triggered",
	},
	[]string{"replica_set"},
)

// StaleWorkersCleanedTotal tracks the total number of stale workers cleaned up.
var StaleWorkersCleanedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_stale_workers_cleaned_total",
		Help: "Total stale workers cleaned up",
	},
	[]string{"replica_set"},
)

// EventsProcessedTotal tracks the total number of events processed.
var EventsProcessedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_events_processed_total",
		Help: "Total events processed",
	},
	[]string{"replica_set", "projection"},
)

// ProjectionErrorsTotal tracks the total number of projection errors.
var ProjectionErrorsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pupsourcing_orchestrator_projection_errors_total",
		Help: "Total projection errors",
	},
	[]string{"replica_set", "projection"},
)

// ActiveWorkers tracks the current number of active workers.
var ActiveWorkers = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "pupsourcing_orchestrator_active_workers",
		Help: "Current active workers",
	},
	[]string{"replica_set"},
)

// CurrentGenerationPartitions tracks the current generation partition count.
var CurrentGenerationPartitions = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "pupsourcing_orchestrator_current_generation_partitions",
		Help: "Current generation partition count",
	},
	[]string{"replica_set"},
)

// WorkerState tracks worker state (value 1 for current state, 0 otherwise).
var WorkerState = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "pupsourcing_orchestrator_worker_state",
		Help: "Worker state (1 for current state, 0 otherwise)",
	},
	[]string{"replica_set", "worker_id", "state"},
)

// CoordinationDuration tracks time spent in coordination phase.
var CoordinationDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "pupsourcing_orchestrator_coordination_duration_seconds",
		Help:    "Time spent in coordination phase",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"replica_set"},
)

// EventProcessingDuration tracks event processing latency.
var EventProcessingDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "pupsourcing_orchestrator_event_processing_duration_seconds",
		Help:    "Event processing latency",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"replica_set", "projection"},
)

// HeartbeatLatency tracks heartbeat round-trip latency.
var HeartbeatLatency = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "pupsourcing_orchestrator_heartbeat_latency_seconds",
		Help:    "Heartbeat round-trip latency",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"replica_set"},
)
