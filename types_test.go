package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkerState_Constants(t *testing.T) {
	t.Run("WorkerStatePending equals pending", func(t *testing.T) {
		assert.Equal(t, WorkerState("pending"), WorkerStatePending)
	})

	t.Run("WorkerStateReady equals ready", func(t *testing.T) {
		assert.Equal(t, WorkerState("ready"), WorkerStateReady)
	})

	t.Run("WorkerStateRunning equals running", func(t *testing.T) {
		assert.Equal(t, WorkerState("running"), WorkerStateRunning)
	})

	t.Run("WorkerStateStopping equals stopping", func(t *testing.T) {
		assert.Equal(t, WorkerState("stopping"), WorkerStateStopping)
	})

	t.Run("WorkerStateStopped equals stopped", func(t *testing.T) {
		assert.Equal(t, WorkerState("stopped"), WorkerStateStopped)
	})
}

func TestGeneration_ZeroValues(t *testing.T) {
	t.Run("zero value generation", func(t *testing.T) {
		var gen Generation

		assert.Equal(t, "", gen.ID)
		assert.Equal(t, ReplicaSetName(""), gen.ReplicaSet)
		assert.Equal(t, 0, gen.TotalPartitions)
		assert.True(t, gen.CreatedAt.IsZero())
	})

	t.Run("initialized generation", func(t *testing.T) {
		now := time.Now()
		gen := Generation{
			ID:              "gen-123",
			ReplicaSet:      ReplicaSetName("main-projections"),
			TotalPartitions: 3,
			CreatedAt:       now,
		}

		assert.Equal(t, "gen-123", gen.ID)
		assert.Equal(t, ReplicaSetName("main-projections"), gen.ReplicaSet)
		assert.Equal(t, 3, gen.TotalPartitions)
		assert.Equal(t, now, gen.CreatedAt)
	})
}

func TestWorker_ZeroValues(t *testing.T) {
	t.Run("zero value worker", func(t *testing.T) {
		var worker Worker

		assert.Equal(t, "", worker.ID)
		assert.Equal(t, ReplicaSetName(""), worker.ReplicaSet)
		assert.Equal(t, "", worker.GenerationID)
		assert.Equal(t, 0, worker.PartitionKey)
		assert.Equal(t, WorkerState(""), worker.State)
		assert.True(t, worker.LastHeartbeat.IsZero())
		assert.True(t, worker.StartedAt.IsZero())
	})

	t.Run("initialized worker with all fields", func(t *testing.T) {
		now := time.Now()
		lastHeartbeat := now.Add(-5 * time.Second)

		worker := Worker{
			ID:            "worker-456",
			ReplicaSet:    ReplicaSetName("analytics-projections"),
			GenerationID:  "gen-789",
			PartitionKey:  2,
			State:         WorkerStateRunning,
			LastHeartbeat: lastHeartbeat,
			StartedAt:     now,
		}

		assert.Equal(t, "worker-456", worker.ID)
		assert.Equal(t, ReplicaSetName("analytics-projections"), worker.ReplicaSet)
		assert.Equal(t, "gen-789", worker.GenerationID)
		assert.Equal(t, 2, worker.PartitionKey)
		assert.Equal(t, WorkerStateRunning, worker.State)
		assert.Equal(t, lastHeartbeat, worker.LastHeartbeat)
		assert.Equal(t, now, worker.StartedAt)
	})

	t.Run("worker with unassigned partition", func(t *testing.T) {
		worker := Worker{
			ID:           "worker-pending",
			ReplicaSet:   ReplicaSetName("main-projections"),
			GenerationID: "gen-123",
			PartitionKey: -1,
			State:        WorkerStatePending,
		}

		assert.Equal(t, -1, worker.PartitionKey)
		assert.Equal(t, WorkerStatePending, worker.State)
	})
}

func TestPartitionAssignment_Initialization(t *testing.T) {
	t.Run("zero value partition assignment", func(t *testing.T) {
		var assignment PartitionAssignment

		assert.Equal(t, 0, assignment.PartitionKey)
		assert.Equal(t, 0, assignment.TotalPartitions)
		assert.Equal(t, "", assignment.GenerationID)
	})

	t.Run("initialized partition assignment", func(t *testing.T) {
		assignment := PartitionAssignment{
			PartitionKey:    1,
			TotalPartitions: 4,
			GenerationID:    "gen-abc",
		}

		assert.Equal(t, 1, assignment.PartitionKey)
		assert.Equal(t, 4, assignment.TotalPartitions)
		assert.Equal(t, "gen-abc", assignment.GenerationID)
	})

	t.Run("single partition assignment", func(t *testing.T) {
		assignment := PartitionAssignment{
			PartitionKey:    0,
			TotalPartitions: 1,
			GenerationID:    "gen-single",
		}

		assert.Equal(t, 0, assignment.PartitionKey)
		assert.Equal(t, 1, assignment.TotalPartitions)
	})
}
