package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/getpup/pupsourcing/es"
)

// Config holds configuration for the Coordinator.
type Config struct {
	// Store is the generation store for worker coordination (required).
	Store store.GenerationStore

	// StaleWorkerTimeout is the duration after which a worker is considered dead (default: 30s).
	StaleWorkerTimeout time.Duration

	// PollInterval is how often to check state (default: 1s).
	PollInterval time.Duration

	// CoordinationTimeout is the max time to wait for coordination (default: 60s).
	CoordinationTimeout time.Duration

	// Logger is for observability (optional).
	Logger es.Logger
}

// Coordinator coordinates generation transitions when workers join or leave.
type Coordinator struct {
	config     Config
	replicaSet orchestrator.ReplicaSetName
}

// New creates a new Coordinator with the given configuration and replica set.
// Applies default values for timeout/interval values if zero.
func New(cfg Config, replicaSet orchestrator.ReplicaSetName) *Coordinator {
	if cfg.StaleWorkerTimeout == 0 {
		cfg.StaleWorkerTimeout = 30 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.CoordinationTimeout == 0 {
		cfg.CoordinationTimeout = 60 * time.Second
	}

	return &Coordinator{
		config:     cfg,
		replicaSet: replicaSet,
	}
}

// JoinOrCreate gets the active generation for the replica set, or creates one if none exists.
// This method simply returns the current active generation or creates the first generation.
// It does NOT trigger reconfiguration based on pending or stale workers.
//
// NOTE: The current JoinOrCreate implementation is simplified for single-worker scenarios.
// For multi-worker coordination in the Recreate strategy, the orchestrator (Task 9) is
// responsible for coordinating generation transitions. Workers should:
// 1. Join the current generation
// 2. Register themselves
// 3. Wait for partition assignment
// 4. Watch for generation changes and restart when superseded
func (c *Coordinator) JoinOrCreate(ctx context.Context) (orchestrator.Generation, error) {
	gen, err := c.config.Store.GetActiveGeneration(ctx, c.replicaSet)
	if errors.Is(err, orchestrator.ErrReplicaSetNotFound) {
		// No generation exists, create first one with 1 partition
		newGen, createErr := c.config.Store.CreateGeneration(ctx, c.replicaSet, 1)
		if createErr != nil {
			return orchestrator.Generation{}, createErr
		}
		if c.config.Logger != nil {
			c.config.Logger.Info(ctx, "created first generation",
				"replicaSet", c.replicaSet, "generationID", newGen.ID)
		}
		return newGen, nil
	}
	if err != nil {
		return orchestrator.Generation{}, err
	}
	return gen, nil
}

// NeedsReconfiguration checks if reconfiguration is needed due to pending or stale workers.
// Returns (needsReconfig bool, newPartitionCount int, error).
func (c *Coordinator) NeedsReconfiguration(ctx context.Context) (bool, int, error) {
	// Check for pending workers
	pendingWorkers, err := c.config.Store.GetPendingWorkers(ctx, c.replicaSet)
	if err != nil {
		return false, 0, err
	}

	// Check for stale workers
	activeWorkers, err := c.config.Store.GetActiveWorkers(ctx, c.replicaSet)
	if err != nil {
		return false, 0, err
	}

	staleCount := 0
	now := time.Now()
	for _, w := range activeWorkers {
		if now.Sub(w.LastHeartbeat) > c.config.StaleWorkerTimeout {
			staleCount++
		}
	}

	// If we have pending workers or stale workers, need to reconfigure
	if len(pendingWorkers) > 0 || staleCount > 0 {
		// Calculate new partition count: active workers - stale + pending
		newPartitionCount := len(activeWorkers) - staleCount + len(pendingWorkers)
		if newPartitionCount < 1 {
			newPartitionCount = 1
		}
		return true, newPartitionCount, nil
	}

	return false, 0, nil
}

// CreateNewGeneration creates a new generation with the specified number of partitions.
// This should be called when reconfiguration is needed (e.g., workers joining or leaving).
func (c *Coordinator) CreateNewGeneration(ctx context.Context, totalPartitions int) (orchestrator.Generation, error) {
	newGen, err := c.config.Store.CreateGeneration(ctx, c.replicaSet, totalPartitions)
	if err != nil {
		return orchestrator.Generation{}, err
	}

	if c.config.Logger != nil {
		c.config.Logger.Info(ctx, "created new generation",
			"replicaSet", c.replicaSet,
			"generationID", newGen.ID,
			"totalPartitions", totalPartitions)
	}

	return newGen, nil
}

// WaitForPartitionAssignment polls the store until the worker has a partition assignment.
// Returns the partition assignment once PartitionKey >= 0.
// Returns ErrCoordinationTimeout if the timeout is exceeded.
func (c *Coordinator) WaitForPartitionAssignment(ctx context.Context, workerID string) (orchestrator.PartitionAssignment, error) {
	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return orchestrator.PartitionAssignment{}, ctx.Err()
		case <-ticker.C:
			worker, err := c.config.Store.GetWorker(ctx, workerID)
			if err != nil {
				return orchestrator.PartitionAssignment{}, err
			}

			if worker.PartitionKey >= 0 {
				gen, err := c.config.Store.GetActiveGeneration(ctx, c.replicaSet)
				if err != nil {
					return orchestrator.PartitionAssignment{}, err
				}

				return orchestrator.PartitionAssignment{
					PartitionKey:    worker.PartitionKey,
					TotalPartitions: gen.TotalPartitions,
					GenerationID:    worker.GenerationID,
				}, nil
			}

			if time.Since(startTime) > c.config.CoordinationTimeout {
				return orchestrator.PartitionAssignment{}, orchestrator.ErrCoordinationTimeout
			}
		}
	}
}

// WaitForAllReady polls the store until all workers in the generation are in Ready state.
// Returns nil when all workers are ready.
// Returns ErrCoordinationTimeout if the timeout is exceeded.
func (c *Coordinator) WaitForAllReady(ctx context.Context, generationID string) error {
	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			workers, err := c.config.Store.GetWorkersByGeneration(ctx, generationID)
			if err != nil {
				return err
			}

			if len(workers) == 0 {
				if time.Since(startTime) > c.config.CoordinationTimeout {
					return orchestrator.ErrCoordinationTimeout
				}
				continue
			}

			allReady := true
			for _, w := range workers {
				if w.State != orchestrator.WorkerStateReady {
					allReady = false
					break
				}
			}

			if allReady {
				if c.config.Logger != nil {
					c.config.Logger.Info(ctx, "all workers ready", "generationID", generationID, "workerCount", len(workers))
				}
				return nil
			}

			if time.Since(startTime) > c.config.CoordinationTimeout {
				return orchestrator.ErrCoordinationTimeout
			}
		}
	}
}

// WatchGeneration polls the store to detect when the active generation changes.
// Returns ErrGenerationSuperseded when the active generation ID differs from the provided generationID.
// Returns nil if the context is cancelled.
// Continues polling on transient store errors (logs and retries).
func (c *Coordinator) WatchGeneration(ctx context.Context, generationID string) error {
	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			gen, err := c.config.Store.GetActiveGeneration(ctx, c.replicaSet)
			if err != nil {
				// Log transient errors and continue
				if c.config.Logger != nil {
					c.config.Logger.Error(ctx, "failed to get active generation during watch", "error", err)
				}
				continue
			}

			if gen.ID != generationID {
				if c.config.Logger != nil {
					c.config.Logger.Info(ctx, "generation superseded",
						"oldGenerationID", generationID,
						"newGenerationID", gen.ID)
				}
				return orchestrator.ErrGenerationSuperseded
			}
		}
	}
}

// CleanupStaleWorkers identifies and marks stale workers as dead.
// A worker is considered stale if its LastHeartbeat is older than StaleWorkerTimeout.
func (c *Coordinator) CleanupStaleWorkers(ctx context.Context) error {
	activeWorkers, err := c.config.Store.GetActiveWorkers(ctx, c.replicaSet)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, w := range activeWorkers {
		if now.Sub(w.LastHeartbeat) > c.config.StaleWorkerTimeout {
			if err := c.config.Store.MarkWorkerDead(ctx, w.ID); err != nil {
				return err
			}

			if c.config.Logger != nil {
				c.config.Logger.Info(ctx, "marked stale worker as dead",
					"workerID", w.ID,
					"lastHeartbeat", w.LastHeartbeat,
					"staleDuration", now.Sub(w.LastHeartbeat))
			}
		}
	}

	return nil
}
