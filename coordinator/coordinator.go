package coordinator

import (
	"context"
	"errors"
	"fmt"
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

// GetOrCreateGeneration returns the current active generation for the replica set.
// If no generation exists, creates one with 1 partition.
// This method does NOT handle reconfiguration - that's done separately.
func (c *Coordinator) GetOrCreateGeneration(ctx context.Context) (orchestrator.Generation, error) {
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

// CountExpectedWorkers returns the number of workers that should be in the current generation.
// This is: active workers (non-stopped, non-stale) + pending workers in current generation.
func (c *Coordinator) CountExpectedWorkers(ctx context.Context, generationID string) (int, error) {
	// Get all workers for this generation
	workers, err := c.config.Store.GetWorkersByGeneration(ctx, generationID)
	if err != nil {
		return 0, err
	}

	// Count non-stopped workers that are not stale
	count := 0
	now := time.Now()
	for _, w := range workers {
		// Skip stopped workers
		if w.State == orchestrator.WorkerStateStopped {
			continue
		}
		// Skip stale workers
		if now.Sub(w.LastHeartbeat) > c.config.StaleWorkerTimeout {
			continue
		}
		count++
	}

	return count, nil
}

// ShouldTriggerReconfiguration checks if a new generation should be created.
// Returns true if there are pending workers waiting for partition assignment,
// OR if there are stale workers that need to be cleaned up.
func (c *Coordinator) ShouldTriggerReconfiguration(ctx context.Context) (bool, error) {
	// Check for pending workers
	pendingWorkers, err := c.config.Store.GetPendingWorkers(ctx, c.replicaSet)
	if err != nil {
		return false, err
	}

	if len(pendingWorkers) > 0 {
		return true, nil
	}

	// Check for stale workers
	activeWorkers, err := c.config.Store.GetActiveWorkers(ctx, c.replicaSet)
	if err != nil {
		return false, err
	}

	now := time.Now()
	for _, w := range activeWorkers {
		if now.Sub(w.LastHeartbeat) > c.config.StaleWorkerTimeout {
			return true, nil
		}
	}

	return false, nil
}

// TriggerReconfiguration creates a new generation with the correct number of partitions.
// It counts active (non-stale) workers + pending workers to determine partition count.
// Returns the new generation.
func (c *Coordinator) TriggerReconfiguration(ctx context.Context) (orchestrator.Generation, error) {
	// Clean up stale workers first
	if err := c.CleanupStaleWorkers(ctx); err != nil {
		return orchestrator.Generation{}, fmt.Errorf("failed to cleanup stale workers: %w", err)
	}

	// Count active workers (will be the new partition count)
	activeWorkers, err := c.config.Store.GetActiveWorkers(ctx, c.replicaSet)
	if err != nil {
		return orchestrator.Generation{}, err
	}

	partitionCount := len(activeWorkers)
	if partitionCount < 1 {
		partitionCount = 1
	}

	newGen, err := c.config.Store.CreateGeneration(ctx, c.replicaSet, partitionCount)
	if err != nil {
		return orchestrator.Generation{}, err
	}

	if c.config.Logger != nil {
		c.config.Logger.Info(ctx, "triggered reconfiguration",
			"replicaSet", c.replicaSet,
			"newGenerationID", newGen.ID,
			"totalPartitions", partitionCount)
	}

	return newGen, nil
}

// WaitForExpectedWorkers waits until the expected number of workers have registered for the given generation.
// The expected count is the generation's TotalPartitions.
// Returns nil when the expected workers have registered.
// Returns nil if the timeout is exceeded (graceful degradation - proceed with available workers).
func (c *Coordinator) WaitForExpectedWorkers(ctx context.Context, generationID string) error {
	// Get active generation to determine expected worker count
	gen, err := c.config.Store.GetActiveGeneration(ctx, c.replicaSet)
	if err != nil {
		return err
	}

	expectedWorkers := gen.TotalPartitions
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

			// Count non-stopped workers
			activeCount := 0
			now := time.Now()
			for _, w := range workers {
				// Skip stopped workers
				if w.State == orchestrator.WorkerStateStopped {
					continue
				}
				// Skip stale workers
				if now.Sub(w.LastHeartbeat) > c.config.StaleWorkerTimeout {
					continue
				}
				activeCount++
			}

			if activeCount >= expectedWorkers {
				if c.config.Logger != nil {
					c.config.Logger.Info(ctx, "expected workers have registered",
						"generationID", generationID,
						"expectedWorkers", expectedWorkers,
						"activeCount", activeCount)
				}
				return nil
			}

			// Check timeout
			if time.Since(startTime) > c.config.CoordinationTimeout {
				// Graceful degradation - proceed with available workers
				if c.config.Logger != nil {
					c.config.Logger.Info(ctx, "timeout waiting for expected workers, proceeding with available workers",
						"generationID", generationID,
						"expectedWorkers", expectedWorkers,
						"activeCount", activeCount)
				}
				return nil
			}
		}
	}
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

// IsLeader checks if the given worker is the leader for partition assignment.
// The leader is the worker with the lexicographically smallest ID in the generation.
// Leaders are responsible for calling AssignPartitions.
func (c *Coordinator) IsLeader(ctx context.Context, generationID string, workerID string) (bool, error) {
	workers, err := c.config.Store.GetWorkersByGeneration(ctx, generationID)
	if err != nil {
		return false, err
	}

	if len(workers) == 0 {
		return false, nil
	}

	// Find the worker with smallest ID
	leaderID := workers[0].ID
	for _, w := range workers[1:] {
		if w.ID < leaderID {
			leaderID = w.ID
		}
	}

	return workerID == leaderID, nil
}

// AssignPartitionsIfLeader assigns partitions only if this worker is the leader.
// Returns true if this worker performed the assignment, false otherwise.
func (c *Coordinator) AssignPartitionsIfLeader(ctx context.Context, generationID string, workerID string) (bool, error) {
	isLeader, err := c.IsLeader(ctx, generationID, workerID)
	if err != nil {
		return false, err
	}

	if !isLeader {
		return false, nil
	}

	assigner := NewAssigner(c.config.Store)
	if err := assigner.AssignPartitions(ctx, generationID); err != nil {
		return false, err
	}

	if c.config.Logger != nil {
		c.config.Logger.Info(ctx, "leader assigned partitions",
			"workerID", workerID,
			"generationID", generationID)
	}

	return true, nil
}

// JoinOrCreate coordinates a worker joining or creating a generation.
// It handles the full coordination flow:
// 1. Get or create the current generation
// 2. Return the active generation (without checking for reconfiguration)
//
// This method is designed to be called by workers at startup or after
// a generation supersession event. The worker should register in the returned
// generation, then the orchestrator will check if reconfiguration is needed.
func (c *Coordinator) JoinOrCreate(ctx context.Context) (orchestrator.Generation, error) {
	// Get or create the current generation
	gen, err := c.GetOrCreateGeneration(ctx)
	if err != nil {
		return orchestrator.Generation{}, fmt.Errorf("failed to get or create generation: %w", err)
	}

	return gen, nil
}
