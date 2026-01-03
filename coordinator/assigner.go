package coordinator

import (
	"context"
	"fmt"
	"sort"

	"github.com/getpup/pupsourcing-orchestrator/store"
)

// Assigner handles deterministic partition assignment to workers.
type Assigner struct {
	store store.GenerationStore
}

// NewAssigner creates a new Assigner with the given generation store.
func NewAssigner(s store.GenerationStore) *Assigner {
	return &Assigner{
		store: s,
	}
}

// AssignPartitions assigns partitions to all workers in a generation.
// Workers are sorted by ID (ascending string order) to ensure deterministic assignment.
// Each worker receives a partition key equal to its index in the sorted list (0, 1, 2, ...).
// If no workers exist for the generation, returns nil without error.
// If any assignment fails, returns an error immediately.
func (a *Assigner) AssignPartitions(ctx context.Context, generationID string) error {
	workers, err := a.store.GetWorkersByGeneration(ctx, generationID)
	if err != nil {
		return err
	}

	// Nothing to assign if no workers
	if len(workers) == 0 {
		return nil
	}

	// Sort workers by ID for deterministic ordering
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].ID < workers[j].ID
	})

	// Assign partition keys sequentially
	for i, worker := range workers {
		if err := a.store.AssignPartition(ctx, worker.ID, i); err != nil {
			return fmt.Errorf("failed to assign partition %d to worker %s: %w", i, worker.ID, err)
		}
	}

	return nil
}
