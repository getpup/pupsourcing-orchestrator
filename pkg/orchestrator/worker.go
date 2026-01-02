package orchestrator

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultHeartbeatInterval is the default time between worker heartbeats
	DefaultHeartbeatInterval = 5 * time.Second

	// DefaultStaleWorkerThreshold is the default duration after which a worker is considered stale
	DefaultStaleWorkerThreshold = 30 * time.Second

	// DefaultCleanupTimeout is the default timeout for cleanup operations
	DefaultCleanupTimeout = 5 * time.Second
)

// WorkerState represents the current state of a worker in the lifecycle
type WorkerState string

const (
	// WorkerStateStarting indicates the worker is initializing
	WorkerStateStarting WorkerState = "starting"

	// WorkerStateReady indicates the worker is connected and idle
	WorkerStateReady WorkerState = "ready"

	// WorkerStateRunning indicates the worker is processing shards
	WorkerStateRunning WorkerState = "running"

	// WorkerStateDraining indicates the worker is finishing current batch
	WorkerStateDraining WorkerState = "draining"

	// WorkerStateStopped indicates the worker has stopped
	WorkerStateStopped WorkerState = "stopped"
)

// Worker represents an orchestrator instance with identity and lifecycle management
type Worker struct {
	id                 string
	state              WorkerState
	generationSeen     int64
	lastHeartbeat      time.Time
	heartbeatInterval  time.Duration
	heartbeatStopChan  chan struct{}
	heartbeatDone      chan struct{}
	mu                 sync.RWMutex
	persistenceAdapter WorkerPersistenceAdapter
}

// WorkerPersistenceAdapter defines the interface for persisting worker state to the database
type WorkerPersistenceAdapter interface {
	// RegisterWorker inserts or updates a worker record in the database
	RegisterWorker(ctx context.Context, workerID string, generation int64, state WorkerState) error

	// UpdateWorkerHeartbeat updates the last_heartbeat timestamp for a worker
	UpdateWorkerHeartbeat(ctx context.Context, workerID string, state WorkerState) error

	// UpdateWorkerState updates the worker's state
	UpdateWorkerState(ctx context.Context, workerID string, state WorkerState, generation int64) error

	// GetCurrentGeneration retrieves the current recreate generation
	GetCurrentGeneration(ctx context.Context) (int64, error)

	// DeleteStaleWorkers removes workers whose last heartbeat is older than the threshold
	DeleteStaleWorkers(ctx context.Context, staleThreshold time.Duration) error
}

// WorkerConfig configures worker behavior
type WorkerConfig struct {
	// HeartbeatInterval is the duration between heartbeat updates
	HeartbeatInterval time.Duration

	// PersistenceAdapter handles database operations
	PersistenceAdapter WorkerPersistenceAdapter
}

// NewWorker creates a new worker with a stable identity
func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.PersistenceAdapter == nil {
		return nil, fmt.Errorf("persistence adapter is required")
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = DefaultHeartbeatInterval
	}

	// Generate stable worker ID using hostname + UUID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	workerID := fmt.Sprintf("%s-%s", hostname, uuid.New().String())

	return &Worker{
		id:                 workerID,
		state:              WorkerStateStarting,
		generationSeen:     0,
		lastHeartbeat:      time.Now(),
		heartbeatInterval:  config.HeartbeatInterval,
		heartbeatStopChan:  make(chan struct{}),
		heartbeatDone:      make(chan struct{}),
		persistenceAdapter: config.PersistenceAdapter,
	}, nil
}

// ID returns the worker's unique identifier
func (w *Worker) ID() string {
	return w.id
}

// State returns the worker's current state
func (w *Worker) State() WorkerState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state
}

// GenerationSeen returns the last observed generation
func (w *Worker) GenerationSeen() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.generationSeen
}

// Start initializes the worker and begins heartbeat mechanism
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get current generation
	generation, err := w.persistenceAdapter.GetCurrentGeneration(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current generation: %w", err)
	}
	w.generationSeen = generation

	// Register worker in database
	if err := w.persistenceAdapter.RegisterWorker(ctx, w.id, w.generationSeen, WorkerStateStarting); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	// Start heartbeat goroutine
	go w.heartbeatLoop(ctx)

	return nil
}

// TransitionTo changes the worker state
func (w *Worker) TransitionTo(ctx context.Context, newState WorkerState) error {
	w.mu.Lock()
	oldState := w.state
	w.state = newState
	generation := w.generationSeen
	w.mu.Unlock()

	if err := w.persistenceAdapter.UpdateWorkerState(ctx, w.id, newState, generation); err != nil {
		// Rollback state on error
		w.mu.Lock()
		w.state = oldState
		w.mu.Unlock()
		return fmt.Errorf("failed to update worker state: %w", err)
	}

	return nil
}

// ObserveGeneration updates the worker's observed generation
func (w *Worker) ObserveGeneration(ctx context.Context) error {
	generation, err := w.persistenceAdapter.GetCurrentGeneration(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current generation: %w", err)
	}

	w.mu.Lock()
	w.generationSeen = generation
	w.mu.Unlock()

	return nil
}

// SetGenerationSeen updates the worker's observed generation without fetching from database.
// This is useful when the generation has just been advanced by the current worker.
func (w *Worker) SetGenerationSeen(generation int64) {
	w.mu.Lock()
	w.generationSeen = generation
	w.mu.Unlock()
}

// Stop gracefully stops the worker
func (w *Worker) Stop(ctx context.Context) error {
	// Stop heartbeat
	close(w.heartbeatStopChan)

	// Wait for heartbeat to finish with timeout
	select {
	case <-w.heartbeatDone:
	case <-time.After(DefaultCleanupTimeout):
		// Timeout waiting for heartbeat to stop
	}

	// Transition to stopped state
	return w.TransitionTo(ctx, WorkerStateStopped)
}

// heartbeatLoop periodically updates the worker's heartbeat
func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer close(w.heartbeatDone)

	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.heartbeatStopChan:
			return
		case <-ticker.C:
			w.mu.RLock()
			state := w.state
			w.mu.RUnlock()

			if err := w.persistenceAdapter.UpdateWorkerHeartbeat(ctx, w.id, state); err != nil {
				// Log error but continue - heartbeat failures shouldn't stop the worker
				// In production, this would use a proper logger
				continue
			}

			w.mu.Lock()
			w.lastHeartbeat = time.Now()
			w.mu.Unlock()
		}
	}
}

// CleanupStaleWorkers removes workers that haven't sent a heartbeat within the threshold
// This is typically called during Recreate phase to clean up crashed workers
func CleanupStaleWorkers(ctx context.Context, adapter WorkerPersistenceAdapter, staleThreshold time.Duration) error {
	if adapter == nil {
		return fmt.Errorf("persistence adapter is required")
	}

	if staleThreshold == 0 {
		staleThreshold = DefaultStaleWorkerThreshold
	}

	return adapter.DeleteStaleWorkers(ctx, staleThreshold)
}
