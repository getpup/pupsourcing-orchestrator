package lifecycle

import (
	"context"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing-orchestrator/store"
	"github.com/getpup/pupsourcing/es"
)

// Config holds configuration for the lifecycle Manager.
type Config struct {
	// Store is the generation store for worker coordination (required).
	Store store.GenerationStore

	// HeartbeatInterval is the interval between heartbeats (default: 5s).
	HeartbeatInterval time.Duration

	// Logger is for observability (optional).
	Logger es.Logger
}

// Manager manages heartbeating and state transitions for a single worker.
type Manager struct {
	config   Config
	workerID string
}

// New creates a new lifecycle Manager with the given configuration.
// Applies default values for HeartbeatInterval if not set.
func New(cfg Config) *Manager {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}

	return &Manager{
		config: cfg,
	}
}

// Register registers a new worker for the given replica set and generation.
// Stores the returned worker ID in the manager and returns it.
func (m *Manager) Register(ctx context.Context, replicaSet orchestrator.ReplicaSetName, generationID string) (string, error) {
	worker, err := m.config.Store.RegisterWorker(ctx, replicaSet, generationID)
	if err != nil {
		return "", err
	}

	m.workerID = worker.ID
	return worker.ID, nil
}

// StartHeartbeat runs a heartbeat loop until the context is cancelled.
// Sends heartbeats at the configured interval and logs if a logger is provided.
func (m *Manager) StartHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.config.Store.Heartbeat(ctx, m.workerID); err != nil {
				if m.config.Logger != nil {
					m.config.Logger.Error(ctx, "heartbeat failed", "workerID", m.workerID, "error", err)
				}
				return err
			}

			if m.config.Logger != nil {
				m.config.Logger.Debug(ctx, "heartbeat sent", "workerID", m.workerID)
			}
		}
	}
}

// UpdateState updates the worker's state and logs the transition if a logger is provided.
func (m *Manager) UpdateState(ctx context.Context, state orchestrator.WorkerState) error {
	if err := m.config.Store.UpdateWorkerState(ctx, m.workerID, state); err != nil {
		return err
	}

	if m.config.Logger != nil {
		m.config.Logger.Info(ctx, "worker state updated", "workerID", m.workerID, "state", state)
	}

	return nil
}

// GetWorker returns the current worker from the store.
func (m *Manager) GetWorker(ctx context.Context) (orchestrator.Worker, error) {
	return m.config.Store.GetWorker(ctx, m.workerID)
}

// WorkerID returns the stored worker ID.
func (m *Manager) WorkerID() string {
	return m.workerID
}
