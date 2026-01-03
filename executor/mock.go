package executor

import (
	"context"
	"sync"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing/es/projection"
)

// MockRunner is a mock implementation of Runner for testing.
type MockRunner struct {
	mu       sync.Mutex
	RunFunc  func(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error
	RunCalls []RunCall
}

// RunCall records the parameters of a single Run call.
type RunCall struct {
	Projections []projection.Projection
	Assignment  orchestrator.PartitionAssignment
}

// NewMockRunner creates a new MockRunner with an empty call history.
func NewMockRunner() *MockRunner {
	return &MockRunner{
		RunCalls: make([]RunCall, 0),
	}
}

// Run implements the Runner interface.
// It records the call parameters, then:
// - If RunFunc is set, calls and returns it
// - Otherwise, blocks until ctx.Done() and returns ctx.Err()
func (m *MockRunner) Run(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
	m.mu.Lock()
	m.RunCalls = append(m.RunCalls, RunCall{
		Projections: projections,
		Assignment:  assignment,
	})
	m.mu.Unlock()

	if m.RunFunc != nil {
		return m.RunFunc(ctx, projections, assignment)
	}

	// Default behavior: block until context is cancelled
	<-ctx.Done()
	return ctx.Err()
}

// Reset clears the call history.
func (m *MockRunner) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RunCalls = make([]RunCall, 0)
}
