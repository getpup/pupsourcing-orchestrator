package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/getpup/pupsourcing/es"
)

// mockTestProjection for testing (also implements projection.Projection)
type mockTestProjection struct {
	name string
}

func (m *mockTestProjection) Name() string {
	return m.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (m *mockTestProjection) Handle(_ context.Context, _ es.PersistedEvent) error {
	return nil
}

func TestRecreate_Constructor(t *testing.T) {
	strategy := Recreate()
	if strategy == nil {
		t.Fatal("Expected Recreate() to return a non-nil strategy")
	}
}

func TestRecreateStrategy_Run_RegistersProjections(t *testing.T) {
	strategy := &RecreateStrategy{}

	proj1 := &mockTestProjection{name: "projection1"}
	proj2 := &mockTestProjection{name: "projection2"}
	projections := []Projection{proj1, proj2}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine since it blocks
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_Run_GracefulShutdown(t *testing.T) {
	strategy := &RecreateStrategy{}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithCancel(context.Background())

	// Run in goroutine
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, projections)
	}()

	// Cancel immediately
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_Run_EmptyProjectionsList(t *testing.T) {
	strategy := &RecreateStrategy{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run with empty projections
	done := make(chan error, 1)
	go func() {
		done <- strategy.Run(ctx, []Projection{})
	}()

	// Cancel immediately
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}
}

func TestRecreateStrategy_Run_ContextTimeout(t *testing.T) {
	strategy := &RecreateStrategy{}

	proj := &mockTestProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Should not panic
	err := strategy.Run(ctx, projections)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context error, got: %v", err)
	}
}
