package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/getpup/pupsourcing/es"
)

// mockProjection is a test implementation of Projection
type mockProjection struct {
	name string
}

func (m *mockProjection) Name() string {
	return m.name
}

//nolint:gocritic // hugeParam: Intentionally pass by value to enforce immutability
func (m *mockProjection) Handle(_ context.Context, _ es.PersistedEvent) error {
	return nil
}

// mockStrategy is a test implementation of Strategy
type mockStrategy struct {
	runCalled     bool
	runError      error
	projections   []Projection
	blockDuration time.Duration
}

func (m *mockStrategy) Run(ctx context.Context, projections []Projection) error {
	m.runCalled = true
	m.projections = projections

	if m.blockDuration > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.blockDuration):
		}
	}

	return m.runError
}

func TestNew_RequiresStrategy(t *testing.T) {
	proj := &mockProjection{name: "test"}

	_, err := New(WithProjections(proj))
	if err == nil {
		t.Fatal("Expected error when strategy is not provided")
	}
	if err.Error() != "strategy is required (use WithStrategy)" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestNew_RequiresProjections(t *testing.T) {
	strategy := &mockStrategy{}

	_, err := New(WithStrategy(strategy))
	if err == nil {
		t.Fatal("Expected error when projections are not provided")
	}
	if err.Error() != "at least one projection is required (use WithProjections)" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestNew_SuccessWithStrategyAndProjections(t *testing.T) {
	strategy := &mockStrategy{}
	proj := &mockProjection{name: "test"}

	orch, err := New(
		WithStrategy(strategy),
		WithProjections(proj),
	)
	if err != nil {
		t.Fatalf("Expected no error, got: %s", err.Error())
	}
	if orch == nil {
		t.Fatal("Expected orchestrator to be created")
	}
}

func TestNew_MultipleProjections(t *testing.T) {
	strategy := &mockStrategy{}
	proj1 := &mockProjection{name: "test1"}
	proj2 := &mockProjection{name: "test2"}
	proj3 := &mockProjection{name: "test3"}

	orch, err := New(
		WithStrategy(strategy),
		WithProjections(proj1, proj2, proj3),
	)
	if err != nil {
		t.Fatalf("Expected no error, got: %s", err.Error())
	}
	if len(orch.projections) != 3 {
		t.Errorf("Expected 3 projections, got %d", len(orch.projections))
	}
}

func TestWithStrategy_NilStrategy(t *testing.T) {
	proj := &mockProjection{name: "test"}

	_, err := New(
		WithStrategy(nil),
		WithProjections(proj),
	)
	if err == nil {
		t.Fatal("Expected error when strategy is nil")
	}
	if err.Error() != "failed to apply option: strategy cannot be nil" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestWithProjections_EmptyList(t *testing.T) {
	strategy := &mockStrategy{}

	_, err := New(
		WithStrategy(strategy),
		WithProjections(),
	)
	if err == nil {
		t.Fatal("Expected error when projections list is empty")
	}
	if err.Error() != "failed to apply option: at least one projection is required" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestRun_CallsStrategy(t *testing.T) {
	strategy := &mockStrategy{blockDuration: 10 * time.Millisecond}
	proj := &mockProjection{name: "test"}

	orch, err := New(
		WithStrategy(strategy),
		WithProjections(proj),
	)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %s", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run in goroutine since it blocks
	done := make(chan error, 1)
	go func() {
		done <- orch.Run(ctx)
	}()

	// Let it run briefly
	time.Sleep(5 * time.Millisecond)
	cancel()

	// Wait for completion
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not complete in time")
	}

	if !strategy.runCalled {
		t.Error("Expected strategy.Run to be called")
	}
	if len(strategy.projections) != 1 {
		t.Errorf("Expected 1 projection passed to strategy, got %d", len(strategy.projections))
	}
}

func TestRun_PropagatesStrategyError(t *testing.T) {
	expectedErr := errors.New("strategy error")
	strategy := &mockStrategy{runError: expectedErr}
	proj := &mockProjection{name: "test"}

	orch, err := New(
		WithStrategy(strategy),
		WithProjections(proj),
	)
	if err != nil {
		t.Fatalf("Failed to create orchestrator: %s", err.Error())
	}

	ctx := context.Background()
	err = orch.Run(ctx)
	if err == nil {
		t.Fatal("Expected error from Run")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to contain strategy error, got: %v", err)
	}
}
