package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// mockLogger captures log messages for testing
type mockLogger struct {
	messages []string
}

func (m *mockLogger) Printf(format string, v ...interface{}) {
	// Don't use fmt.Sprintf here since we might get plain strings from log.Printf
	// Just check the format string matches and arguments exist
	m.messages = append(m.messages, format)
}

func TestRecreate_Constructor(t *testing.T) {
	strategy := Recreate()
	if strategy == nil {
		t.Fatal("Expected Recreate() to return a non-nil strategy")
	}
	if strategy.Logger != nil {
		t.Error("Expected default logger to be nil")
	}
}

func TestRecreateStrategy_Run_LogsProjections(t *testing.T) {
	logger := &mockLogger{}
	strategy := &RecreateStrategy{Logger: logger}

	proj1 := &mockProjection{name: "projection1"}
	proj2 := &mockProjection{name: "projection2"}
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

	// Verify logging
	if len(logger.messages) < 2 {
		t.Fatalf("Expected at least 2 log messages, got %d", len(logger.messages))
	}

	// Check for start message (format string)
	found := false
	for _, msg := range logger.messages {
		if strings.Contains(msg, "Starting Recreate strategy") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find 'Starting Recreate strategy' in logs")
	}

	// Check for projection registration (format string)
	foundProjMsg := false
	for _, msg := range logger.messages {
		if strings.Contains(msg, "Projection registered:") {
			foundProjMsg = true
			break
		}
	}
	if !foundProjMsg {
		t.Error("Expected to find projection registration message in logs")
	}

	// We should have at least: start message + 2 projection messages = 3 messages minimum
	if len(logger.messages) < 3 {
		t.Errorf("Expected at least 3 log messages (start + 2 projections), got %d", len(logger.messages))
	}
}

func TestRecreateStrategy_Run_GracefulShutdown(t *testing.T) {
	logger := &mockLogger{}
	strategy := &RecreateStrategy{Logger: logger}

	proj := &mockProjection{name: "test"}
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

	// Check for shutdown message
	found := false
	for _, msg := range logger.messages {
		if strings.Contains(msg, "Context canceled") || strings.Contains(msg, "stopping") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find shutdown message in logs")
	}
}

func TestRecreateStrategy_Run_EmptyProjectionsList(t *testing.T) {
	logger := &mockLogger{}
	strategy := &RecreateStrategy{Logger: logger}

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

	// Verify it started with 0 projections
	if len(logger.messages) == 0 {
		t.Fatal("Expected at least one log message")
	}
	if !strings.Contains(logger.messages[0], "Starting Recreate strategy") {
		t.Errorf("Expected message about starting strategy, got: %s", logger.messages[0])
	}
}

func TestRecreateStrategy_DefaultLogger(t *testing.T) {
	// Test that default logger doesn't panic
	strategy := &RecreateStrategy{Logger: nil}

	proj := &mockProjection{name: "test"}
	projections := []Projection{proj}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Should not panic
	err := strategy.Run(ctx, projections)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context error, got: %v", err)
	}
}
