package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/getpup/pupsourcing-orchestrator"
	"github.com/getpup/pupsourcing/es"
	"github.com/getpup/pupsourcing/es/projection"
	"github.com/stretchr/testify/assert"
)

// mockProjectionForMock is a simple test projection
type mockProjectionForMock struct {
	name string
}

func (m *mockProjectionForMock) Name() string {
	return m.name
}

func (m *mockProjectionForMock) Handle(ctx context.Context, event es.PersistedEvent) error {
	return nil
}

func TestMockRunner_RecordsCallsCorrectly(t *testing.T) {
	mock := NewMockRunner()
	ctx := context.Background()

	proj1 := &mockProjectionForMock{name: "projection-1"}
	proj2 := &mockProjectionForMock{name: "projection-2"}
	projections := []projection.Projection{proj1, proj2}

	assignment := orchestrator.PartitionAssignment{
		PartitionKey:    2,
		TotalPartitions: 5,
		GenerationID:    "gen-123",
	}

	// Set a custom RunFunc that returns immediately
	mock.RunFunc = func(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
		return nil
	}

	err := mock.Run(ctx, projections, assignment)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(mock.RunCalls))
	assert.Equal(t, projections, mock.RunCalls[0].Projections)
	assert.Equal(t, assignment, mock.RunCalls[0].Assignment)
}

func TestMockRunner_RecordsMultipleCalls(t *testing.T) {
	mock := NewMockRunner()
	ctx := context.Background()

	// Set a custom RunFunc that returns immediately
	mock.RunFunc = func(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
		return nil
	}

	proj1 := &mockProjectionForMock{name: "projection-1"}
	assignment1 := orchestrator.PartitionAssignment{
		PartitionKey:    0,
		TotalPartitions: 3,
		GenerationID:    "gen-1",
	}

	proj2 := &mockProjectionForMock{name: "projection-2"}
	assignment2 := orchestrator.PartitionAssignment{
		PartitionKey:    1,
		TotalPartitions: 3,
		GenerationID:    "gen-1",
	}

	_ = mock.Run(ctx, []projection.Projection{proj1}, assignment1)
	_ = mock.Run(ctx, []projection.Projection{proj2}, assignment2)

	assert.Equal(t, 2, len(mock.RunCalls))
	assert.Equal(t, "projection-1", mock.RunCalls[0].Projections[0].Name())
	assert.Equal(t, 0, mock.RunCalls[0].Assignment.PartitionKey)
	assert.Equal(t, "projection-2", mock.RunCalls[1].Projections[0].Name())
	assert.Equal(t, 1, mock.RunCalls[1].Assignment.PartitionKey)
}

func TestMockRunner_RespectsContextCancellation(t *testing.T) {
	mock := NewMockRunner()
	ctx, cancel := context.WithCancel(context.Background())

	proj := &mockProjectionForMock{name: "projection"}
	assignment := orchestrator.PartitionAssignment{
		PartitionKey:    0,
		TotalPartitions: 1,
		GenerationID:    "gen-1",
	}

	// Don't set RunFunc, so it uses default behavior (blocks until ctx.Done())
	errChan := make(chan error, 1)
	go func() {
		errChan <- mock.Run(ctx, []projection.Projection{proj}, assignment)
	}()

	// Give it a moment to ensure Run() has started
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for Run to return
	select {
	case err := <-errChan:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	// Verify the call was recorded
	assert.Equal(t, 1, len(mock.RunCalls))
}

func TestMockRunner_CustomRunFuncReturnsError(t *testing.T) {
	mock := NewMockRunner()
	ctx := context.Background()
	expectedErr := errors.New("custom error")

	proj := &mockProjectionForMock{name: "projection"}
	assignment := orchestrator.PartitionAssignment{
		PartitionKey:    0,
		TotalPartitions: 1,
		GenerationID:    "gen-1",
	}

	// Set a RunFunc that returns an error
	mock.RunFunc = func(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
		return expectedErr
	}

	err := mock.Run(ctx, []projection.Projection{proj}, assignment)

	assert.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 1, len(mock.RunCalls))
}

func TestMockRunner_Reset(t *testing.T) {
	mock := NewMockRunner()
	ctx := context.Background()

	proj := &mockProjectionForMock{name: "projection"}
	assignment := orchestrator.PartitionAssignment{
		PartitionKey:    0,
		TotalPartitions: 1,
		GenerationID:    "gen-1",
	}

	// Set a custom RunFunc that returns immediately
	mock.RunFunc = func(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
		return nil
	}

	// Make a call
	_ = mock.Run(ctx, []projection.Projection{proj}, assignment)
	assert.Equal(t, 1, len(mock.RunCalls))

	// Reset
	mock.Reset()
	assert.Equal(t, 0, len(mock.RunCalls))

	// Make another call after reset
	_ = mock.Run(ctx, []projection.Projection{proj}, assignment)
	assert.Equal(t, 1, len(mock.RunCalls))
}

func TestMockRunner_ConcurrentCalls(t *testing.T) {
	mock := NewMockRunner()
	ctx := context.Background()

	proj := &mockProjectionForMock{name: "projection"}
	assignment := orchestrator.PartitionAssignment{
		PartitionKey:    0,
		TotalPartitions: 1,
		GenerationID:    "gen-1",
	}

	// Set a custom RunFunc that returns immediately
	mock.RunFunc = func(ctx context.Context, projections []projection.Projection, assignment orchestrator.PartitionAssignment) error {
		return nil
	}

	// Make concurrent calls
	const numCalls = 10
	done := make(chan bool, numCalls)

	for i := 0; i < numCalls; i++ {
		go func() {
			_ = mock.Run(ctx, []projection.Projection{proj}, assignment)
			done <- true
		}()
	}

	// Wait for all calls to complete
	for i := 0; i < numCalls; i++ {
		<-done
	}

	// Verify all calls were recorded
	assert.Equal(t, numCalls, len(mock.RunCalls))
}

func TestNewMockRunner_InitializesEmptyRunCalls(t *testing.T) {
	mock := NewMockRunner()

	assert.NotNil(t, mock.RunCalls)
	assert.Equal(t, 0, len(mock.RunCalls))
}
