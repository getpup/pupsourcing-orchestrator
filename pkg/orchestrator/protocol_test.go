package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// mockProtocolPersistence is a mock implementation of ProtocolPersistence
type mockProtocolPersistence struct {
	state                  *ProtocolState
	readyWorkerCount       int
	totalActiveWorkerCount int
	activeWorkerIDs        []string
	projectionNames        []string
	shardCounts            map[string]int
	shardAssignments       map[string]map[string][]int // projection -> worker -> shards
	clearedShardOwnership  bool

	// Error injection
	getStateErr        error
	transitionPhaseErr error
	incrementGenErr    error
	getReadyCountErr   error
	getTotalCountErr   error
	clearOwnershipErr  error
	assignShardsErr    error
	getProjectionsErr  error
	getWorkersErr      error
	getShardCountErr   error
}

func newMockProtocolPersistence() *mockProtocolPersistence {
	return &mockProtocolPersistence{
		state: &ProtocolState{
			Generation: 0,
			Phase:      RecreatePhaseIdle,
		},
		activeWorkerIDs:  []string{"worker-1", "worker-2"},
		projectionNames:  []string{"projection-1"},
		shardCounts:      map[string]int{"projection-1": 4},
		shardAssignments: make(map[string]map[string][]int),
	}
}

func (m *mockProtocolPersistence) GetProtocolState(ctx context.Context) (*ProtocolState, error) {
	if m.getStateErr != nil {
		return nil, m.getStateErr
	}
	return &ProtocolState{
		Generation: m.state.Generation,
		Phase:      m.state.Phase,
	}, nil
}

func (m *mockProtocolPersistence) TransitionPhase(ctx context.Context, newPhase RecreatePhase) error {
	if m.transitionPhaseErr != nil {
		return m.transitionPhaseErr
	}
	m.state.Phase = newPhase
	return nil
}

func (m *mockProtocolPersistence) IncrementGeneration(ctx context.Context) (int64, error) {
	if m.incrementGenErr != nil {
		return 0, m.incrementGenErr
	}
	m.state.Generation++
	m.state.Phase = RecreatePhaseDraining
	return m.state.Generation, nil
}

func (m *mockProtocolPersistence) GetReadyWorkerCount(ctx context.Context) (int, error) {
	if m.getReadyCountErr != nil {
		return 0, m.getReadyCountErr
	}
	return m.readyWorkerCount, nil
}

func (m *mockProtocolPersistence) GetTotalActiveWorkerCount(ctx context.Context) (int, error) {
	if m.getTotalCountErr != nil {
		return 0, m.getTotalCountErr
	}
	return m.totalActiveWorkerCount, nil
}

func (m *mockProtocolPersistence) ClearAllShardOwnership(ctx context.Context) error {
	if m.clearOwnershipErr != nil {
		return m.clearOwnershipErr
	}
	m.clearedShardOwnership = true
	m.shardAssignments = make(map[string]map[string][]int)
	return nil
}

func (m *mockProtocolPersistence) AssignShardsToWorker(ctx context.Context, projectionName string, shardIDs []int, workerID string) error {
	if m.assignShardsErr != nil {
		return m.assignShardsErr
	}
	if m.shardAssignments[projectionName] == nil {
		m.shardAssignments[projectionName] = make(map[string][]int)
	}
	m.shardAssignments[projectionName][workerID] = shardIDs
	return nil
}

func (m *mockProtocolPersistence) GetAllProjectionNames(ctx context.Context) ([]string, error) {
	if m.getProjectionsErr != nil {
		return nil, m.getProjectionsErr
	}
	return m.projectionNames, nil
}

func (m *mockProtocolPersistence) GetActiveWorkerIDs(ctx context.Context) ([]string, error) {
	if m.getWorkersErr != nil {
		return nil, m.getWorkersErr
	}
	return m.activeWorkerIDs, nil
}

func (m *mockProtocolPersistence) GetShardCount(ctx context.Context, projectionName string) (int, error) {
	if m.getShardCountErr != nil {
		return 0, m.getShardCountErr
	}
	count, ok := m.shardCounts[projectionName]
	if !ok {
		return 0, fmt.Errorf("projection not found: %s", projectionName)
	}
	return count, nil
}

func TestNewProtocolCoordinator_RequiresPersistence(t *testing.T) {
	_, err := NewProtocolCoordinator(nil)
	if err == nil {
		t.Fatal("Expected error when persistence is nil")
	}
}

func TestNewProtocolCoordinator_Success(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, err := NewProtocolCoordinator(persistence)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if coordinator == nil {
		t.Fatal("Expected coordinator to be created")
	}
}

func TestGetCurrentState(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, _ := NewProtocolCoordinator(persistence)

	ctx := context.Background()
	state, err := coordinator.GetCurrentState(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if state.Generation != 0 {
		t.Errorf("Expected generation 0, got: %d", state.Generation)
	}
	if state.Phase != RecreatePhaseIdle {
		t.Errorf("Expected phase idle, got: %s", state.Phase)
	}
}

func TestGetCurrentState_Error(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.getStateErr = errors.New("database error")
	coordinator, _ := NewProtocolCoordinator(persistence)

	ctx := context.Background()
	_, err := coordinator.GetCurrentState(ctx)
	if err == nil {
		t.Fatal("Expected error when getting state fails")
	}
}

func TestStartRecreate_IncrementsGeneration(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, _ := NewProtocolCoordinator(persistence)

	ctx := context.Background()
	generation, err := coordinator.StartRecreate(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if generation != 1 {
		t.Errorf("Expected generation 1, got: %d", generation)
	}

	state, _ := coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseDraining {
		t.Errorf("Expected phase draining after start, got: %s", state.Phase)
	}
}

func TestStartRecreate_Error(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.incrementGenErr = errors.New("database error")
	coordinator, _ := NewProtocolCoordinator(persistence)

	ctx := context.Background()
	_, err := coordinator.StartRecreate(ctx)
	if err == nil {
		t.Fatal("Expected error when increment fails")
	}
}

func TestTryTransitionToAssigning_WaitsForAllWorkers(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseDraining
	persistence.readyWorkerCount = 1
	persistence.totalActiveWorkerCount = 2

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	transitioned, err := coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if transitioned {
		t.Error("Should not transition when not all workers are ready")
	}

	state, _ := coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseDraining {
		t.Errorf("Expected to remain in draining phase, got: %s", state.Phase)
	}
}

func TestTryTransitionToAssigning_SucceedsWhenAllReady(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseDraining
	persistence.readyWorkerCount = 2
	persistence.totalActiveWorkerCount = 2

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	transitioned, err := coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !transitioned {
		t.Error("Should transition when all workers are ready")
	}

	state, _ := coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseAssigning {
		t.Errorf("Expected assigning phase, got: %s", state.Phase)
	}
}

func TestTryTransitionToAssigning_OnlyFromDrainingPhase(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseIdle
	persistence.readyWorkerCount = 2
	persistence.totalActiveWorkerCount = 2

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	transitioned, err := coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if transitioned {
		t.Error("Should not transition from non-draining phase")
	}
}

func TestPerformShardAssignment_ClearsAndAssigns(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseAssigning

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.PerformShardAssignment(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if !persistence.clearedShardOwnership {
		t.Error("Expected shard ownership to be cleared")
	}

	// Verify assignments were made
	assignments := persistence.shardAssignments["projection-1"]
	if len(assignments) == 0 {
		t.Error("Expected shard assignments to be made")
	}

	// Verify all shards were assigned
	totalAssigned := 0
	for _, shards := range assignments {
		totalAssigned += len(shards)
	}
	if totalAssigned != 4 {
		t.Errorf("Expected 4 shards to be assigned, got: %d", totalAssigned)
	}
}

func TestPerformShardAssignment_RequiresAssigningPhase(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseDraining

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.PerformShardAssignment(ctx)
	if err == nil {
		t.Fatal("Expected error when not in assigning phase")
	}
}

func TestPerformShardAssignment_NoWorkersNoError(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseAssigning
	persistence.activeWorkerIDs = []string{}

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.PerformShardAssignment(ctx)
	if err != nil {
		t.Fatalf("Expected no error with no workers, got: %v", err)
	}
}

func TestPerformShardAssignment_DeterministicDistribution(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseAssigning
	persistence.activeWorkerIDs = []string{"worker-1", "worker-2", "worker-3"}
	persistence.shardCounts["projection-1"] = 9

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.PerformShardAssignment(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify distribution
	assignments := persistence.shardAssignments["projection-1"]

	// Each worker should get 3 shards (9 / 3 = 3)
	for workerID, shards := range assignments {
		if len(shards) != 3 {
			t.Errorf("Worker %s should have 3 shards, got: %d", workerID, len(shards))
		}
	}
}

func TestPerformShardAssignment_MultipleProjections(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseAssigning
	persistence.projectionNames = []string{"projection-1", "projection-2"}
	persistence.shardCounts["projection-1"] = 4
	persistence.shardCounts["projection-2"] = 6

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.PerformShardAssignment(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify both projections have assignments
	if len(persistence.shardAssignments) != 2 {
		t.Errorf("Expected 2 projections to have assignments, got: %d", len(persistence.shardAssignments))
	}

	// Verify projection-1
	total1 := 0
	for _, shards := range persistence.shardAssignments["projection-1"] {
		total1 += len(shards)
	}
	if total1 != 4 {
		t.Errorf("Expected 4 shards for projection-1, got: %d", total1)
	}

	// Verify projection-2
	total2 := 0
	for _, shards := range persistence.shardAssignments["projection-2"] {
		total2 += len(shards)
	}
	if total2 != 6 {
		t.Errorf("Expected 6 shards for projection-2, got: %d", total2)
	}
}

func TestTransitionToRunning_RequiresAssigningPhase(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseDraining

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.TransitionToRunning(ctx)
	if err == nil {
		t.Fatal("Expected error when not in assigning phase")
	}
}

func TestTransitionToRunning_Success(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseAssigning

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.TransitionToRunning(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	state, _ := coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseRunning {
		t.Errorf("Expected running phase, got: %s", state.Phase)
	}
}

func TestTransitionToIdle_RequiresRunningPhase(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseAssigning

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.TransitionToIdle(ctx)
	if err == nil {
		t.Fatal("Expected error when not in running phase")
	}
}

func TestTransitionToIdle_Success(t *testing.T) {
	persistence := newMockProtocolPersistence()
	persistence.state.Phase = RecreatePhaseRunning

	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	err := coordinator.TransitionToIdle(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	state, _ := coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseIdle {
		t.Errorf("Expected idle phase, got: %s", state.Phase)
	}
}

func TestDistributeShards_EvenDistribution(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, _ := NewProtocolCoordinator(persistence)

	workers := []string{"worker-1", "worker-2", "worker-3"}
	assignments := coordinator.distributeShards(9, workers)

	// Each worker should get 3 shards
	for workerID, shards := range assignments {
		if len(shards) != 3 {
			t.Errorf("Worker %s should have 3 shards, got: %d", workerID, len(shards))
		}
	}

	// Verify all shards are assigned
	allShards := make(map[int]bool)
	for _, shards := range assignments {
		for _, shardID := range shards {
			if allShards[shardID] {
				t.Errorf("Shard %d assigned multiple times", shardID)
			}
			allShards[shardID] = true
		}
	}

	if len(allShards) != 9 {
		t.Errorf("Expected 9 unique shards, got: %d", len(allShards))
	}
}

func TestDistributeShards_UnevenDistribution(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, _ := NewProtocolCoordinator(persistence)

	workers := []string{"worker-1", "worker-2", "worker-3"}
	assignments := coordinator.distributeShards(10, workers)

	// Total shards assigned
	totalShards := 0
	for _, shards := range assignments {
		totalShards += len(shards)
	}

	if totalShards != 10 {
		t.Errorf("Expected 10 total shards, got: %d", totalShards)
	}

	// First worker should get 4, others get 3 (due to round-robin)
	if len(assignments["worker-1"]) != 4 {
		t.Errorf("Worker-1 should have 4 shards, got: %d", len(assignments["worker-1"]))
	}
}

func TestDistributeShards_SingleWorker(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, _ := NewProtocolCoordinator(persistence)

	workers := []string{"worker-1"}
	assignments := coordinator.distributeShards(10, workers)

	if len(assignments["worker-1"]) != 10 {
		t.Errorf("Worker-1 should have all 10 shards, got: %d", len(assignments["worker-1"]))
	}
}

func TestFullRecreateProtocolFlow(t *testing.T) {
	persistence := newMockProtocolPersistence()
	coordinator, _ := NewProtocolCoordinator(persistence)
	ctx := context.Background()

	// 1. Start recreate (idle -> draining)
	generation, err := coordinator.StartRecreate(ctx)
	if err != nil {
		t.Fatalf("Failed to start recreate: %v", err)
	}
	if generation != 1 {
		t.Errorf("Expected generation 1, got: %d", generation)
	}

	state, _ := coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseDraining {
		t.Fatalf("Expected draining phase, got: %s", state.Phase)
	}

	// 2. Try to transition to assigning (should fail, workers not ready)
	persistence.readyWorkerCount = 1
	persistence.totalActiveWorkerCount = 2
	transitioned, err := coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if transitioned {
		t.Error("Should not transition when workers not ready")
	}

	// 3. All workers ready, transition to assigning
	persistence.readyWorkerCount = 2
	transitioned, err = coordinator.TryTransitionToAssigning(ctx)
	if err != nil {
		t.Fatalf("Failed to transition to assigning: %v", err)
	}
	if !transitioned {
		t.Error("Should transition when all workers ready")
	}

	state, _ = coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseAssigning {
		t.Fatalf("Expected assigning phase, got: %s", state.Phase)
	}

	// 4. Perform shard assignment
	err = coordinator.PerformShardAssignment(ctx)
	if err != nil {
		t.Fatalf("Failed to perform shard assignment: %v", err)
	}

	if !persistence.clearedShardOwnership {
		t.Error("Shard ownership should be cleared")
	}

	// 5. Transition to running
	err = coordinator.TransitionToRunning(ctx)
	if err != nil {
		t.Fatalf("Failed to transition to running: %v", err)
	}

	state, _ = coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseRunning {
		t.Fatalf("Expected running phase, got: %s", state.Phase)
	}

	// 6. Transition to idle
	err = coordinator.TransitionToIdle(ctx)
	if err != nil {
		t.Fatalf("Failed to transition to idle: %v", err)
	}

	state, _ = coordinator.GetCurrentState(ctx)
	if state.Phase != RecreatePhaseIdle {
		t.Fatalf("Expected idle phase, got: %s", state.Phase)
	}
	if state.Generation != 1 {
		t.Errorf("Generation should remain 1, got: %d", state.Generation)
	}
}
