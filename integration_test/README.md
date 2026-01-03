# Integration Tests

This directory contains end-to-end integration tests for the pupsourcing-orchestrator.

## Overview

These tests verify the complete orchestration workflow including:
- Worker coordination and partition assignment
- Event processing with real PostgreSQL
- Generation management (workers joining/leaving)
- Stale worker detection and cleanup
- Graceful shutdown handling
- Recovery after restart
- Multiple replica sets independence

## Running Tests

### Prerequisites

You need a running PostgreSQL instance. You can use Docker:

```bash
docker run -d \
  --name postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=orchestrator_test \
  -p 5432:5432 \
  postgres:15-alpine
```

### Environment Variables

Set the database connection:

```bash
export DATABASE_URL="host=localhost port=5432 user=postgres password=postgres dbname=orchestrator_test sslmode=disable"
```

Or use individual variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=orchestrator_test
```

### Running Tests

Run all integration tests:

```bash
make test-integration
```

Or run specific tests:

```bash
go test -v -tags=integration ./integration_test/... -run TestSingleWorkerProcessesEvents
```

Run with race detector:

```bash
go test -v -race -tags=integration ./integration_test/...
```

## Test Structure

Each test follows this pattern:

1. **Setup**: Create database tables, event store, and generation store
2. **Execute**: Start orchestrators and process events
3. **Verify**: Assert expected behavior
4. **Cleanup**: Clean up database state

## Test Cases

- **TestSingleWorkerProcessesEvents**: Single worker processes all events
- **TestTwoWorkersPartitionEvents**: Two workers partition events without duplicates
- **TestThirdWorkerJoinsReconfiguration**: Third worker joins, all reconfigure
- **TestWorkerLeavesReconfiguration**: Worker leaves, remaining workers reconfigure
- **TestStaleWorkerCleanup**: Stale worker detection and cleanup
- **TestGracefulShutdownDuringCoordination**: Shutdown during coordination phase
- **TestGracefulShutdownDuringExecution**: Shutdown during event processing
- **TestMultipleReplicaSetsIndependent**: Multiple replica sets operate independently
- **TestProjectionErrorHandling**: Projection errors are handled correctly
- **TestRecoveryAfterRestart**: Orchestrator resumes from checkpoint after restart

## Notes

- Tests run sequentially (not in parallel) to avoid database conflicts
- Each test uses unique replica set names to prevent interference
- Tests use shorter timeouts than production for faster feedback
- Tests clean up after themselves but may leave data on failure
