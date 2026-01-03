# Integration Tests

## Prerequisites

- PostgreSQL 12+ running locally or accessible
- Database created for testing

## Environment Variables

- DATABASE_URL: PostgreSQL connection string
  Example: "host=localhost port=5432 user=postgres password=postgres dbname=orchestrator_test sslmode=disable"

## Running Tests

Run integration tests only:
```bash
make test-integration
```

Or directly with go test (note: use `-p=1` to run tests sequentially):
```bash
go test -v -p=1 -tags=integration ./integration_test/...
go test -v -p=1 -tags=integration ./store/postgres/integration_test/...
```

**Note:** Integration tests must run sequentially (not in parallel) as they share database resources. The `-p=1` flag ensures only one test package runs at a time.

## CI

Integration tests run in CI with a PostgreSQL service container.
