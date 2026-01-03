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

Or directly with go test:
```bash
go test -v -tags=integration ./integration_test/...
go test -v -tags=integration ./store/postgres/integration_test/...
```

## CI

Integration tests run in CI with a PostgreSQL service container.
