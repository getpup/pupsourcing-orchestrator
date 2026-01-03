# Copilot Instructions

## Project Overview

pupsourcing-orchestrator is a companion library to [pupsourcing](https://github.com/getpup/pupsourcing) responsible for 
running, supervising, and scaling projections in an event-sourced system.

All tasks should align with the DEVELOPMENT_PLAN.md document. Always read the DEVELOPMENT_PLAN.md before starting work 
on a task to understand the scope better.

Since pupsourcing-orchestrator depends on https://github.com/getpup/pupsourcing, always study the core library before
proceeding with work on the orchestrator tasks.

## Testing

### Test Execution and Coverage

- Run tests with `make test`
- Use testify for assertions
- Test coverage requirements: aim for 100% for files, packages, and total (see `.testcoverage.yml` when implemented)

### Test Structure and Organization

- **Use `t.Run` for named test cases** - Group related test scenarios with descriptive names
  ```go
  func TestUserCreation(t *testing.T) {
      t.Run("creates user successfully", func(t *testing.T) {
          // test implementation
      })
      
      t.Run("returns error when email is invalid", func(t *testing.T) {
          // test implementation
      })
  }
  ```

- **One test function per behavior** - Test functions should focus on a single unit of behavior
- **Descriptive test names** - Test function names should clearly describe what is being tested using the pattern `TestComponentName_Behavior`

### Test Code Style

- **Code duplication is acceptable in tests** - Prioritize clarity and readability over DRY principles
    - Each test should be self-contained and easy to understand in isolation
    - Avoid helper functions that obscure test logic unless they significantly improve readability
    - Duplicating setup code across tests is preferred over complex shared setup that makes tests harder to understand

- **Avoid logic in tests** - Tests should be straightforward: setup, execute, assert
    - ❌ No loops (for, while, range)
    - ❌ No conditional statements (if, else, switch)
    - ❌ No complex data transformations
    - ✅ Simple variable assignments
    - ✅ Direct function calls
    - ✅ Clear assertions

  ```go
  // ❌ Bad: Logic in test
  func TestMultipleUsers(t *testing.T) {
      for i := 0; i < 5; i++ {
          user := createUser(i)
          if user.IsAdmin() {
              assert.True(t, user.HasPermission("admin"))
          }
      }
  }
  
  // ✅ Good: Explicit test cases
  func TestUserPermissions(t *testing.T) {
      t.Run("admin user has admin permission", func(t *testing.T) {
          user := User{Role: "admin"}
          assert.True(t, user.HasPermission("admin"))
      })
      
      t.Run("regular user lacks admin permission", func(t *testing.T) {
          user := User{Role: "user"}
          assert.False(t, user.HasPermission("admin"))
      })
  }
  ```

### Test Isolation and Dependencies

- **No external dependencies** - Unit tests must not require external systems to be running
    - ❌ No databases (PostgreSQL, MySQL, etc.)
    - ❌ No message queues (RabbitMQ, Kafka, etc.)
    - ❌ No external APIs or web services
    - ❌ No file system operations (unless testing file system code specifically)

- **Use mocks and stubs** - Replace external dependencies with test doubles

- **Rare exceptions** - In-memory databases like SQLite may be used as a last resort
    - Only when testing code that is tightly coupled to a 3rd party library requiring a real database connection
    - Document why the external dependency is necessary
    - Prefer refactoring to eliminate the dependency over using in-memory databases

### Test Pattern

A well-structured unit test follows this pattern:

```go
func TestComponentName_Behavior(t *testing.T) {
    t.Run("scenario description", func(t *testing.T) {
        // Arrange: Set up test data and mocks
        input := "test-input"
        expected := "expected-output"
        
        // Act: Execute the behavior being tested
        actual := FunctionUnderTest(input)
        
        // Assert: Verify the outcome
        assert.Equal(t, expected, actual)
    })
}
```

### Integration Tests

Integration tests are organized in a **shared `integration_test/` directory** at the repository root. This structure is chosen because:
- The orchestrator's integration tests span multiple packages and require coordination between them
- Shared infrastructure setup (databases, message brokers) is needed across integration tests
- Integration tests verify the entire orchestration workflow, not just individual packages

Integration test organization:
- Place integration tests in `/integration_test/` directory
- Use build tags to separate integration from unit tests: `//go:build integration`
- Run with: `go test -tags=integration ./integration_test/...`
- Integration tests may use real external dependencies (databases, etc.)
- Document required infrastructure in integration test README

## Dependencies

- This project depends on [github.com/getpup/pupsourcing](https://github.com/getpup/pupsourcing)
- Always use types and interfaces from pupsourcing where appropriate
- Do not redefine interfaces that already exist in pupsourcing

## Logging

- Do not use the standard library `log` package for logging
- Inject the `github.com/getpup/pupsourcing/es.Logger` interface for observability
- Use structured logging with key-value pairs: `logger.Info(ctx, "message", "key", value)`

## Code Style

- Follow standard Go conventions
- Use golangci-lint for linting
- All public APIs should have documentation comments
- Use functional options pattern for configuration
