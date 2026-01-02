# pupsourcing-orchestrator

Scalable orchestrator for projection processes

## Prerequisites

- Go 1.24 or later

## Project Structure

```
.
├── cmd/                    # Main applications
│   └── orchestrator/       # Orchestrator application entry point
├── internal/               # Private application code
│   └── orchestrator/       # Orchestrator business logic
├── pkg/                    # Public library code
│   └── version/            # Version information
├── Makefile                # Build and CI automation
├── .golangci.yml           # Linter configuration
└── go.mod                  # Go module definition
```

## Getting Started

### Build

```bash
make build
```

The binary will be built to `bin/orchestrator`.

### Run

```bash
./bin/orchestrator
```

### Development

#### Install Development Tools

```bash
make install-tools
```

This installs:
- golangci-lint (linter)

#### Format Code

```bash
make fmt
```

#### Run Linter

```bash
make lint
```

#### Run Go Vet

```bash
make vet
```

#### Run Tests

```bash
make test
```

#### Run All CI Checks

```bash
make ci
```

This runs: format, vet, lint, and test.

#### Clean Build Artifacts

```bash
make clean
```

## Makefile Targets

Run `make help` to see all available targets:

```bash
make help
```

