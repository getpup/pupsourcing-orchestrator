.PHONY: all build test clean fmt vet lint install-tools help

# Build variables
BINARY_NAME=orchestrator
BUILD_DIR=bin
CMD_DIR=cmd/orchestrator

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

all: fmt vet lint test build ## Run all checks and build

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build the application
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) ./$(CMD_DIR)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

test: ## Run tests
	@echo "Running tests..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	@echo "Tests complete"

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	$(GOTEST) -v -p=1 -tags=integration ./integration_test/... ./store/postgres/integration_test/...
	@echo "Integration tests complete"

clean: ## Clean build artifacts
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out
	@echo "Clean complete"

fmt: ## Format code with gofmt
	@echo "Formatting code..."
	$(GOFMT) -s -w .
	@echo "Format complete"

vet: ## Run go vet
	@echo "Running go vet..."
	$(GOVET) ./...
	@echo "Vet complete"

lint: ## Run golangci-lint
	@echo "Running golangci-lint..."
	@test -f $(shell go env GOPATH)/bin/golangci-lint || (echo "golangci-lint not found. Run 'make install-tools' to install it." && exit 1)
	@PATH="$(PATH):$(shell go env GOPATH)/bin" golangci-lint run ./...
	@echo "Lint complete"

install-tools: ## Install development tools
	@echo "Installing development tools..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin latest)
	@echo "Tools installed"

mod-tidy: ## Tidy go.mod
	@echo "Tidying go.mod..."
	$(GOMOD) tidy
	@echo "Tidy complete"

mod-download: ## Download dependencies
	@echo "Downloading dependencies..."
	$(GOMOD) download
	@echo "Download complete"

ci: fmt vet lint test ## Run all CI checks (format, vet, lint, test)
	@echo "All CI checks passed!"
