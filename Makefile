GO ?= go
GOLANGCI_LINT ?= golangci-lint
PROJECT_GO_VERSION := $(shell awk '/^go / { print $$2; exit }' go.mod)
PROJECT_TOOLCHAIN ?= go$(PROJECT_GO_VERSION)

.PHONY: help format fmt format-check fmt-check mod-verify vet lint test test-race integration-test verify verify-all

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

format: ## Format all Go files
	$(GO) fmt ./...

fmt: format ## Alias for format

format-check: ## Fail if any Go files need formatting
	@unformatted_files="$$(gofmt -l .)"; \
	if [ -n "$$unformatted_files" ]; then \
		echo "The following files need gofmt:"; \
		echo "$$unformatted_files"; \
		exit 1; \
	fi

fmt-check: format-check ## Alias for format-check

mod-verify: ## Verify downloaded module contents
	$(GO) mod verify

vet: ## Run Go's static analyzer
	$(GO) vet ./...

lint: ## Run golangci-lint, including integration-tagged code
	@command -v $(GOLANGCI_LINT) >/dev/null 2>&1 || { \
		echo "golangci-lint is not installed: https://golangci-lint.run/docs/welcome/install/"; \
		exit 1; \
	}
	GOTOOLCHAIN=$(PROJECT_TOOLCHAIN) $(GOLANGCI_LINT) run --build-tags=integration ./...

test: ## Run unit tests
	$(GO) test ./...

test-race: ## Run unit tests with race detection and coverage
	$(GO) test -race -coverprofile=coverage.out ./...

integration-test: ## Run Docker-backed integration tests
	$(GO) test -race -tags=integration -timeout=5m ./integration_tests/...

verify: format-check mod-verify vet lint test-race ## Run all checks except integration tests

verify-all: verify integration-test ## Run every local and CI check
