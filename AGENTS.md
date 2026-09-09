# Repository Guidelines

## Project Structure & Module Organization

The root package contains the CLI entry point (`main.go`, `cli.go`) and billing-summary logic. Domain code is grouped by responsibility: `event_client/` consumes Kafka records, `session/` derives VM sessions, `store/` provides PostgreSQL repositories, and `concurrency/` contains channel helpers. The development producer lives in `cmd/event-producer/`. Database changes belong in paired files under `migrations/`. Docker-backed tests are in `integration_tests/`; other tests sit beside their implementation as `*_test.go`. Observability configuration is under `dev/observability/`.

## Build, Test, and Development Commands

- `go run . -help` lists runtime flags; set `DB_URL` to run the consumer.
- `docker compose up --build` starts PostgreSQL, Kafka, migrations, the app, producer, and observability services.
- `make test` runs all unit tests.
- `make test-race` runs unit tests with race detection and writes `coverage.out`.
- `make integration-test` runs integration-tagged tests against Docker dependencies.
- `make verify` runs formatting checks, module verification, `go vet`, `golangci-lint`, and race-enabled tests.
- `make verify-all` adds integration tests to the verification suite.

## Coding Style & Naming Conventions

Use `make format` and the tabs emitted by `gofmt`. Keep package names short and lowercase; use `MixedCaps` for Go identifiers and snake-case SQL migration names such as `000002_add_index.up.sql`. Keep transport, persistence, and sessionization concerns in their existing packages. Wrap errors with operation context, and pass `context.Context` through I/O boundaries. Run `make lint` before submitting; it includes integration-tagged code.

## Testing Guidelines

Tests use Go's `testing` package with `testify`. Name tests `TestBehavior` and favor table-driven cases for validation, ordering, retries, and session boundaries. Add unit tests beside changed code. Put tests requiring PostgreSQL or Kafka in `integration_tests/` with the `integration` build tag. There is no fixed coverage threshold; exercise new behavior and regressions.

## Commit & Pull Request Guidelines

Recent commits use concise, imperative, sentence-case subjects, for example `Add CLI arg support and ENV support`. Keep each commit focused. Pull requests should explain the change, identify migration or configuration impacts, link the issue, and report commands run (ideally `make verify-all`). Include screenshots for Grafana changes and sample logs or JSON for observable pipeline changes.

## Configuration & Security

Never commit credentials or production connection strings. Local defaults in `compose.yaml` are development-only. Document new environment variables in `README.md`, and make schema changes through migrations rather than manual database edits.
