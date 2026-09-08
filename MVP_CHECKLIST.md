# Dev Environment MVP Checklist

## Goal

Run and observe this pipeline in a development environment:

```text
Kafka -> decode -> persist raw event -> acknowledge offset
      -> watermark -> create sessions and checkpoint transactionally
```

The first deployment does not need every production feature. It should process
realistic events reliably, shut down cleanly, and expose enough information to
understand what the pipeline is doing.

## Required before deployment

### 1. Fix and test the Kafka JSON contract

Kafka events use snake-case JSON fields. `event_ingestion.VmEvent` now uses
explicit mappings:

```go
type VmEvent struct {
    EventId    string    `json:"event_id"`
    Flavour    string    `json:"flavor"`
    InstanceId string    `json:"instance_id"`
    OccurredAt time.Time `json:"occurred_at"`
    ProjectId  string    `json:"project_id"`
    EventType  string    `json:"type"`
}
```

The encoder tests decode a literal external JSON payload in addition to testing
a round trip.

Required fields are validated, so valid JSON such as `{}` is reported and
skipped according to the malformed-message policy.

### 2. Add runtime configuration

Load and validate at least these environment variables:

- `KAFKA_BROKERS`
- `KAFKA_GROUP`
- `KAFKA_TOPIC`
- `DB_URL`
- `MAX_OUT_OF_ORDERNESS`
- Optionally, `LOG_LEVEL`

Environment variables are sufficient for the MVP. A configuration framework is
not required. Startup should fail with a clear error if required configuration
is absent or invalid.

### 3. Handle operating-system shutdown

Use a signal-aware context so `SIGTERM` and Ctrl+C stop Kafka polling, retry
waits, and database work:

```go
ctx, stop := signal.NotifyContext(
    context.Background(),
    os.Interrupt,
    syscall.SIGTERM,
)
defer stop()
```

### 4. Decide terminal worker behavior

Handler retries currently stop after two minutes. The error then propagates:

```text
KafkaClient.Listen -> Ingestor -> Sessionizer.Run -> main
```

Because this executable currently only runs the pipeline, the simplest MVP
policy is to log the terminal error, exit non-zero, and let the deployment
restart it.

Use a `run(ctx) error` function so resource-owning defers execute before
`os.Exit(1)`:

```go
func main() {
    ctx, stop := signal.NotifyContext(...)
    defer stop()

    if err := run(ctx); err != nil {
        slog.Error("application stopped", slog.Any("error", err))
        os.Exit(1)
    }
}
```

If the application later provides unrelated functionality, supervise and
restart only the ingestion worker instead of terminating the process.

### 5. Apply database migrations

The SQL exists in `migrations/000001_init.up.sql`. The local Compose deployment
runs it through a one-shot `migrate/migrate` service after PostgreSQL is healthy.
For other environments, choose one of:

- A migration job or init container.
- A migration command in the deployment pipeline.
- A documented manual command for the first dev environment.

Avoid having every application replica execute raw migration files at startup
unless a migration tool supplies versioning and locking.

### 6. Provision the Kafka topic

The local Compose deployment creates `billing.vm.events` through an idempotent
one-shot `kafka-init` service and disables automatic topic creation. For other
environments, either:

- Provision the topic separately, preferably; or
- Temporarily enable Kafka auto-topic creation in the dev environment.

One partition is sufficient for the first observable deployment.

### 7. Package and deploy the application

The local environment now includes:

- A multi-stage `Dockerfile`.
- Environment-variable wiring.
- A deployment definition for the dev environment.
- Migration instructions or a migration job.
- Kafka topic provisioning instructions.

Docker Compose is useful for a complete local environment, but is optional if
the dev environment already provides Kafka and Postgres.

## Strongly recommended for observability

### Startup connectivity

Call `db.PingContext` with a startup timeout. `sql.Open` does not establish a
connection, so invalid database configuration otherwise appears only during
processing.

Consider a Kafka connectivity check as part of startup or readiness as well.

### Structured logs

Log important lifecycle events:

- Application starting and stopping.
- Database connection established.
- Kafka consumer starting and stopping.
- Event persistence failure and retry.
- Watermark advancement.
- Session creation.
- Terminal consumer failure.

Include useful identifiers where available:

- `event_id`
- `instance_id`
- Kafka topic, partition and offset
- Watermark
- Session ID

Successful per-event logs can use debug level to avoid excessive production
logging.

### Health endpoints

A health endpoint is not strictly required for the first run. If the deployment
platform expects probes, expose:

- `/healthz`: the process is alive.
- `/readyz`: startup completed and the ingestion worker has not terminated.

Metrics and dashboards can be added after logs and database inspection prove
insufficient.

## Tests to add

Added for the local deployment:

- [x] Literal snake-case Kafka JSON decoding.
- [x] Missing required event-field validation.
- [x] Configuration parsing and validation.

Existing coverage already includes:

- Producing and consuming Kafka events.
- JSON round trips and malformed JSON.
- Handler retry ordering.
- Later records waiting for an earlier failed record.
- Committed events not being replayed.
- Uncommitted events being replayed after restart.
- Retry cancellation leaving the event replayable.
- Malformed records being committed and skipped.
- Terminal Kafka and ingestion error propagation.
- Sessionizer cancellation of ingestion on failure.
- Postgres transaction commit and rollback.
- Race-enabled unit and integration tests.

## Documentation to update

The README still describes the original JSONL billing program. Update it with:

- Current Kafka/Postgres architecture.
- Required environment variables.
- Local startup instructions.
- Migration and topic provisioning commands.
- Build and deployment instructions.
- A sample event publishing command.
- SQL queries for inspecting raw events, sessions, and checkpoints.
- Delivery guarantee: at least once with idempotent raw-event insertion.
- Current malformed-message policy: report, commit, and skip.
- Known limitations.

## Explicitly deferred

These are not MVP blockers:

- Dead-letter topic.
- Per-partition retry workers.
- Advanced rebalance handling.
- Kafka transactions or exactly-once delivery.
- Project and flavor fields on sessions.
- Billing calculation from persisted sessions.
- HTTP API.
- Prometheus dashboards.
- Further unit-of-work abstraction.
- Sophisticated configuration frameworks.
- Multiple Kafka partitions and horizontal scaling.
- Automated reconciliation.

## Recommended implementation order

1. Fix JSON tags and event validation.
2. Add environment configuration and validation.
3. Add signal handling and non-zero terminal exit.
4. Add startup database connectivity checking.
5. Add a Dockerfile.
6. Document and apply migrations.
7. Provision one Kafka topic.
8. Publish sample events.
9. Observe `vm_events`, `sessions`, and `watermark_checkpoints`.
