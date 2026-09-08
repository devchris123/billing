# VM event sessionizer

Consumes VM lifecycle events from Kafka, stores the raw events in PostgreSQL,
and derives start/stop sessions as event-time watermarks advance.

```text
Kafka -> decode -> vm_events -> watermark -> sessions
                              \-> watermark_checkpoints
```

## Run the local development stack

The Compose stack contains PostgreSQL, Kafka, one-shot database and topic
initializers, the application, and a development event producer.

```sh
docker compose up --build
```

Startup is ordered as follows:

```text
PostgreSQL healthy -> migrate exits successfully -> application
Kafka healthy     -> kafka-init exits successfully -> application + producer
```

The producer emits valid VM start/stop lifecycles every five seconds. Synthetic
event time advances by one minute per event so the ten-minute watermark delay
can be observed quickly. It stops after 30 minutes.

Run another finite batch when needed:

```sh
docker compose run --rm producer -count=25 -seed=42
```

Stop the stack while retaining PostgreSQL data:

```sh
docker compose down
```

Delete all local state and start from a clean baseline:

```sh
docker compose down -v
```

## Inspect the pipeline

Open an interactive PostgreSQL session:

```sh
docker compose exec postgres psql -U billing -d billing
```

Useful queries:

```sql
SELECT * FROM vm_events ORDER BY occurred_at DESC LIMIT 20;
SELECT * FROM sessions ORDER BY started_at DESC LIMIT 20;
SELECT * FROM session_starts ORDER BY started_at DESC LIMIT 20;
SELECT * FROM session_stops ORDER BY stopped_at DESC LIMIT 20;
SELECT * FROM watermark_checkpoints;
```

Follow application or producer logs:

```sh
docker compose logs -f app
docker compose logs -f producer
```

Docker logs rotate after 10 MB and retain at most three files per service.
Kafka records have a 30-minute retention period in this local stack.

## Event contract

The Kafka topic is `billing.vm.events`. A payload has this shape:

```json
{
  "event_id": "81652602-c7e8-41f8-9f09-af5a23d012d6",
  "flavor": "s1.small",
  "instance_id": "instance-001",
  "occurred_at": "2026-09-08T18:30:00Z",
  "project_id": "project-a",
  "type": "instance.start"
}
```

All fields are required. Malformed or incomplete messages are reported,
committed, and skipped until dead-letter handling is implemented.

## Runtime configuration

CLI flags override environment variables, which override built-in defaults.

| Environment variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka brokers |
| `KAFKA_GROUP` | `billing.sessionizer` | Consumer group |
| `KAFKA_TOPIC` | `billing.vm.events` | Input topic |
| `DB_URL` | none | PostgreSQL connection URL; required |
| `MAX_OUT_OF_ORDERNESS` | `10m` | Maximum tolerated event-time delay |

See the available flags with:

```sh
go run . -help
```

## Migrations and topic provisioning

Compose runs `migrate/migrate:v4.19.1` against the committed SQL files in
`migrations/`. To rerun the migration job manually:

```sh
docker compose run --rm migrate
```

The `kafka-init` service explicitly creates one local partition with replication
factor one. Kafka auto-topic creation is disabled so misspelled topics fail
instead of silently creating another topic.

## Delivery behavior

Kafka auto-commit is disabled. A record is committed after raw event persistence
and delivery of its ingestion result to the sessionizer. Failed ingestion
handlers are retried for a bounded period; a terminal failure exits the
application so Compose or the deployment platform can restart it. Raw insertion
is idempotent by `event_id`, resulting in at-least-once delivery with deduplicated
raw storage.

The session and watermark checkpoint changes share one PostgreSQL transaction.
Watermarks that do not advance the persisted processing position are ignored.

## Verification

```sh
make verify
make integration-test
```

## Current limitations

- One Kafka partition and one local broker.
- No dead-letter topic for malformed events.
- The ingestor's highest observed event time is in memory and resets on restart.
- No project/flavor fields or billing calculation on persisted sessions yet.
- No health HTTP endpoints or metrics.
- No automated reconciliation workflow.
