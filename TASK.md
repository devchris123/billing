# Coding task: usage metering for an IaaS API

Our IaaS API emits an event every time a virtual machine (an "instance")
starts or stops. A billing service consumes these events from a message
queue and turns them into money. Your task is a small version of that
consumer.

You get `events.jsonl` - one JSON event per line, roughly what one month of
events looks like after being dumped from the queue. The queue delivers
at-least-once and in no particular order, and one export job had a bug, so
the file contains duplicates, out-of-order events and a few broken lines.
Your program reads the file and prints a billing summary as JSON to stdout.

Time box: 2 hours. Any programming language - use whatever you are fastest
in. Standard library preferred, small helper libraries are fine. If you hit
the 2 hours, stop and write down where you stopped.

## The events

```json
{
  "event_id": "79c2d2e4-9ae1-a991-524f-93ff30307633",
  "occurred_at": "2026-07-15T07:03:38Z",
  "project_id": "p-eos",
  "instance_id": "i-0079",
  "type": "instance.start",
  "flavor": "s1.medium"
}
```

- `type` is `instance.start` or `instance.stop`.
- All timestamps are UTC, format as shown.
- `project_id` and `flavor` are on every event and never change for a given
  instance.
- After removing duplicates and ordering by time, an instance's events
  alternate start/stop. Exception: stops for instances that never started
  do occur (see below).

## Billing rules

The billing period is July 2026: from `2026-07-01T00:00:00Z` (inclusive) to
`2026-08-01T00:00:00Z` (exclusive). All events fall within this period.

- A session runs from a start event to the matching stop event. Every
  started hour is billed: a 25-minute session is 1 hour, 2.5 hours are 3
  hours.
- An instance with a start but no stop is still running: bill it until the
  end of the billing period.
- Prices are euro cents per started hour. Report all costs in cents as
  integers - we don't do float money.

| flavor    | cents/hour |
|-----------|-----------:|
| s1.small  |          5 |
| s1.medium |         10 |
| s1.large  |         20 |
| g1.xlarge |        150 |

Worked example: `s1.medium`, start 10:00, stop 12:30 - that's 3 billed
hours, 30 cents.

Dirty data:

- Duplicates: two lines with the same `event_id` are the same event. Count
  it once.
- Stops for an instance that never started: don't bill them, count them.
- Lines that are not valid events (broken JSON, missing fields, bad
  timestamps, unknown types): don't crash, count them.

## Output

Print JSON to stdout, roughly this shape:

```json
{
  "period": {"start": "2026-07-01T00:00:00Z", "end": "2026-08-01T00:00:00Z"},
  "projects": {
    "p-example": {
      "billed_hours": {"s1.small": 100, "s1.medium": 50},
      "cost_cents": 1000
    }
  },
  "total_cost_cents": 1000,
  "counters": {
    "invalid_lines": 0,
    "duplicate_events": 0,
    "unmatched_stops": 0
  }
}
```

Exact formatting and key order don't matter as long as it's valid JSON and
the numbers are right.

## What to hand in

- The code.
- A short README: how to run it, assumptions you made, where you stopped if
  you ran out of time.
- In the README, answer this in a few sentences (no code): `events.jsonl`
  fits into memory. What changes in your design when the events arrive
  continuously from a message queue like RabbitMQ and a month of them is
  500 GB?

## What we look at

Correct numbers first. Then: how you handle the dirty data, readable code,
tests where they pay off (we don't need 100% coverage). We don't care about
Docker, config frameworks, fancy CLI parsing or packaging. We'd rather see
clean core logic and an honest "ran out of time for X" than everything
half-done.
