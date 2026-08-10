# Usage metering

Reads `events.jsonl`, prints a July 2026 billing summary as JSON to stdout.

## Run

```sh
go run .                  # defaults to events.jsonl
go run . other.jsonl
go test ./...
```

## Design

```
lines → parse + validate + dedupe → group by instance → sort each group by time
      → pair into sessions → billed hours per (project, flavor) → cost
```

key decisions:
- parser -> owns the dirty data and the counters; 
- summarize -> pure and takes any input order. 
- summarize groups by instance, sorts by time, and pairs starts with stops.


## Assumptions

- Assumptions from TASK.md
- A start while a session is already open is a redelivery; the earlier start
  wins.
-  Blank lines are skipped silently.
- No instance has two events at the same timestamp in this file (checked), so
  the sort needs no tie-break. If ties ever occur the order is undefined,
  which can turn a zero-length session into a month-long one — a deliberate
  gap, not an oversight.

Result: `total_cost_cents: 279520`, with 4 invalid lines, 25 duplicates and
2 unmatched stops. Cross-checked against an independent implementation.

## 500 GB from a queue

- events []VMEvent — 2.8 billion structs. Dead immediately.
- Sorting. Sorting needs the whole dataset. At 500 GB you'd need external merge sort, and you still couldn't do it on a live stream because the data never ends.

## Where I stopped

Within the time box. `parse` has no dedicated test — the parsing rules are
covered only indirectly through the end-to-end run. The tie-break gap above
is known and unhandled.
