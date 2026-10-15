# Ledger

Per-request cost accounting for inference calls. Every call gets a cost line — tokens in/out, model, node, timestamp — appended to an append-only JSONL file. Rollups per tenant, day, or model; CSV export for billing.

## Why

Inference cost is only visible after the bill arrives. Ledger makes it a first-class record at request time: one line per call, cheap to append, trivial to aggregate. No database, no daemon — a JSONL file and stdlib.

## Architecture

```
record(tenant, model, node, tokens_in, tokens_out)   → append-only JSONL
rollup(by='tenant' | 'day' | 'model')                → dict of token sums
export(path)                                         → CSV
```

- **Append-only JSONL** — one line per request, crash-safe, greppable.
- **Thread-safe** — a lock guards appends so concurrent request paths can share one ledger.
- **Rollups** — group by tenant (chargeback), day (trend), or model (unit economics).
- **Reload** — point a new `Ledger` at an existing file and it picks up where it left off.

## Usage

```python
from ledger import Ledger

led = Ledger("ledger.jsonl")
led.record("tenant-a", "glm-5.3-flash", "spark-head", 120, 340)

led.rollup(by="tenant")   # {'tenant-a': 460}
led.rollup(by="day")      # {'2026-10-14': 460}
led.export("costs.csv")
```

## Results

- 10 tests, all passing (rollups, export, reload, thread-safety, input validation).
- Pure Python stdlib + pytest. No network in CI.

## Scope

Read-only with respect to any serving stack — Ledger only observes and records. Sample tenants are generic (`tenant-a`, `tenant-b`); contact: ops@techguard.io.
