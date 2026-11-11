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

## Rust port

`src/lib.rs` is a dependency-free Rust port of the same ledger (Python stays as the reference). Same line format, same semantics:

```rust
use ledger::{Ledger, By};

let led = Ledger::new(Some(std::path::Path::new("ledger.jsonl")))?;
led.record("tenant-a", "glm-5.3-flash", "spark-head", 120, 340, None)?;
led.rollup(By::Tenant)?;   // {"tenant-a": 460}
led.export(std::path::Path::new("costs.csv"))?;
```

- No external crates — std only (`Mutex` for thread-safety, hand-rolled UTC timestamp).
- Same JSONL line format as the Python version, so files are interchangeable.
- `cargo test` runs the same 10 scenarios as the Python suite.

## Results

- Python: 10 tests, all passing (rollups, export, reload, thread-safety, input validation).
- Rust: 10 tests, all passing (`cargo test`) — same scenarios ported 1:1.
- Pure Python stdlib + pytest; Rust std only. No network in CI.

## Scope

Read-only with respect to any serving stack — Ledger only observes and records. Sample tenants are generic (`tenant-a`, `tenant-b`); contact: ops@techguard.io.

## Docker

Multi-stage build on `python:3.12-slim`, non-root, multi-arch (amd64 + arm64):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t <name> .
```

The image imports the library as a smoke check on run. For repos with a Rust
port (Ledger, Quota, Blackbox), pass `--build-arg BUILD_RUST=1` to also build
the Rust artifact in a `rust:1.83-slim` stage.

## Frontier Benchmark (fleet, 5 nodes)

Clumped all-node benchmark: every fleet machine ran the identical workload simultaneously for 5s x 4 threads — **append-only JSONL cost-line writes (disk-bound)**.

**Combined fleet throughput: 173,360 ops/sec.**

| Node | ops/sec |
|---|---|
| spark-58c4 | 27,338 |
| spark-ebbf | 22,262 |
| mini2 | 40,872 |
| studio | 41,841 |
| mini4 | 41,048 |

*Proof: identical harness (`frontier.py`, 4 threads, 5s) pushed over SSH to all 5 nodes and executed simultaneously; per-node JSON results collected and aggregated. Nodes: spark-58c4 (.182), spark-ebbf (.198), mini2 (.157), Mac Studio (.11), mini4 (.169). Run 2026-11-14.*
