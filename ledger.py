"""Ledger — per-request cost accounting for inference calls.

Every inference call gets a cost line: tokens in/out, model, node, timestamp.
Rollups per tenant/day/model. Append-only JSONL storage.
"""

from __future__ import annotations

import csv
import os
import threading
from collections import defaultdict
from datetime import datetime, timezone


class Ledger:
    """Append-only cost ledger with rollups and CSV export."""

    def __init__(self, path: str | None = None):
        self.path = path
        self._lock = threading.Lock()
        self._lines: list[dict] = []
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._lines.append(_decode(line))

    def record(
        self,
        tenant: str,
        model: str,
        node: str,
        tokens_in: int,
        tokens_out: int,
        ts: str | None = None,
    ) -> dict:
        """Append one cost line. ts defaults to now (UTC, ISO-8601)."""
        if not tenant or not model or not node:
            raise ValueError("tenant, model and node are required")
        if tokens_in < 0 or tokens_out < 0:
            raise ValueError("token counts must be non-negative")
        if ts is None:
            ts = datetime.now(timezone.utc).isoformat()
        entry = {
            "tenant": tenant,
            "model": model,
            "node": node,
            "tokens_in": int(tokens_in),
            "tokens_out": int(tokens_out),
            "ts": ts,
        }
        with self._lock:
            self._lines.append(entry)
            if self.path:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(_encode(entry) + "\n")
        return entry

    def rollup(self, by: str = "tenant") -> dict[str, int]:
        """Sum tokens_in+tokens_out grouped by 'tenant', 'day' or 'model'."""
        if by not in ("tenant", "day", "model"):
            raise ValueError("by must be 'tenant', 'day' or 'model'")
        sums: dict[str, int] = defaultdict(int)
        for e in self._lines:
            key = e["ts"][:10] if by == "day" else e[by]
            sums[key] += e["tokens_in"] + e["tokens_out"]
        return dict(sums)

    def export(self, path: str) -> str:
        """Write all lines to a CSV file. Returns the path."""
        with self._lock:
            rows = list(self._lines)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["ts", "tenant", "model", "node", "tokens_in", "tokens_out"])
            w.writeheader()
            w.writerows(rows)
        return path

    def __len__(self) -> int:
        return len(self._lines)


def _encode(entry: dict) -> str:
    e = dict(entry)
    e["day"] = entry["ts"][:10]
    return ",".join(f"{k}={e[k]}" for k in ("ts", "day", "tenant", "model", "node", "tokens_in", "tokens_out"))


def _decode(line: str) -> dict:
    parts = dict(p.split("=", 1) for p in line.split(","))
    parts["tokens_in"] = int(parts["tokens_in"])
    parts["tokens_out"] = int(parts["tokens_out"])
    return parts
