import csv
import os

import pytest

from ledger import Ledger


def test_record_appends_line(tmp_path):
    p = str(tmp_path / "ledger.jsonl")
    led = Ledger(p)
    led.record("tenant-a", "glm-5.3-flash", "node-1", 100, 50)
    assert len(led) == 1
    with open(p) as f:
        assert "tenant=tenant-a" in f.readline()


def test_record_defaults_ts_now():
    led = Ledger()
    e = led.record("tenant-a", "m", "n", 1, 1)
    assert "T" in e["ts"]


def test_record_rejects_bad_input():
    led = Ledger()
    with pytest.raises(ValueError):
        led.record("", "m", "n", 1, 1)
    with pytest.raises(ValueError):
        led.record("t", "m", "n", -1, 0)


def test_rollup_by_tenant():
    led = Ledger()
    led.record("tenant-a", "m1", "n", 10, 5)
    led.record("tenant-a", "m2", "n", 20, 5)
    led.record("tenant-b", "m1", "n", 1, 1)
    assert led.rollup(by="tenant") == {"tenant-a": 40, "tenant-b": 2}


def test_rollup_by_model():
    led = Ledger()
    led.record("t", "m1", "n", 10, 0)
    led.record("t", "m2", "n", 3, 2)
    assert led.rollup(by="model") == {"m1": 10, "m2": 5}


def test_rollup_by_day():
    led = Ledger()
    led.record("t", "m", "n", 4, 1, ts="2026-10-13T10:00:00+00:00")
    led.record("t", "m", "n", 2, 0, ts="2026-10-13T11:00:00+00:00")
    led.record("t", "m", "n", 9, 0, ts="2026-10-14T11:00:00+00:00")
    assert led.rollup(by="day") == {"2026-10-13": 7, "2026-10-14": 9}


def test_rollup_invalid_key():
    led = Ledger()
    with pytest.raises(ValueError):
        led.rollup(by="banana")


def test_export_csv(tmp_path):
    led = Ledger()
    led.record("tenant-a", "m", "n", 7, 3, ts="2026-10-14T00:00:00+00:00")
    out = str(tmp_path / "out.csv")
    led.export(out)
    with open(out) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["tenant"] == "tenant-a"
    assert rows[0]["tokens_in"] == "7"


def test_reload_from_disk(tmp_path):
    p = str(tmp_path / "ledger.jsonl")
    led = Ledger(p)
    led.record("tenant-a", "m", "n", 5, 5)
    led2 = Ledger(p)
    assert len(led2) == 1
    assert led2.rollup() == {"tenant-a": 10}


def test_threaded_writes(tmp_path):
    import threading

    p = str(tmp_path / "ledger.jsonl")
    led = Ledger(p)
    def worker(i):
        for _ in range(20):
            led.record("tenant-a", "m", "n", 1, 1)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(led) == 80
    with open(p) as f:
        assert sum(1 for _ in f) == 80
