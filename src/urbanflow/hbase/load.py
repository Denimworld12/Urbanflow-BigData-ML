"""Load gold Parquet tables into HBase (run by `make hbase-load`).

gold/demand_by_zone_hour  -> urbanflow:demand
gold/duration_predictions -> urbanflow:duration_pred
gold/daily_kpis           -> urbanflow:daily_kpis

Row keys are deterministic (keys.py). create_tables.rb truncates existing
tables first, so each load replaces the table contents. Values are
stored as UTF-8 strings: HBase itself only stores bytes, and strings keep the
cells readable in the HBase shell.
"""
from __future__ import annotations
import argparse
import time
from collections.abc import Iterable, Iterator

import pyarrow.parquet as pq

from .. import config
from . import keys, store


def _rows(table: str) -> list[dict]:
    path = config.GOLD / table
    if not path.exists():
        raise FileNotFoundError(f"{path} missing — build gold first (`make gold`, "
                                "`make predict-grid`) or copy a prepared gold layer into data/gold")
    return pq.read_table(path).to_pylist()


def _cells(**cols) -> dict[bytes, bytes]:
    return {c.encode(): str(v).encode() for c, v in cols.items() if v is not None}


def demand_puts(rows: Iterable[dict]) -> Iterator[tuple[str, dict[bytes, bytes]]]:
    for r in rows:
        day = keys.daytype(r["is_weekend"])
        yield keys.demand_key(r["pu_zone"], day, r["pickup_hour"]), _cells(**{
            "m:trips": r["trips"],
            "m:avg_distance": r["avg_distance"],
            "m:avg_duration_min": r["avg_duration_min"],
            "z:borough": r["pu_borough"],
            "z:zone": r["pu_zone"],
            "z:daytype": day,
            "z:hour": r["pickup_hour"],
        })


def duration_puts(rows: Iterable[dict]) -> Iterator[tuple[str, dict[bytes, bytes]]]:
    for r in rows:
        day = keys.DOW_TO_DAYTYPE[r["pickup_dow"]]
        yield keys.duration_key(r["pu_borough"], r["do_borough"], day,
                                r["pickup_hour"], r["trip_distance"]), _cells(**{
            "p:minutes": r["predicted_duration_min"],
        })


def daily_puts(rows: Iterable[dict]) -> Iterator[tuple[str, dict[bytes, bytes]]]:
    for r in rows:
        yield keys.daily_key(r["d"]), _cells(**{
            f"k:{c}": r[c] for c in ("trips", "revenue", "avg_fare", "avg_distance", "avg_duration_min")
        })


JOBS = [
    ("demand_by_zone_hour", keys.DEMAND, demand_puts),
    ("duration_predictions", keys.DURATION, duration_puts),
    ("daily_kpis", keys.DAILY, daily_puts),
]


def run(batch_size: int = 2000) -> None:
    conn = store.connect()
    store.require_tables(conn)
    for gold_table, hbase_table, to_puts in JOBS:
        t0 = time.perf_counter()
        n = 0
        with conn.table(hbase_table).batch(batch_size=batch_size) as batch:
            for row_key, cells in to_puts(_rows(gold_table)):
                batch.put(row_key.encode(), cells)
                n += 1
        print(f"  {gold_table:22s} -> {hbase_table:24s} {n:>7,} rows  "
              f"{time.perf_counter() - t0:5.1f}s")
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load gold Parquet into HBase over Thrift")
    ap.add_argument("--batch-size", type=int, default=2000)
    run(ap.parse_args().batch_size)
