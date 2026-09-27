"""Query the HBase serving tables from Python (run by `make hbase-query`).

    python -m urbanflow.hbase.query demo
    python -m urbanflow.hbase.query demand  --zone "JFK Airport" --day weekday [--hour 17]
    python -m urbanflow.hbase.query predict --from Manhattan --to Queens --day weekday --hour 8 [--miles 10]
    python -m urbanflow.hbase.query daily   --month 2025-03
    python -m urbanflow.hbase.query count

Every access below is by row key: a point get, a prefix scan, or a
start/stop range scan. None of them reads the whole table.
"""
from __future__ import annotations
import argparse
import time

from . import keys, store

# Counting in HBase means visiting every row; these filters make the server
# send back only the first cell's key per row, not the data.
COUNT_FILTER = "FirstKeyOnlyFilter() AND KeyOnlyFilter()"


def _timed(label: str, fn):
    t0 = time.perf_counter()
    out = fn()
    print(f"-- {label}  ({(time.perf_counter() - t0) * 1000:.1f} ms)")
    return out


def get(conn, table: str, row_key: str, columns: list[str] | None = None) -> dict[str, str]:
    cols = [c.encode() for c in columns] if columns else None
    return store.decode(conn.table(table).row(row_key.encode(), columns=cols))


def scan_prefix(conn, table: str, prefix: str, columns: list[str] | None = None):
    cols = [c.encode() for c in columns] if columns else None
    return [(k.decode(), store.decode(v))
            for k, v in conn.table(table).scan(row_prefix=prefix.encode(), columns=cols)]


def scan_range(conn, table: str, start: str, stop: str):
    return [(k.decode(), store.decode(v))
            for k, v in conn.table(table).scan(row_start=start.encode(), row_stop=stop.encode())]


def count(conn, table: str) -> int:
    return sum(1 for _ in conn.table(table).scan(filter=COUNT_FILTER))


# ---------------------------------------------------------------- commands

def cmd_demand(conn, zone: str, day: str, hour: int | None) -> None:
    if hour is not None:
        key = keys.demand_key(zone, day, hour)
        row = _timed(f"GET {keys.DEMAND} '{key}'", lambda: get(conn, keys.DEMAND, key))
        print(f"   {row}" if row else "   (no such row)")
        return
    prefix = keys.demand_prefix(zone, day)
    rows = _timed(f"SCAN {keys.DEMAND} prefix '{prefix}' family m",
                  lambda: scan_prefix(conn, keys.DEMAND, prefix, ["m"]))
    for k, v in rows:
        print(f"   {k:40s} trips={v['m:trips']:>8s}  avg_min={v['m:avg_duration_min']}")
    if rows:
        peak = max(rows, key=lambda kv: int(kv[1]["m:trips"]))
        print(f"   {len(rows)} rows; busiest hour: {peak[0].rsplit(keys.SEP, 1)[1]}:00 "
              f"with {int(peak[1]['m:trips']):,} trips")


def cmd_predict(conn, pu: str, do: str, day: str, hour: int, miles: float | None) -> None:
    if miles is not None:
        key = keys.duration_key(pu, do, day, hour, miles)
        row = _timed(f"GET {keys.DURATION} '{key}'", lambda: get(conn, keys.DURATION, key))
        print(f"   predicted duration: {row['p:minutes']} min" if row else "   (not in the grid)")
        return
    prefix = keys.duration_prefix(pu, do, day, hour)
    rows = _timed(f"SCAN {keys.DURATION} prefix '{prefix}'",
                  lambda: scan_prefix(conn, keys.DURATION, prefix))
    for k, v in rows[::5]:
        print(f"   {k:44s} {v['p:minutes']:>6s} min")
    print(f"   {len(rows)} rows (every 5th shown): the model's duration-vs-distance curve")


def cmd_daily(conn, month: str) -> None:
    y, m = (int(x) for x in month.split("-"))
    start = f"{y:04d}-{m:02d}-01"
    stop = f"{y + (m == 12):04d}-{m % 12 + 1:02d}-01"     # exclusive
    rows = _timed(f"SCAN {keys.DAILY} STARTROW '{start}' STOPROW '{stop}'",
                  lambda: scan_range(conn, keys.DAILY, start, stop))
    trips = sum(int(v["k:trips"]) for _, v in rows)
    revenue = sum(float(v["k:revenue"]) for _, v in rows)
    print(f"   {len(rows)} days  {trips:,} trips  ${revenue:,.0f} revenue")
    if rows:
        top = max(rows, key=lambda kv: int(kv[1]["k:trips"]))
        print(f"   busiest day: {top[0]} with {int(top[1]['k:trips']):,} trips")


def cmd_count(conn) -> None:
    for t in store.FAMILIES:
        n = _timed(f"COUNT {t}", lambda t=t: count(conn, t))
        print(f"   {n:,} rows")


def demo(conn) -> None:
    cmd_demand(conn, "JFK Airport", "weekday", 17)
    cmd_demand(conn, "JFK Airport", "weekday", None)
    cmd_predict(conn, "Manhattan", "Queens", "weekday", 8, 10)
    cmd_predict(conn, "Manhattan", "Queens", "weekday", 8, None)
    cmd_daily(conn, "2025-03")
    cmd_count(conn)


def main() -> None:
    ap = argparse.ArgumentParser(description="Query UrbanFlow's HBase serving tables")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("demo", help="run every query type with example values")
    d = sub.add_parser("demand", help="demand for a pickup zone")
    d.add_argument("--zone", required=True)
    d.add_argument("--day", choices=keys.DAYTYPES, default="weekday")
    d.add_argument("--hour", type=int, help="omit to scan all 24 hours")
    p = sub.add_parser("predict", help="predicted trip duration")
    p.add_argument("--from", dest="pu", required=True)
    p.add_argument("--to", dest="do", required=True)
    p.add_argument("--day", choices=keys.DAYTYPES, default="weekday")
    p.add_argument("--hour", type=int, required=True)
    p.add_argument("--miles", type=float, help="omit to scan all distances")
    k = sub.add_parser("daily", help="daily KPIs for one month (range scan)")
    k.add_argument("--month", required=True, help="YYYY-MM")
    sub.add_parser("count", help="row count of every table")
    a = ap.parse_args()

    conn = store.connect()
    store.require_tables(conn)
    {"demo": lambda: demo(conn),
     "demand": lambda: cmd_demand(conn, a.zone, a.day, a.hour),
     "predict": lambda: cmd_predict(conn, a.pu, a.do, a.day, a.hour, a.miles),
     "daily": lambda: cmd_daily(conn, a.month),
     "count": lambda: cmd_count(conn)}[a.cmd]()
    conn.close()


if __name__ == "__main__":
    main()
