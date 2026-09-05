"""Synthetic TLC-shaped data, so the team can build the whole pipeline on day 1
without waiting for a 7 GB download — and so tests run in CI with no network.

It deliberately injects the SAME dirt the real dataset contains:
  * timestamps outside the file's own month (real TLC files contain 2001 and 2098)
  * dropoff before pickup
  * zero / absurd trip distance
  * negative fares
  * impossible implied speeds
  * cash trips with tip_amount forced to 0 (the real recording bias)

If your cleaning rules work here, they will work on the real files.
"""
from __future__ import annotations
import argparse, random
from datetime import datetime, timedelta
from pathlib import Path
from .. import config

DIRT_RATE = 0.04


def _write_parquet(rows, path: Path) -> None:
    import pyarrow as pa, pyarrow.parquet as pq
    cols = {k: [r[k] for r in rows] for k in rows[0]}
    pq.write_table(pa.table(cols), path)


def generate(year: int = 2025, month: int = 1, n: int = 200_000, seed: int = 7) -> Path:
    rnd = random.Random(seed)
    base = datetime(year, month, 1)
    days = 28
    rows = []
    for _ in range(n):
        pu = base + timedelta(days=rnd.randrange(days), hours=rnd.randrange(24),
                              minutes=rnd.randrange(60), seconds=rnd.randrange(60))
        # rush-hour bias so the hourly demand chart shows something real
        if rnd.random() < 0.35:
            pu = pu.replace(hour=rnd.choice([8, 9, 17, 18, 19]))
        dist = round(max(0.3, rnd.lognormvariate(0.7, 0.7)), 2)
        dur = int(dist * rnd.uniform(150, 380)) + rnd.randrange(60, 300)
        do = pu + timedelta(seconds=dur)
        fare = round(3.0 + dist * rnd.uniform(2.4, 3.6) + dur / 60 * 0.35, 2)
        ptype = rnd.choices([1, 2], weights=[0.72, 0.28])[0]
        tip = round(fare * rnd.uniform(0.10, 0.30), 2) if ptype == 1 else 0.0
        r = {
            "VendorID": rnd.choice([1, 2]),
            "tpep_pickup_datetime": pu,
            "tpep_dropoff_datetime": do,
            "passenger_count": float(rnd.choices([1, 2, 3, 4, 5], weights=[.7, .15, .07, .05, .03])[0]),
            "trip_distance": dist,
            "RatecodeID": 1.0,
            "store_and_fwd_flag": "N",
            "PULocationID": rnd.randrange(1, 264),
            "DOLocationID": rnd.randrange(1, 264),
            "payment_type": ptype,
            "fare_amount": fare,
            "extra": 0.5,
            "mta_tax": 0.5,
            "tip_amount": tip,
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "total_amount": round(fare + tip + 1.3, 2),
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
        }
        # ---- inject realistic dirt ----
        if rnd.random() < DIRT_RATE:
            k = rnd.randrange(6)
            if k == 0:                                   # stray year, as in the real files
                r["tpep_pickup_datetime"] = datetime(rnd.choice([2001, 2098]), 1, 1, 0, 0)
            elif k == 1:                                 # dropoff before pickup
                r["tpep_dropoff_datetime"] = pu - timedelta(minutes=5)
            elif k == 2:                                 # impossible distance
                r["trip_distance"] = rnd.choice([0.0, 250.0])
            elif k == 3:                                 # negative money
                r["fare_amount"] = -abs(fare); r["total_amount"] = -abs(fare)
            elif k == 4:                                 # teleporting taxi
                r["tpep_dropoff_datetime"] = pu + timedelta(seconds=20)
            else:                                        # zero passengers
                r["passenger_count"] = 0.0
        rows.append(r)

    out_dir = config.RAW / "yellow" / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"yellow_{year:04d}-{month:02d}.parquet"
    _write_parquet(rows, out)
    print(f"synth: {len(rows):,} rows -> {out}  ({out.stat().st_size/1e6:.1f} MB)")
    return out


def generate_zones() -> Path:
    import csv as _csv
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
    out = config.RAW / "taxi_zone_lookup.csv"
    with out.open("w", newline="") as fh:
        w = _csv.writer(fh)
        w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        for i in range(1, 266):
            b = boroughs[i % len(boroughs)]
            w.writerow([i, b, f"{b} Zone {i}", "Boro Zone" if b != "Manhattan" else "Yellow Zone"])
    print(f"synth: zone lookup -> {out}")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate TLC-shaped synthetic trips (no network needed)")
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--month", type=int, default=1)
    a = ap.parse_args()
    generate_zones()
    generate(a.year, a.month, a.rows)
