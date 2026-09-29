"""Real-time path, step 1 — replay bronze trips into Kafka as live events.

A taxi trip becomes a fact the moment the meter stops, so the producer sends
trips in DROPOFF order, one JSON event each, at a fixed rate. The stream job
then windows them by PICKUP time (the event time). That gap is the point:

  * event time      pickup_ts — when the trip actually started
  * ingestion time  the Kafka record timestamp — when this producer sent it
  * processing time when Spark gets round to it

A long trip arrives after many shorter trips that started later, i.e. out of
order. The watermark in job.py decides how late is still acceptable. Nothing
here cleans the data: the stray 2001/2098 timestamps are sent as-is, and it
is the stream job's job to reject them — exactly as curate does in batch.

    python -m urbanflow.stream.producer --rate 2000 --limit 300000
"""
from __future__ import annotations
import argparse, json, time
from datetime import datetime
from pathlib import Path
from .. import config

# Bronze columns an event carries. Yellow and green share these names; only
# the two timestamp columns differ, and config.TS_COLS already maps those.
FIELDS = ("PULocationID", "DOLocationID", "trip_distance", "passenger_count",
          "fare_amount", "tip_amount", "total_amount", "payment_type")
SUPPORTED = ("yellow", "green")


def bronze_path(dataset: str, year: int, month: int) -> Path:
    return config.RAW / dataset / f"year={year}" / f"{dataset}_{year:04d}-{month:02d}.parquet"


def _iso(ts: datetime | None) -> str | None:
    return ts.isoformat(timespec="seconds") if ts is not None else None


def to_event(row: dict, dataset: str, trip_id: str) -> dict:
    """One bronze row -> one JSON-ready trip event. Pure, so it is unit-tested."""
    pu, do = config.TS_COLS[dataset]
    ev = {"trip_id": trip_id, "dataset": dataset,
          "pickup_ts": _iso(row[pu]), "dropoff_ts": _iso(row[do])}
    for f in FIELDS:
        v = row.get(f)
        ev[f] = None if v is None else (int(v) if f in ("PULocationID", "DOLocationID", "payment_type") else float(v))
    return ev


def load_ordered(dataset: str, year: int, month: int, limit: int, skip: int = 0):
    """Bronze rows sorted by dropoff time, as a pyarrow Table. No Spark needed."""
    import pyarrow.parquet as pq
    src = bronze_path(dataset, year, month)
    if not src.exists():
        raise SystemExit(f"bronze file missing: {src}\n  run `make synth` or `make ingest TIER=0` first")
    pu, do = config.TS_COLS[dataset]
    table = pq.read_table(src, columns=[pu, do, *FIELDS])
    table = table.sort_by([(do, "ascending")])
    return table.slice(skip, limit) if limit else table.slice(skip)


def run(dataset: str, year: int, month: int, rate: float, limit: int,
        topic: str, bootstrap: str, skip: int = 0, batch: int = 5000) -> None:
    from kafka import KafkaProducer
    table = load_ordered(dataset, year, month, limit, skip)
    total = table.num_rows
    print(f"producer: {total:,} {dataset} trips from {year}-{month:02d} -> topic '{topic}' "
          f"@ {bootstrap}, {'max speed' if rate <= 0 else f'{rate:,.0f} events/s'}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",           # wait until every in-sync replica has the record
        linger_ms=20,         # batch sends for up to 20 ms — far fewer requests
    )
    _, do = config.TS_COLS[dataset]
    sent, start, last_report = 0, time.monotonic(), 0.0
    try:
        for offset in range(0, total, batch):
            for i, row in enumerate(table.slice(offset, batch).to_pylist()):
                ev = to_event(row, dataset, f"{dataset}-{year:04d}{month:02d}-{skip + offset + i}")
                # Key = pickup zone: every event for one zone lands on the same
                # partition, so per-zone order is preserved inside Kafka.
                producer.send(topic, key=str(ev["PULocationID"]).encode(), value=json.dumps(ev).encode())
                sent += 1
                if rate > 0:
                    ahead = sent / rate - (time.monotonic() - start)
                    if ahead > 0:
                        time.sleep(ahead)
                now = time.monotonic()
                if now - last_report >= 5:
                    last_report = now
                    print(f"  sent {sent:>9,}/{total:,}  {sent / (now - start):>7,.0f} ev/s  "
                          f"dropoff clock {_iso(row[do])}")
        producer.flush()
    except KeyboardInterrupt:
        print("  interrupted — flushing what was already sent")
        producer.flush()
    finally:
        producer.close()
    took = time.monotonic() - start
    print(f"producer: done, {sent:,} events in {took:,.1f}s ({sent / took if took else 0:,.0f} ev/s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Replay bronze trips into Kafka as JSON events")
    ap.add_argument("--dataset", default="yellow", choices=SUPPORTED)
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--month", type=int, default=1)
    ap.add_argument("--rate", type=float, default=2000, help="events per second; 0 = as fast as possible")
    ap.add_argument("--limit", type=int, default=300_000, help="max trips to send; 0 = the whole month")
    ap.add_argument("--skip", type=int, default=0, help="start after this many trips, to continue an earlier replay")
    ap.add_argument("--topic", default=config.TRIPS_TOPIC)
    ap.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    a = ap.parse_args()
    run(a.dataset, a.year, a.month, a.rate, a.limit, a.topic, a.bootstrap, a.skip)
