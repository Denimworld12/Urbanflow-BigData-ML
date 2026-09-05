"""Stage S5 — prove it scales. This is the chapter that turns a data-analysis
project into a BIG DATA project, and it is the cheapest marks in the whole
build because the code is short and the results are quotable.

Four experiments, each changing exactly one variable:

  1. FORMAT       Parquet vs CSV, same query, same rows.
  2. PARTITIONING partitioned vs flat, querying a single month.
  3. CORES        local[1] vs local[2] vs local[4] — the speedup curve.
  4. JOIN         broadcast vs sort-merge for the zone lookup.

Report the numbers AND explain where the curve bends away from ideal. Saying
"4 cores gave 3.1x, not 4x, because of shuffle coordination and disk contention"
earns more than the chart itself.
"""
from __future__ import annotations
import argparse, json, subprocess, sys, time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from .. import config
from ..session import get_spark, describe


def timed(fn, repeat: int = 3) -> float:
    """Median of N runs. One run is noise, not a measurement."""
    ts = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    ts.sort()
    return round(ts[len(ts) // 2], 3)


def _query(df):
    return (df.groupBy("pu_borough")
              .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("rev"))
              .collect())


# ---------------------------------------------------------------- 1. format
def bench_format(spark: SparkSession, dataset: str) -> dict:
    src = config.CURATED / dataset
    csv_dir = config.BENCH / "as_csv"
    if not csv_dir.exists():
        print("  materialising a CSV copy (one-off)...")
        spark.read.parquet(str(src)).write.mode("overwrite").option("header", True).csv(str(csv_dir))
    pq = timed(lambda: _query(spark.read.parquet(str(src))))
    cs = timed(lambda: _query(spark.read.option("header", True).option("inferSchema", True).csv(str(csv_dir))))
    return {"experiment": "format", "parquet_s": pq, "csv_s": cs,
            "speedup": round(cs / pq, 2) if pq else None,
            "note": "Parquet reads only the columns the query touches; CSV must decode every byte of every row."}


# ---------------------------------------------------------------- 2. partitioning
def bench_partitioning(spark: SparkSession, dataset: str) -> dict:
    part = config.CURATED / dataset
    flat = config.BENCH / "flat"
    if not flat.exists():
        print("  materialising an unpartitioned copy (one-off)...")
        spark.read.parquet(str(part)).write.mode("overwrite").parquet(str(flat))
    parts = spark.read.parquet(str(part)).select("year", "month").distinct().collect()
    y, m = parts[0]["year"], parts[0]["month"]
    if len(parts) < 2:
        return {"experiment": "partitioning", "skipped": True, "partitions_present": len(parts),
                "note": "SKIPPED — the curated layer holds a single month, so there is nothing to prune. "
                        "Partition pruning can only be demonstrated on Tier 1 or Tier 2 (many months). "
                        "Run: make curate TIER=1, then re-run this benchmark."}
    p = timed(lambda: _query(spark.read.parquet(str(part)).where((F.col("year") == y) & (F.col("month") == m))))
    f = timed(lambda: _query(spark.read.parquet(str(flat)).where((F.col("year") == y) & (F.col("month") == m))))
    return {"experiment": "partitioning", "partitioned_s": p, "flat_s": f,
            "speedup": round(f / p, 2) if p else None, "queried": f"{y}-{m:02d}",
            "note": "Partition pruning lets Spark skip whole folders using the directory names, before opening any file."}


# ---------------------------------------------------------------- 3. cores
def bench_single_core_run(dataset: str, cores: int) -> float:
    """Run in THIS process with a fixed core count. Called via subprocess."""
    spark = get_spark(f"bench-{cores}", cores=cores, shuffle_parts=cores * 8)
    df = spark.read.parquet(str(config.CURATED / dataset))
    t = timed(lambda: _query(df), repeat=3)
    spark.stop()
    return t


def bench_cores(dataset: str, ladder: list[int]) -> dict:
    """Each core count needs a fresh JVM — master cannot change once a
    SparkContext exists — so we shell out per rung."""
    results = {}
    for c in ladder:
        print(f"  local[{c}] ...", flush=True)
        p = subprocess.run([sys.executable, "-m", "urbanflow.analyze.benchmark",
                            "--single-core-run", str(c), "--dataset", dataset],
                           capture_output=True, text=True)
        line = [l for l in p.stdout.splitlines() if l.startswith("SECONDS=")]
        if not line:
            print(p.stdout[-800:], p.stderr[-800:])
            raise RuntimeError(f"core benchmark failed for local[{c}]")
        results[c] = float(line[0].split("=")[1])
    base = results[ladder[0]]
    sp = {str(k): round(base / v, 2) for k, v in results.items()}
    note = ("Speedup is sub-linear: coordination overhead, shuffle writes and a single shared disk "
            "mean N cores never give N times the throughput. Amdahl's law in one chart.")
    if any(v < 1.0 for v in sp.values()):
        note = ("NEGATIVE SCALING DETECTED — adding cores made it slower. This is a real and explainable "
                "result on a small dataset: task scheduling, JVM thread coordination and shuffle setup cost "
                "more than the parallel work saves. It is strong evidence FOR the big-data argument, not "
                "against it: parallelism only pays once the data is large enough to amortise the overhead. "
                "Re-run on Tier 1 or Tier 2 to see the curve turn positive, and put BOTH results in the "
                "report — the crossover point is a genuinely interesting finding.")
    return {"experiment": "cores", "seconds": {str(k): v for k, v in results.items()},
            "speedup_vs_1": sp, "note": note}


# ---------------------------------------------------------------- 4. join
def bench_join(spark: SparkSession, dataset: str) -> dict:
    from pyspark.sql.functions import broadcast
    from ..curate.clean import read_zones
    trips = spark.read.parquet(str(config.CURATED / dataset)).select("PULocationID")
    zones = read_zones(spark)

    def run(bcast: bool):
        z = broadcast(zones) if bcast else zones
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1 if not bcast else 10 * 1024 * 1024)
        return trips.join(z, trips.PULocationID == z.loc_id, "left").count()

    b = timed(lambda: run(True))
    s = timed(lambda: run(False))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
    return {"experiment": "join", "broadcast_s": b, "sort_merge_s": s,
            "speedup": round(s / b, 2) if b else None,
            "note": "Broadcasting the 265-row zone table avoids shuffling 240M trip rows across partitions. "
                    "Run .explain() on both to show BroadcastHashJoin vs SortMergeJoin."}


# ---------------------------------------------------------------- driver
def run(dataset: str = "yellow", ladder: list[int] | None = None) -> None:
    ladder = ladder or [1, 2, 4]
    spark = get_spark("UrbanFlow-bench")
    print(describe(spark), "\n")
    rows = spark.read.parquet(str(config.CURATED / dataset)).count()
    months = spark.read.parquet(str(config.CURATED / dataset)).select("year", "month").distinct().count()
    print(f"benchmarking against {rows:,} rows across {months} month partition(s)\n")
    if rows < 5_000_000:
        print("  NOTE: under 5M rows. Timings will be dominated by fixed overhead.")
        print("        Quote Tier 1/2 numbers in the report, not these.\n")
    out = [{"experiment": "scale", "rows": rows, "month_partitions": months,
            "note": "Context for every timing below. Benchmarks on small data measure overhead, not throughput."}]
    for name, fn in (("format", bench_format), ("partitioning", bench_partitioning), ("join", bench_join)):
        print(f"[{name}]")
        r = fn(spark, dataset)
        print("  ", {k: v for k, v in r.items() if k != "note"})
        out.append(r)
    spark.stop()

    print("[cores]")
    out.append(bench_cores(dataset, ladder))
    print("  ", out[-1]["speedup_vs_1"])

    dest = config.GOLD / "benchmarks.json"
    dest.write_text(json.dumps(out, indent=2))
    (config.GOLD / "benchmarks.md").write_text(as_markdown(out))
    print(f"\n-> {dest}\n-> {config.GOLD / 'benchmarks.md'}")


def as_markdown(rows: list[dict]) -> str:
    L = ["# Benchmark results", "", "_Measure on your own machine and quote these numbers in the report._", ""]
    for r in rows:
        L.append(f"## {r['experiment']}")
        L.append("")
        for k, v in r.items():
            if k in ("experiment", "note"):
                continue
            L.append(f"- **{k}**: {v}")
        L.append("")
        L.append(f"> {r['note']}")
        L.append("")
    return "\n".join(L)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Benchmark suite: format, partitioning, cores, join")
    ap.add_argument("--dataset", default="yellow")
    ap.add_argument("--cores", default="1,2,4", help="core ladder, e.g. 1,2,4")
    ap.add_argument("--single-core-run", type=int, default=None, help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.single_core_run:
        print(f"SECONDS={bench_single_core_run(a.dataset, a.single_core_run)}")
    else:
        run(a.dataset, [int(x) for x in a.cores.split(",")])
