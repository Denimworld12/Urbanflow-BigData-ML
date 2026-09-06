"""Stage S2 — Bronze to Silver.

What happens here, and why each part earns marks:
  1. Normalise the per-dataset timestamp column names into pickup_ts/dropoff_ts.
  2. Derive the columns the analysis needs (duration, speed, hour, weekday, tip %).
  3. Apply the cleaning rules and record how many rows each one removed.
  4. BROADCAST JOIN the 265-row zone table onto the trip table. Spark would
     otherwise shuffle both sides across the network; broadcasting sends the
     small side to every worker instead. Run .explain() before and after and you
     will see SortMergeJoin become BroadcastHashJoin.
  5. Write PARTITIONED BY (year, month) so later queries touching one month open
     one folder instead of scanning everything. That is partition pruning, and it
     is most of why the dashboard feels instant.
"""
from __future__ import annotations
import argparse, json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from .. import config
from ..session import get_spark, describe
from .rules import rules, quality_report

CURATED_COLUMNS = [
    "pickup_ts", "dropoff_ts", "duration_s", "duration_min", "trip_distance", "speed_mph",
    "passenger_count", "fare_amount", "tip_amount", "total_amount", "payment_type",
    "is_card", "tip_pct", "pickup_hour", "pickup_dow", "is_weekend",
    "PULocationID", "DOLocationID", "pu_borough", "pu_zone", "do_borough", "do_zone",
    "dataset", "year", "month",
]


def read_zones(spark: SparkSession) -> DataFrame:
    path = config.RAW / "taxi_zone_lookup.csv"
    if not path.exists():
        raise FileNotFoundError(f"zone lookup missing: {path} — run ingest first")
    return (spark.read.option("header", True).csv(str(path))
            .select(F.col("LocationID").cast("int").alias("loc_id"),
                    F.col("Borough").alias("borough"),
                    F.col("Zone").alias("zone")))


def normalise(df: DataFrame, dataset: str) -> DataFrame:
    pu, do = config.TS_COLS[dataset]
    out = df.withColumnRenamed(pu, "pickup_ts").withColumnRenamed(do, "dropoff_ts")

    if dataset == "fhvhv":
        # High-volume for-hire (Uber/Lyft): distance and money live under
        # different names entirely, not just missing — mapping them is not
        # optional, every cleaning rule (distance/fare/speed) needs a real
        # value or every single row gets filtered out.
        out = out.withColumnRenamed("trip_miles", "trip_distance")
        surcharges = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in
                          ("tolls", "bcf", "sales_tax", "congestion_surcharge", "airport_fee"))
        out = (out
               .withColumn("fare_amount", F.coalesce(F.col("base_passenger_fare"), F.lit(0.0)) + surcharges)
               .withColumnRenamed("tips", "tip_amount")
               .withColumn("total_amount", F.col("fare_amount") + F.coalesce(F.col("tip_amount"), F.lit(0.0))))

    # fhvhv/fhv genuinely have no passenger_count or card/cash payment_type —
    # fill those two so one schema serves every dataset.
    for c, t in (("trip_distance", "double"), ("fare_amount", "double"), ("tip_amount", "double"),
                 ("total_amount", "double"), ("passenger_count", "double"), ("payment_type", "int")):
        if c not in out.columns:
            out = out.withColumn(c, F.lit(None).cast(t))
    return out


def derive(df: DataFrame) -> DataFrame:
    dur = F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")
    return (df
            .withColumn("duration_s", dur)
            .withColumn("duration_min", F.round(dur / 60.0, 2))
            .withColumn("speed_mph", F.when(dur > 0, F.round(F.col("trip_distance") / (dur / 3600.0), 2)).otherwise(F.lit(None)))
            .withColumn("pickup_hour", F.hour("pickup_ts"))
            .withColumn("pickup_dow", F.dayofweek("pickup_ts"))
            .withColumn("is_weekend", F.dayofweek("pickup_ts").isin(1, 7))
            .withColumn("is_card", F.col("payment_type") == 1)
            .withColumn("tip_pct", F.when(F.col("fare_amount") > 0,
                                          F.round(100 * F.col("tip_amount") / F.col("fare_amount"), 2))))


def curate_month(spark: SparkSession, dataset: str, year: int, month: int,
                 use_broadcast: bool = True, report: bool = True) -> dict:
    src = config.RAW / dataset / f"year={year}" / f"{dataset}_{year:04d}-{month:02d}.parquet"
    if not src.exists():
        print(f"  skip {src.name} — not downloaded")
        return {}

    raw = spark.read.parquet(str(src))
    df = derive(normalise(raw, dataset))
    before = df.count()

    qr = quality_report(df, year, month) if report else []

    keep = None
    for _, _, cond in rules(year, month):
        c = cond & cond.isNotNull() if False else cond
        keep = c if keep is None else (keep & c)
    clean = df.filter(keep)

    zones = read_zones(spark)
    z_pu = zones.select(F.col("loc_id").alias("pu_id"), F.col("borough").alias("pu_borough"), F.col("zone").alias("pu_zone"))
    z_do = zones.select(F.col("loc_id").alias("do_id"), F.col("borough").alias("do_borough"), F.col("zone").alias("do_zone"))
    if use_broadcast:
        z_pu, z_do = broadcast(z_pu), broadcast(z_do)

    joined = (clean
              .join(z_pu, clean.PULocationID == z_pu.pu_id, "left")
              .join(z_do, clean.DOLocationID == z_do.do_id, "left")
              .withColumn("dataset", F.lit(dataset))
              .withColumn("year", F.lit(year))
              .withColumn("month", F.lit(month))
              .select(*CURATED_COLUMNS))

    (joined.write.mode("overwrite")
     .partitionBy("year", "month")
     .parquet(str(config.CURATED / dataset)))

    after = spark.read.parquet(str(config.CURATED / dataset)).where((F.col("year") == year) & (F.col("month") == month)).count()
    stats = {"dataset": dataset, "year": year, "month": month,
             "rows_in": before, "rows_out": after,
             "removed": before - after,
             "removed_pct": round(100.0 * (before - after) / before, 3) if before else 0.0,
             "rules": qr}
    print(f"  {dataset} {year}-{month:02d}: {before:,} -> {after:,} rows "
          f"({stats['removed']:,} removed, {stats['removed_pct']}%)")
    return stats


def run(tier: str, use_broadcast: bool = True) -> None:
    spec = config.TIERS[tier]
    spark = get_spark("UrbanFlow-curate")
    print(describe(spark))
    all_stats = []
    for ds in spec["datasets"]:
        for (y, m) in spec["months"]:
            s = curate_month(spark, ds, y, m, use_broadcast=use_broadcast)
            if s:
                all_stats.append(s)
    out = config.CURATED / "quality_report.json"
    out.write_text(json.dumps(all_stats, indent=2))
    print(f"\nquality report -> {out}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Bronze -> Silver: clean, enrich, partition")
    ap.add_argument("--tier", default="0", choices=sorted(config.TIERS))
    ap.add_argument("--no-broadcast", action="store_true", help="disable the broadcast join (for the benchmark comparison)")
    a = ap.parse_args()
    run(a.tier, use_broadcast=not a.no_broadcast)
