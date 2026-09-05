"""Stage S3 — Silver to Gold. Each question becomes one small answer table.

The dashboard NEVER reads the curated layer. It reads these, which are
megabytes rather than gigabytes — that gap is the whole point of the
architecture and the thing to demonstrate live.
"""
from __future__ import annotations
import argparse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from .. import config
from ..session import get_spark, describe


def curated(spark: SparkSession, dataset: str = "yellow") -> DataFrame:
    p = config.CURATED / dataset
    if not p.exists():
        raise FileNotFoundError(f"{p} missing — run curate first")
    return spark.read.parquet(str(p))


def _save(df: DataFrame, name: str) -> None:
    out = config.GOLD / name
    df.coalesce(1).write.mode("overwrite").parquet(str(out))
    print(f"  gold/{name}")


def demand_by_zone_hour(df: DataFrame) -> DataFrame:
    return (df.groupBy("pu_borough", "pu_zone", "pickup_hour", "is_weekend")
              .agg(F.count("*").alias("trips"),
                   F.round(F.avg("trip_distance"), 3).alias("avg_distance"),
                   F.round(F.avg("duration_min"), 2).alias("avg_duration_min"))
              .orderBy(F.desc("trips")))


def od_matrix(df: DataFrame, top: int = 200) -> DataFrame:
    return (df.groupBy("pu_borough", "pu_zone", "do_borough", "do_zone")
              .agg(F.count("*").alias("trips"),
                   F.round(F.avg("total_amount"), 2).alias("avg_fare"),
                   F.round(F.avg("duration_min"), 2).alias("avg_duration_min"))
              .orderBy(F.desc("trips")).limit(top))


def daily_kpis(df: DataFrame) -> DataFrame:
    return (df.withColumn("d", F.to_date("pickup_ts"))
              .groupBy("d")
              .agg(F.count("*").alias("trips"),
                   F.round(F.sum("total_amount"), 2).alias("revenue"),
                   F.round(F.avg("total_amount"), 2).alias("avg_fare"),
                   F.round(F.avg("trip_distance"), 3).alias("avg_distance"),
                   F.round(F.avg("duration_min"), 2).alias("avg_duration_min"))
              .orderBy("d"))


def tipping(df: DataFrame) -> DataFrame:
    """The cash-tip bias, quantified.

    tip_amount is only recorded for card payments. Any 'average tip' computed
    over all trips is therefore wrong. This table reports card and cash
    separately so the bias is visible rather than hidden — a finding worth a
    paragraph in the report.
    """
    return (df.groupBy("pu_borough", "is_card")
              .agg(F.count("*").alias("trips"),
                   F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
                   F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                   F.round(F.sum(F.when(F.col("tip_amount") > 0, 1).otherwise(0)) / F.count("*") * 100, 2)
                    .alias("pct_trips_with_tip"))
              .orderBy("pu_borough", "is_card"))


def speed_by_hour(df: DataFrame) -> DataFrame:
    """A congestion proxy: the city visibly slows down at rush hour."""
    return (df.groupBy("pu_borough", "pickup_hour", "is_weekend")
              .agg(F.count("*").alias("trips"),
                   F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
                   F.round(F.expr("percentile_approx(speed_mph, 0.5)"), 2).alias("median_speed_mph"))
              .orderBy("pu_borough", "pickup_hour"))


def airport_flows(df: DataFrame) -> DataFrame:
    air = F.col("pu_zone").rlike("(?i)airport|JFK|LaGuardia|EWR") | F.col("do_zone").rlike("(?i)airport|JFK|LaGuardia|EWR")
    return (df.filter(air)
              .groupBy("pickup_hour", "is_weekend")
              .agg(F.count("*").alias("trips"),
                   F.round(F.avg("total_amount"), 2).alias("avg_fare"),
                   F.round(F.avg("trip_distance"), 2).alias("avg_distance"))
              .orderBy("pickup_hour"))


TABLES = {
    "demand_by_zone_hour": demand_by_zone_hour,
    "od_matrix": od_matrix,
    "daily_kpis": daily_kpis,
    "tipping": tipping,
    "speed_by_hour": speed_by_hour,
    "airport_flows": airport_flows,
}


def run(dataset: str = "yellow", only: str | None = None) -> None:
    spark = get_spark("UrbanFlow-gold")
    print(describe(spark))
    df = curated(spark, dataset).cache()
    n = df.count()
    print(f"curated rows: {n:,}")
    for name, fn in TABLES.items():
        if only and name != only:
            continue
        _save(fn(df), name)
    print(f"\n{len(TABLES) if not only else 1} gold tables written to {config.GOLD}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Silver -> Gold aggregations")
    ap.add_argument("--dataset", default="yellow")
    ap.add_argument("--only", default=None, choices=sorted(TABLES))
    a = ap.parse_args()
    run(a.dataset, a.only)
