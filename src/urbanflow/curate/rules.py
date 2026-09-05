"""Cleaning rules, each named and independently measurable.

Every rule is (name, human_reason, condition_that_KEEPS_the_row).
Keeping them in this shape is what lets clean.py produce a data-quality table
saying exactly how many rows each rule removed — which is a required section of
your report, and the thing that shows you looked at the data rather than
trusting it.
"""
from __future__ import annotations
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from .. import config


def rules(year: int, month: int) -> list[tuple[str, str, Column]]:
    """Conditions that a VALID row satisfies."""
    return [
        ("out_of_month",
         "pickup timestamp outside the file's own month (real TLC files carry stray 2001 and 2098 records)",
         (F.year("pickup_ts") == year) & (F.month("pickup_ts") == month)),

        ("dropoff_before_pickup",
         "journey ends before it starts",
         F.col("dropoff_ts") > F.col("pickup_ts")),

        ("duration_out_of_range",
         f"under {config.MIN_TRIP_SECONDS}s or over {config.MAX_TRIP_SECONDS//3600}h",
         (F.col("duration_s") >= config.MIN_TRIP_SECONDS) & (F.col("duration_s") <= config.MAX_TRIP_SECONDS)),

        ("distance_out_of_range",
         f"zero, negative, or over {config.MAX_TRIP_MILES:.0f} miles",
         (F.col("trip_distance") > 0) & (F.col("trip_distance") <= config.MAX_TRIP_MILES)),

        ("negative_money",
         "negative fare or total",
         (F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0)),

        ("impossible_speed",
         f"implied speed over {config.MAX_SPEED_MPH:.0f} mph — not possible in New York traffic",
         F.col("speed_mph") <= config.MAX_SPEED_MPH),

        ("zero_passengers",
         "no passengers recorded",
         F.coalesce(F.col("passenger_count"), F.lit(1.0)) > 0),
    ]


def quality_report(df: DataFrame, year: int, month: int) -> list[dict]:
    """Rows removed by each rule, measured independently.

    Note these overlap — a single bad row can violate several rules — so the
    numbers do not sum to the total removed. Say that in the report rather than
    presenting them as a partition.
    """
    total = df.count()
    out = []
    for name, reason, keep in rules(year, month):
        bad = df.filter(~keep | keep.isNull()).count()
        out.append({"rule": name, "reason": reason, "rows_removed": bad,
                    "pct": round(100.0 * bad / total, 3) if total else 0.0})
    return out
