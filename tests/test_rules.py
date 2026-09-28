"""Unit tests that run in seconds with no network and no big data.

These exist so Claude Code (or anyone) can refactor the cleaning logic and get
an immediate answer about whether it still behaves. Run: make test
"""
from datetime import datetime
import pytest
from pyspark.sql import Row
from urbanflow.curate.rules import rules
from urbanflow.curate.clean import derive


def _row(**kw):
    base = dict(pickup_ts=datetime(2025, 1, 5, 9, 0), dropoff_ts=datetime(2025, 1, 5, 9, 20),
                trip_distance=3.0, passenger_count=1.0, fare_amount=15.0,
                tip_amount=2.0, total_amount=18.0, payment_type=1)
    base.update(kw)
    return Row(**base)


def _survives(spark, row) -> bool:
    df = derive(spark.createDataFrame([row]))
    keep = None
    for _, _, cond in rules(2025, 1):
        keep = cond if keep is None else (keep & cond)
    return df.filter(keep).count() == 1


def test_clean_row_survives(spark):
    assert _survives(spark, _row())


@pytest.mark.parametrize("bad", [
    dict(pickup_ts=datetime(2001, 1, 1), dropoff_ts=datetime(2001, 1, 1, 0, 20)),  # stray year
    dict(dropoff_ts=datetime(2025, 1, 5, 8, 55)),                                  # ends before it starts
    dict(dropoff_ts=datetime(2025, 1, 5, 9, 0, 20)),                               # 20 seconds
    dict(trip_distance=0.0),                                                       # zero distance
    dict(trip_distance=250.0),                                                     # absurd distance
    dict(fare_amount=-5.0, total_amount=-5.0),                                     # negative money
    dict(passenger_count=0.0),                                                     # no passengers
    dict(trip_distance=90.0),                                                      # 270 mph
])
def test_dirty_rows_are_removed(spark, bad):
    assert not _survives(spark, _row(**bad))


def test_derived_columns(spark):
    df = derive(spark.createDataFrame([_row()])).first()
    assert df["duration_s"] == 1200
    assert df["duration_min"] == 20.0
    assert df["pickup_hour"] == 9
    assert df["is_card"] is True
    assert round(df["tip_pct"], 1) == 13.3
