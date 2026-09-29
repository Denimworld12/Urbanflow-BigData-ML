"""The real-time path's logic, tested without Kafka.

The streaming transforms in stream/job.py are ordinary DataFrame functions, so
they run on a static DataFrame shaped like Kafka records (withWatermark is a
no-op in batch). Run: make test
"""
import json
from datetime import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from urbanflow.stream.producer import to_event
from urbanflow.stream.job import parse, clean, zone_metrics


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.appName("test-stream").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s


def _bronze(pu, do, zone=161, fare=15.0, payment=1, tip=3.0):
    return {"tpep_pickup_datetime": pu, "tpep_dropoff_datetime": do,
            "PULocationID": zone, "DOLocationID": 237, "trip_distance": 2.5,
            "passenger_count": 1, "fare_amount": fare, "tip_amount": tip,
            "total_amount": fare + tip + 2.0, "payment_type": payment}


def test_to_event_is_json_ready():
    ev = to_event(_bronze(datetime(2025, 1, 5, 9, 1), datetime(2025, 1, 5, 9, 15)), "yellow", "t-1")
    assert ev["pickup_ts"] == "2025-01-05T09:01:00"
    assert ev["dropoff_ts"] == "2025-01-05T09:15:00"
    assert ev["PULocationID"] == 161 and isinstance(ev["PULocationID"], int)
    assert isinstance(ev["passenger_count"], float)
    json.dumps(ev)  # must serialise as-is


def test_to_event_keeps_nulls():
    row = _bronze(datetime(2025, 1, 5, 9, 1), datetime(2025, 1, 5, 9, 15))
    row["passenger_count"] = None
    assert to_event(row, "yellow", "t-2")["passenger_count"] is None


def _kafka_df(spark, events):
    rows = [(json.dumps(e).encode(), datetime(2026, 1, 1)) for e in events]
    return spark.createDataFrame(rows, "value binary, timestamp timestamp")


def test_windowed_metrics_and_cleaning(spark):
    events = [
        # two trips in the 09:00-09:05 window for zone 161
        to_event(_bronze(datetime(2025, 1, 5, 9, 1), datetime(2025, 1, 5, 9, 15), fare=10.0), "yellow", "a"),
        to_event(_bronze(datetime(2025, 1, 5, 9, 4), datetime(2025, 1, 5, 9, 20), fare=20.0), "yellow", "b"),
        # one in the next window
        to_event(_bronze(datetime(2025, 1, 5, 9, 6), datetime(2025, 1, 5, 9, 30)), "yellow", "c"),
        # dirt the stream must reject before the watermark ever sees it
        to_event(_bronze(datetime(2098, 1, 1, 0, 0), datetime(2098, 1, 1, 0, 20)), "yellow", "stray-year"),
        to_event(_bronze(datetime(2025, 1, 5, 9, 2), datetime(2025, 1, 5, 9, 2, 20)), "yellow", "teleport"),
        to_event(_bronze(datetime(2025, 1, 5, 9, 2), datetime(2025, 1, 5, 9, 12), fare=-5.0), "yellow", "neg-fare"),
    ]
    zones = spark.createDataFrame([(161, "Manhattan", "Midtown Center")], "loc_id int, borough string, zone string")

    trips = clean(parse(_kafka_df(spark, events)), 2025, 1)
    assert sorted(r.trip_id for r in trips.collect()) == ["a", "b", "c"]

    # windows compared as UTC strings — collect() would convert to the machine's local zone
    metrics = (zone_metrics(trips, zones, "5 minutes", "30 minutes")
               .withColumn("start", F.date_format("window_start", "yyyy-MM-dd HH:mm"))
               .withColumn("end", F.date_format("window_end", "HH:mm")))
    out = {r.start: r for r in metrics.collect()}
    first = out["2025-01-05 09:00"]
    assert first.trips == 2 and first.avg_fare == 15.0
    assert first.pu_zone == "Midtown Center" and first.end == "09:05"
    assert out["2025-01-05 09:05"].trips == 1


def test_cash_tips_do_not_count_toward_tip_pct(spark):
    events = [
        to_event(_bronze(datetime(2025, 1, 5, 9, 1), datetime(2025, 1, 5, 9, 15), fare=10.0, tip=2.0), "yellow", "card"),
        to_event(_bronze(datetime(2025, 1, 5, 9, 2), datetime(2025, 1, 5, 9, 16), fare=10.0, tip=0.0, payment=2), "yellow", "cash"),
    ]
    zones = spark.createDataFrame([(161, "Manhattan", "Midtown Center")], "loc_id int, borough string, zone string")
    [r] = zone_metrics(clean(parse(_kafka_df(spark, events)), 2025, 1), zones, "5 minutes", "30 minutes").collect()
    assert r.trips == 2 and r.avg_card_tip_pct == 20.0
