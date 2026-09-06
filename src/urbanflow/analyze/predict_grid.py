"""Stage S4b — score the trained duration model over a representative grid
of trips, so the dashboard can offer "type in a trip, get a predicted
duration" without ever loading Spark into that process.

The dashboard is deliberately Spark-free (see dashboard/app.py — it reads
gold through DuckDB only). Scoring the saved MLlib model live, per request,
would need Spark in that process, breaking that boundary. Instead: score a
small grid once, here, in Spark, and let the dashboard do an exact-match
DuckDB lookup against the result — the dropdowns only ever offer values
that are actually in the grid, so there is no nearest-match fuzziness.
"""
from __future__ import annotations
import argparse
from itertools import product
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from .. import config
from ..session import get_spark, describe

BOROUGHS = ["Bronx", "Brooklyn", "EWR", "Manhattan", "Queens", "Staten Island"]
HOURS = list(range(24))
DOWS = {4: "Weekday", 7: "Weekend"}          # Spark convention: 1=Sun..7=Sat
DISTANCES = [float(d) for d in range(1, 31)]  # 1-30 miles, 1mi steps — dense enough to
                                               # read as close to continuous, not "pick one of 5"
PASSENGER_COUNT_DEFAULT = 1.0                 # fixed — least important feature, not worth a picker


def run(dataset: str = "yellow") -> None:
    spark = get_spark("UrbanFlow-predict-grid")
    print(describe(spark))

    model_path = config.MODELS / "duration_gbt"
    if not model_path.exists():
        raise FileNotFoundError(f"{model_path} missing — run `make model` first")
    model = PipelineModel.load(str(model_path))

    rows = [
        {"pickup_hour": h, "pickup_dow": d, "trip_distance": dist,
         "pu_borough": pu, "do_borough": do, "passenger_count": PASSENGER_COUNT_DEFAULT}
        for h, d, dist, pu, do in product(HOURS, DOWS, DISTANCES, BOROUGHS, BOROUGHS)
    ]
    grid = spark.createDataFrame(rows)
    pred = (model.transform(grid)
            .select("pickup_hour", "pickup_dow", "trip_distance", "pu_borough", "do_borough",
                    F.round("prediction", 1).alias("predicted_duration_min")))

    out = config.GOLD / "duration_predictions"
    n = pred.count()
    pred.coalesce(1).write.mode("overwrite").parquet(str(out))
    print(f"  {n:,} grid predictions -> {out}")

    # Real average distance per route, from the curated data — used only to give
    # the dashboard's distance picker a sensible starting point instead of a
    # blind guess. Not part of the prediction grid itself.
    curated = spark.read.parquet(str(config.CURATED / dataset))
    route_dist = (curated.groupBy("pu_borough", "do_borough")
                  .agg(F.round(F.avg("trip_distance"), 1).alias("avg_distance")))
    route_out = config.GOLD / "route_avg_distance"
    route_dist.coalesce(1).write.mode("overwrite").parquet(str(route_out))
    print(f"  route averages -> {route_out}")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Score the trained model over a representative trip grid")
    ap.add_argument("--dataset", default="yellow")
    a = ap.parse_args()
    run(a.dataset)
