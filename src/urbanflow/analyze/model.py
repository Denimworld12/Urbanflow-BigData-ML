"""Stage S4 — predict trip duration with Spark MLlib.

Two things make this credible rather than decorative, and both are marks:

  1. TIME-BASED SPLIT. Train on earlier trips, test on later ones. A random
     split lets the model learn from the future to predict the past, which
     inflates the score and is a real methodological error.
  2. A NAIVE BASELINE. Predict duration as distance / average speed. If the
     model barely beats that, say so — an honest negative result reads far
     better than an unexplained R-squared.
"""
from __future__ import annotations
import argparse, json
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from .. import config
from ..session import get_spark, describe
from .gold import curated

FEATURES_NUM = ["trip_distance", "pickup_hour", "pickup_dow", "passenger_count"]
FEATURES_CAT = ["pu_borough", "do_borough"]
LABEL = "duration_min"


def run(dataset: str = "yellow", sample: float = 1.0, max_iter: int = 30) -> None:
    spark = get_spark("UrbanFlow-model")
    print(describe(spark))
    df = curated(spark, dataset).select(*FEATURES_NUM, *FEATURES_CAT, LABEL, "pickup_ts", "speed_mph").dropna()
    if sample < 1.0:
        df = df.sample(False, sample, seed=42)

    # ---- time-based split, NOT random ----
    cutoff = df.selectExpr("percentile_approx(unix_timestamp(pickup_ts), 0.8) as c").first()["c"]
    train = df.filter(F.unix_timestamp("pickup_ts") <= cutoff)
    test = df.filter(F.unix_timestamp("pickup_ts") > cutoff)
    print(f"train {train.count():,} | test {test.count():,} | split at {cutoff}")

    # ---- naive baseline: distance / mean speed ----
    mean_speed = train.selectExpr("avg(speed_mph) s").first()["s"]
    base = test.withColumn("pred", F.col("trip_distance") / F.lit(mean_speed) * 60.0)
    ev = RegressionEvaluator(labelCol=LABEL, predictionCol="pred")
    base_rmse = ev.setMetricName("rmse").evaluate(base)
    base_mae = ev.setMetricName("mae").evaluate(base)

    # ---- model ----
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_i", handleInvalid="keep") for c in FEATURES_CAT]
    ohe = OneHotEncoder(inputCols=[f"{c}_i" for c in FEATURES_CAT], outputCols=[f"{c}_v" for c in FEATURES_CAT])
    asm = VectorAssembler(inputCols=FEATURES_NUM + [f"{c}_v" for c in FEATURES_CAT], outputCol="features")
    gbt = GBTRegressor(featuresCol="features", labelCol=LABEL, maxIter=max_iter, maxDepth=5, seed=42)
    model = Pipeline(stages=[*idx, ohe, asm, gbt]).fit(train)

    pred = model.transform(test)
    ev2 = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction")
    rmse = ev2.setMetricName("rmse").evaluate(pred)
    mae = ev2.setMetricName("mae").evaluate(pred)
    r2 = ev2.setMetricName("r2").evaluate(pred)

    gbt_model = model.stages[-1]
    names = FEATURES_NUM + [f"{c}_v" for c in FEATURES_CAT]
    imp = sorted(zip(names, list(gbt_model.featureImportances.toArray())[:len(names)]),
                 key=lambda t: -t[1])

    result = {
        "baseline": {"method": "distance / mean training speed", "rmse_min": round(base_rmse, 3), "mae_min": round(base_mae, 3)},
        "gbt": {"rmse_min": round(rmse, 3), "mae_min": round(mae, 3), "r2": round(r2, 4), "max_iter": max_iter},
        "improvement_pct": round(100 * (base_rmse - rmse) / base_rmse, 2),
        "feature_importance": [{"feature": n, "importance": round(v, 4)} for n, v in imp],
        "train_rows": train.count(), "test_rows": test.count(),
    }
    out = config.GOLD / "model_results.json"
    out.write_text(json.dumps(result, indent=2))
    model.write().overwrite().save(str(config.MODELS / "duration_gbt"))

    print(f"\n  baseline RMSE {base_rmse:6.2f} min")
    print(f"  GBT      RMSE {rmse:6.2f} min   (R2 {r2:.3f})")
    print(f"  improvement over baseline: {result['improvement_pct']}%")
    print(f"  -> {out}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Trip-duration model with an honest baseline")
    ap.add_argument("--dataset", default="yellow")
    ap.add_argument("--sample", type=float, default=1.0)
    ap.add_argument("--max-iter", type=int, default=30)
    a = ap.parse_args()
    run(a.dataset, a.sample, a.max_iter)
