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

# fhvhv/fhv have no passenger_count field at all (see curate/clean.py
# normalise()) — it's null for every row, so including it here would make
# .dropna() below discard the entire dataset instead of just bad rows.
NO_PASSENGER_COUNT = {"fhvhv", "fhv"}


def build_pipeline(features_num: list[str], max_iter: int = 30) -> Pipeline:
    """Index + one-hot the boroughs, assemble one feature vector, fit a GBT."""
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_i", handleInvalid="keep") for c in FEATURES_CAT]
    ohe = OneHotEncoder(inputCols=[f"{c}_i" for c in FEATURES_CAT], outputCols=[f"{c}_v" for c in FEATURES_CAT])
    asm = VectorAssembler(inputCols=features_num + [f"{c}_v" for c in FEATURES_CAT], outputCol="features")
    gbt = GBTRegressor(featuresCol="features", labelCol=LABEL, maxIter=max_iter, maxDepth=5, seed=42)
    return Pipeline(stages=[*idx, ohe, asm, gbt])


def group_importances(importances: list[float], slots: dict[int, str],
                      features: list[str]) -> list[dict]:
    """One importance per input feature, not per vector slot.

    The assembled feature vector is wider than the feature list: each
    one-hot-encoded borough becomes one slot per borough (named e.g.
    "pu_borough_v_Manhattan" in the vector's metadata). GBT scores every
    slot, so a categorical feature's importance is the sum over its slots.
    `slots` maps slot index -> slot name; each slot is credited to the
    feature whose name it starts with (longest match, so a feature named
    like the prefix of another can't steal its slots)."""
    by_len = sorted(features, key=len, reverse=True)
    totals = {f: 0.0 for f in features}
    for i, name in slots.items():
        owner = next((f for f in by_len if name == f or name.startswith(f + "_")), None)
        if owner is None:
            raise ValueError(f"feature vector slot {i} ({name!r}) matches no input feature")
        totals[owner] += float(importances[i])
    return [{"feature": f, "importance": round(v, 4)}
            for f, v in sorted(totals.items(), key=lambda t: -t[1])]


def vector_slots(df, col: str = "features") -> dict[int, str]:
    """Slot index -> name, from the ML attribute metadata VectorAssembler
    attaches to its output column."""
    attrs = df.schema[col].metadata["ml_attr"]["attrs"]
    return {a["idx"]: a["name"] for group in attrs.values() for a in group}


def feature_importance(model, scored) -> list[dict]:
    """Feature importance keyed by the original input names — numeric
    columns as-is, each borough one-hot vector summed back to its column.
    `scored` is any output of model.transform(); only its schema is read."""
    names = model.stages[-2].getInputCols()       # the VectorAssembler's inputs
    grouped = group_importances(list(model.stages[-1].featureImportances.toArray()),
                                vector_slots(scored), names)
    return [{"feature": g["feature"].removesuffix("_v"), "importance": g["importance"]}
            for g in grouped]


def run(dataset: str = "yellow", sample: float = 1.0, max_iter: int = 30) -> None:
    spark = get_spark("UrbanFlow-model")
    print(describe(spark))
    features_num = [c for c in FEATURES_NUM
                     if c != "passenger_count" or dataset not in NO_PASSENGER_COUNT]
    df = curated(spark, dataset).select(*features_num, *FEATURES_CAT, LABEL, "pickup_ts", "speed_mph").dropna()
    if sample < 1.0:
        df = df.sample(False, sample, seed=42)

    # ---- time-based split, NOT random ----
    cutoff = df.selectExpr("percentile_approx(unix_timestamp(pickup_ts), 0.8) as c").first()["c"]
    train = df.filter(F.unix_timestamp("pickup_ts") <= cutoff)
    test = df.filter(F.unix_timestamp("pickup_ts") > cutoff)
    n_train, n_test = train.count(), test.count()
    print(f"train {n_train:,} | test {n_test:,} | split at {cutoff}")

    # ---- naive baseline: distance / mean speed ----
    mean_speed = train.selectExpr("avg(speed_mph) s").first()["s"]
    base = test.withColumn("pred", F.col("trip_distance") / F.lit(mean_speed) * 60.0)
    ev = RegressionEvaluator(labelCol=LABEL, predictionCol="pred")
    base_rmse = ev.setMetricName("rmse").evaluate(base)
    base_mae = ev.setMetricName("mae").evaluate(base)

    # ---- model ----
    model = build_pipeline(features_num, max_iter).fit(train)

    pred = model.transform(test)
    ev2 = RegressionEvaluator(labelCol=LABEL, predictionCol="prediction")
    rmse = ev2.setMetricName("rmse").evaluate(pred)
    mae = ev2.setMetricName("mae").evaluate(pred)
    r2 = ev2.setMetricName("r2").evaluate(pred)

    result = {
        "baseline": {"method": "distance / mean training speed", "rmse_min": round(base_rmse, 3), "mae_min": round(base_mae, 3)},
        "gbt": {"rmse_min": round(rmse, 3), "mae_min": round(mae, 3), "r2": round(r2, 4), "max_iter": max_iter},
        "improvement_pct": round(100 * (base_rmse - rmse) / base_rmse, 2),
        "feature_importance": feature_importance(model, pred),
        "train_rows": n_train, "test_rows": n_test,
    }
    out = config.GOLD / "model_results.json"
    out.write_text(json.dumps(result, indent=2))
    model.write().overwrite().save(str(config.MODELS / "duration_gbt"))

    print(f"\n  baseline RMSE {base_rmse:6.2f} min")
    print(f"  GBT      RMSE {rmse:6.2f} min   (R2 {r2:.3f})")
    print(f"  improvement over baseline: {result['improvement_pct']}%")
    print(f"  -> {out}")
    spark.stop()


def rescore_importance() -> None:
    """Recompute feature_importance in an existing model_results.json from
    the saved model, without retraining — for results written before
    feature_importance summed one-hot slots (they labelled the first few
    vector slots, which mis-credits the borough features)."""
    from pyspark.ml import PipelineModel
    spark = get_spark("UrbanFlow-importance")
    model = PipelineModel.load(str(config.MODELS / "duration_gbt"))
    one = spark.createDataFrame([{"trip_distance": 1.0, "pickup_hour": 0, "pickup_dow": 1,
                                  "passenger_count": 1.0, "pu_borough": "Manhattan",
                                  "do_borough": "Manhattan"}])
    out = config.GOLD / "model_results.json"
    result = json.loads(out.read_text())
    result["feature_importance"] = feature_importance(model, model.transform(one))
    out.write_text(json.dumps(result, indent=2))
    for f in result["feature_importance"]:
        print(f"  {f['feature']:<16} {f['importance']:.4f}")
    print(f"  -> {out}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Trip-duration model with an honest baseline")
    ap.add_argument("--dataset", default="yellow")
    ap.add_argument("--sample", type=float, default=1.0)
    ap.add_argument("--max-iter", type=int, default=30)
    ap.add_argument("--importance-only", action="store_true",
                    help="recompute feature_importance from the saved model, no retraining")
    a = ap.parse_args()
    if a.importance_only:
        rescore_importance()
    else:
        run(a.dataset, a.sample, a.max_iter)
