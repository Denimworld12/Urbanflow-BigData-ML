"""The duration model's contracts: feature importance per input feature
(not per one-hot slot), and a saved model that scores the Predict grid."""
import random
from datetime import datetime, timedelta
import pytest
from urbanflow.analyze.model import (FEATURES_NUM, build_pipeline, feature_importance,
                                     group_importances)
from urbanflow.analyze import predict_grid
from urbanflow.curate.clean import derive


def test_group_importances_sums_one_hot_slots():
    slots = {0: "trip_distance", 1: "pickup_hour",
             2: "pu_borough_v_Manhattan", 3: "pu_borough_v_Queens",
             4: "do_borough_v_Manhattan", 5: "do_borough_v_Queens"}
    imp = [0.4, 0.1, 0.05, 0.25, 0.1, 0.1]
    got = group_importances(imp, slots, ["trip_distance", "pickup_hour", "pu_borough_v", "do_borough_v"])
    assert got == [{"feature": "trip_distance", "importance": 0.4},
                   {"feature": "pu_borough_v", "importance": 0.3},
                   {"feature": "do_borough_v", "importance": 0.2},
                   {"feature": "pickup_hour", "importance": 0.1}]


def test_group_importances_rejects_unknown_slot():
    with pytest.raises(ValueError, match="matches no input feature"):
        group_importances([1.0], {0: "mystery"}, ["trip_distance"])


@pytest.fixture(scope="module")
def trained(spark):
    """A small model where pickup borough genuinely matters: Manhattan trips
    crawl, so the pu_borough feature has to earn real importance."""
    rng = random.Random(7)
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx"]
    rows = []
    for _ in range(600):
        pu, do = rng.choice(boroughs), rng.choice(boroughs)
        dist = rng.uniform(1, 10)
        mins = dist * (6.0 if pu == "Manhattan" else 2.0) + rng.uniform(0, 1)
        rows.append({"trip_distance": dist, "pickup_hour": rng.randrange(24),
                     "pickup_dow": rng.randrange(1, 8), "passenger_count": 1.0,
                     "pu_borough": pu, "do_borough": do, "duration_min": mins})
    df = spark.createDataFrame(rows)
    model = build_pipeline(FEATURES_NUM, max_iter=5).fit(df)
    return model, model.transform(df)


def test_feature_importance_is_per_input_feature(trained):
    model, scored = trained
    fi = feature_importance(model, scored)
    names = [f["feature"] for f in fi]
    assert sorted(names) == sorted(FEATURES_NUM + ["pu_borough", "do_borough"])
    assert sum(f["importance"] for f in fi) == pytest.approx(1.0, abs=1e-3)
    imp = {f["feature"]: f["importance"] for f in fi}
    assert imp["pu_borough"] > 0.2                    # the planted effect is found
    assert imp["pu_borough"] > imp["do_borough"]      # ...and credited to the right column


def test_model_scores_a_predict_grid_row(spark, trained):
    """predict_grid.py feeds the saved model exactly these columns."""
    model, _ = trained
    row = {"pickup_hour": 8, "pickup_dow": 4, "trip_distance": 5.0, "pu_borough": "EWR",
           "do_borough": "Manhattan", "passenger_count": predict_grid.PASSENGER_COUNT_DEFAULT}
    out = model.transform(spark.createDataFrame([row])).first()
    assert out["prediction"] > 0                     # unseen borough (EWR) still scores


def test_grid_day_codes_match_curated_weekday_weekend(spark):
    """The Predict tab's Weekday/Weekend map onto pickup_dow codes; those
    must mean the same thing derive() means by pickup_dow / is_weekend."""
    wed, sat = datetime(2025, 1, 8, 9), datetime(2025, 1, 11, 9)
    rows = [{"pickup_ts": t, "dropoff_ts": t + timedelta(minutes=10), "trip_distance": 1.0,
             "passenger_count": 1.0, "fare_amount": 5.0, "tip_amount": 0.0,
             "total_amount": 5.0, "payment_type": 1} for t in (wed, sat)]
    got = {r["pickup_dow"]: r["is_weekend"] for r in derive(spark.createDataFrame(rows)).collect()}
    for code, label in predict_grid.DOWS.items():
        assert got[code] == (label == "Weekend")
