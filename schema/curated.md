# Curated schema — THE CONTRACT

**Owner:** Member A · **Status:** DRAFT · **Freeze date:** end of week 2

This file is the interface between the three of us. Once it is frozen, Member B
writes aggregations against *this document*, not against whatever A happens to
have produced that day, and Member C builds the dashboard against the gold
tables that follow from it.

**Nobody changes a column name or type without telling the other two.** If a
change is genuinely needed after the freeze, announce it, update this file in
the same commit, and say which downstream code you checked.

Location: `data/curated/{dataset}/year=YYYY/month=M/*.parquet`

| Column | Type | Meaning |
|---|---|---|
| `pickup_ts` | timestamp | Meter start. Normalised from each dataset's own column name. |
| `dropoff_ts` | timestamp | Meter stop. |
| `duration_s` | long | Seconds between pickup and dropoff. |
| `duration_min` | double | Same, in minutes, rounded to 2dp. **ML label.** |
| `trip_distance` | double | Miles, as recorded by the meter. |
| `speed_mph` | double | Derived: distance ÷ duration. Null when duration is 0. |
| `passenger_count` | double | As recorded. Double, not int — the source has nulls. |
| `fare_amount` | double | Metered fare, before extras. |
| `tip_amount` | double | **Card only.** Always 0 for cash — see the note below. |
| `total_amount` | double | Everything the passenger paid. |
| `payment_type` | int | 1 = card, 2 = cash, others rare. |
| `is_card` | boolean | `payment_type == 1`. Convenience for grouping. |
| `tip_pct` | double | `100 * tip_amount / fare_amount`. Null when fare is 0. |
| `pickup_hour` | int | 0–23, UTC. |
| `pickup_dow` | int | 1 = Sunday … 7 = Saturday (Spark convention). |
| `is_weekend` | boolean | Saturday or Sunday. |
| `PULocationID` | int | TLC zone id, pickup. |
| `DOLocationID` | int | TLC zone id, dropoff. |
| `pu_borough` | string | Joined from the zone lookup. |
| `pu_zone` | string | Joined from the zone lookup. |
| `do_borough` | string | Joined from the zone lookup. |
| `do_zone` | string | Joined from the zone lookup. |
| `dataset` | string | `yellow` \| `green` \| `fhvhv` \| `fhv`. |
| `year` | int | **Partition key.** |
| `month` | int | **Partition key.** |

## Guarantees this layer makes

Every row in the curated layer has passed all seven cleaning rules in
`src/urbanflow/curate/rules.py`. Specifically:

* `pickup_ts` falls inside the month named by the partition
* `dropoff_ts > pickup_ts`
* duration between 60 seconds and 6 hours
* distance greater than 0 and at most 100 miles
* `fare_amount` and `total_amount` are non-negative
* implied speed at most 90 mph
* `passenger_count` greater than 0

Downstream code may assume all of the above. It may **not** assume that
`tip_amount` is meaningful for cash trips.

## The cash-tip caveat — read before computing any tip statistic

TLC records tips only when paid by card. For `payment_type = 2` the
`tip_amount` field is always `0.00`, whether or not a tip was given. Any
average taken over all trips is therefore biased downward by roughly the cash
share of the market. Always group by `is_card`, and state the limitation
wherever a tip number appears in the report.
