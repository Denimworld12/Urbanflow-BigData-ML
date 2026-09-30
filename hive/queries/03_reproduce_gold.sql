-- 03_reproduce_gold.sql : recompute gold answers in HiveQL and check them
-- against the tables Spark wrote.
--
-- Spark (src/urbanflow/analyze/gold.py) and Hive read the SAME silver
-- Parquet files in HDFS, through two different engines. If both give the same
-- numbers, the gold layer is reproducible and neither engine is hiding a bug.
-- Each Hive query below runs as a Tez DAG (a YARN application).
USE urbanflow;

-- ------------------------------------------------------------------ Q1 daily KPIs
-- gold.py: groupBy(to_date(pickup_ts)) -> trips, revenue, avg fare/distance/duration
WITH hive_kpis AS (
  SELECT to_date(pickup_ts)                AS d,
         COUNT(*)                          AS trips,
         ROUND(SUM(total_amount), 2)       AS revenue,
         ROUND(AVG(total_amount), 2)       AS avg_fare,
         ROUND(AVG(trip_distance), 3)      AS avg_distance,
         ROUND(AVG(duration_min), 2)       AS avg_duration_min
  FROM silver_trips
  GROUP BY to_date(pickup_ts)
)
SELECT h.d, h.trips AS hive_trips, g.trips AS spark_trips,
       h.revenue AS hive_revenue, g.revenue AS spark_revenue,
       h.avg_fare AS hive_avg_fare, g.avg_fare AS spark_avg_fare
FROM hive_kpis h JOIN gold_daily_kpis g ON h.d = g.d
ORDER BY h.d
LIMIT 5;

-- Every day, every column: how many agree? (rounded values may differ by one
-- cent from floating-point summation order, so money is compared to 0.01)
WITH hive_kpis AS (
  SELECT to_date(pickup_ts) AS d, COUNT(*) AS trips,
         ROUND(SUM(total_amount), 2) AS revenue, ROUND(AVG(total_amount), 2) AS avg_fare,
         ROUND(AVG(trip_distance), 3) AS avg_distance, ROUND(AVG(duration_min), 2) AS avg_duration_min
  FROM silver_trips GROUP BY to_date(pickup_ts)
)
SELECT COUNT(*)                                                             AS days_compared,
       SUM(IF(h.trips = g.trips, 1, 0))                                     AS trips_equal,
       SUM(IF(ABS(h.revenue - g.revenue) <= 0.01, 1, 0))                    AS revenue_equal,
       SUM(IF(ABS(h.avg_fare - g.avg_fare) <= 0.01, 1, 0))                  AS avg_fare_equal,
       SUM(IF(ABS(h.avg_duration_min - g.avg_duration_min) <= 0.01, 1, 0))  AS avg_duration_equal
FROM hive_kpis h FULL OUTER JOIN gold_daily_kpis g ON h.d = g.d;

-- ------------------------------------------------------------------ Q2 demand by zone and hour
-- gold.py: groupBy(pu_borough, pu_zone, pickup_hour, is_weekend) -> trips
WITH hive_demand AS (
  SELECT pu_borough, pu_zone, pickup_hour, is_weekend, COUNT(*) AS trips
  FROM silver_trips
  GROUP BY pu_borough, pu_zone, pickup_hour, is_weekend
)
SELECT COUNT(*)                                   AS groups_compared,
       SUM(IF(h.trips = g.trips, 1, 0))           AS trips_equal,
       SUM(IF(h.trips IS NULL OR g.trips IS NULL, 1, 0)) AS only_in_one_side
FROM hive_demand h
FULL OUTER JOIN gold_demand_by_zone_hour g
  ON  h.pu_borough <=> g.pu_borough AND h.pu_zone <=> g.pu_zone     -- <=> : NULL-safe equality
  AND h.pickup_hour = g.pickup_hour AND h.is_weekend = g.is_weekend;

-- The answer itself: the ten busiest pickup zone-hours on weekdays.
SELECT pu_borough, pu_zone, pickup_hour, trips, avg_duration_min
FROM gold_demand_by_zone_hour
WHERE NOT is_weekend
ORDER BY trips DESC
LIMIT 10;

-- ------------------------------------------------------------------ Q3 the cash-tip finding
-- gold.py tipping(): card and cash reported separately, because TLC records
-- tip_amount only for card payments. Recomputed here from silver.
SELECT pu_borough,
       IF(is_card, 'card', 'cash/other')                                   AS payment,
       COUNT(*)                                                            AS trips,
       ROUND(AVG(tip_pct), 2)                                              AS avg_tip_pct,
       ROUND(SUM(IF(tip_amount > 0, 1, 0)) / COUNT(*) * 100, 2)            AS pct_trips_with_tip
FROM silver_trips
WHERE pu_borough IN ('Manhattan', 'Queens', 'Brooklyn')
GROUP BY pu_borough, is_card
ORDER BY pu_borough, payment;

-- Same rows straight from Spark's gold table, for comparison.
SELECT pu_borough, IF(is_card, 'card', 'cash/other') AS payment, trips, avg_tip_pct, pct_trips_with_tip
FROM gold_tipping
WHERE pu_borough IN ('Manhattan', 'Queens', 'Brooklyn')
ORDER BY pu_borough, payment;

-- ------------------------------------------------------------------ Q4 MapReduce vs Spark
-- The Hadoop Streaming job (make hadoop-mr) counted trips per zone per hour
-- with Python mapper/reducer. Spark's gold table splits the same counts by
-- is_weekend, so sum those two halves and compare every (zone, hour).
WITH spark_counts AS (
  SELECT pu_borough, pu_zone, pickup_hour, SUM(trips) AS trips
  FROM gold_demand_by_zone_hour
  GROUP BY pu_borough, pu_zone, pickup_hour
)
SELECT COUNT(*)                                          AS zone_hours_compared,
       SUM(IF(m.trips = s.trips, 1, 0))                  AS counts_equal,
       SUM(IF(m.trips IS NULL OR s.trips IS NULL, 1, 0)) AS only_in_one_side,
       SUM(m.trips)                                      AS mapreduce_total_trips,
       SUM(s.trips)                                      AS spark_total_trips
FROM mr_zone_hour_counts m
FULL OUTER JOIN spark_counts s
  ON m.pu_borough <=> s.pu_borough AND m.pu_zone <=> s.pu_zone AND m.pickup_hour = s.pickup_hour;
