-- 04_analytics.sql : questions answered in HiveQL over the HDFS data lake.
USE urbanflow;

-- ------------------------------------------------------------------ data quality, bronze -> silver
-- How many raw TLC rows survived the seven cleaning rules?
-- (Spark's curate stage reports the same numbers in data/curated/quality_report.json.)
SELECT b.raw_rows, s.clean_rows, b.raw_rows - s.clean_rows AS removed,
       ROUND(100 * (b.raw_rows - s.clean_rows) / b.raw_rows, 3) AS removed_pct
FROM (SELECT COUNT(*) AS raw_rows FROM bronze_yellow_trips) b
CROSS JOIN (SELECT COUNT(*) AS clean_rows FROM silver_trips) s;

-- ------------------------------------------------------------------ partition pruning
-- silver_trips is partitioned by (year, month). A filter on those columns
-- lets Hive open only the matching folders; EXPLAIN DEPENDENCY lists them.
EXPLAIN DEPENDENCY
SELECT COUNT(*) FROM silver_trips WHERE year = 2025 AND month = 1;

-- A join with the bronze zone lookup (a CSV table) in HiveQL: trips per borough.
SELECT z.Borough AS borough, COUNT(*) AS trips
FROM silver_trips t
JOIN bronze_zones z ON t.PULocationID = CAST(z.LocationID AS BIGINT)
GROUP BY z.Borough
ORDER BY trips DESC;

-- ------------------------------------------------------------------ Tier 2: real FHVHV, 243.5M trips
-- These read the real Tier 2 gold tables (empty unless loaded with TIER2_GOLD=...).

-- Rush hour, measured: Manhattan's weekday average speed by hour.
SELECT pickup_hour, trips, avg_speed_mph
FROM tier2_speed_by_hour
WHERE pu_borough = 'Manhattan' AND NOT is_weekend
ORDER BY pickup_hour;

-- The whole year's totals, straight from the daily KPI table.
SELECT COUNT(*) AS days, SUM(trips) AS trips, ROUND(SUM(revenue) / 1e9, 2) AS revenue_billion_usd,
       ROUND(SUM(revenue) / SUM(trips), 2) AS avg_fare
FROM tier2_daily_kpis;

-- The five busiest origin -> destination zone pairs.
SELECT pu_zone, do_zone, trips, avg_fare, avg_duration_min
FROM tier2_od_matrix
ORDER BY trips DESC
LIMIT 5;

-- The trained GBT duration model's output: a 5-mile Manhattan -> Manhattan trip
-- on a Wednesday (pickup_dow 4), by hour of day.
SELECT pickup_hour, ROUND(predicted_duration_min, 1) AS predicted_min
FROM tier2_duration_predictions
WHERE pu_borough = 'Manhattan' AND do_borough = 'Manhattan'
  AND pickup_dow = 4 AND trip_distance = 5.0
ORDER BY pickup_hour;
