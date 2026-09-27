-- 01_create_tables.sql : Hive's view of the UrbanFlow data lake in HDFS.
--
-- Every table here is EXTERNAL: Hive stores only the schema and the HDFS
-- location in its metastore (Postgres). The Parquet files stay exactly where
-- `make hadoop-load` put them, and DROP TABLE never deletes them.
-- Safe to re-run: every table is dropped and re-declared.

CREATE DATABASE IF NOT EXISTS urbanflow
  COMMENT 'NYC TLC trips: bronze -> silver -> gold, as loaded into HDFS'
  LOCATION '/urbanflow/hive/urbanflow.db';
USE urbanflow;

-- Integer-like columns are declared with the WIDER type (BIGINT, DOUBLE)
-- because the two data sources disagree: real TLC files store the zone ids as
-- 32-bit ints and passenger_count as a 64-bit int, `make synth` writes 64-bit
-- ids and a double. Hive widens on read, so one DDL serves both.

-- ---------------------------------------------------------------- bronze
-- The raw TLC file, unmodified. Column names are the TLC's own.
-- (cbd_congestion_fee exists only in 2025+ files; older or synthetic files read it as NULL.)
DROP TABLE IF EXISTS bronze_yellow_trips;
CREATE EXTERNAL TABLE bronze_yellow_trips (
  VendorID              BIGINT,
  tpep_pickup_datetime  TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count       DOUBLE,
  trip_distance         DOUBLE,
  RatecodeID            DOUBLE,
  store_and_fwd_flag    STRING,
  PULocationID          BIGINT,
  DOLocationID          BIGINT,
  payment_type          BIGINT,
  fare_amount           DOUBLE,
  extra                 DOUBLE,
  mta_tax               DOUBLE,
  tip_amount            DOUBLE,
  tolls_amount          DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount          DOUBLE,
  congestion_surcharge  DOUBLE,
  Airport_fee           DOUBLE,
  cbd_congestion_fee    DOUBLE
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION '/urbanflow/bronze/yellow';
MSCK REPAIR TABLE bronze_yellow_trips;

-- The 265-row zone lookup, straight from the CSV (every field is quoted).
DROP TABLE IF EXISTS bronze_zones;
CREATE EXTERNAL TABLE bronze_zones (
  LocationID   STRING,
  Borough      STRING,
  Zone         STRING,
  service_zone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/urbanflow/bronze/zones'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- ---------------------------------------------------------------- silver
-- The cleaned layer Spark wrote with partitionBy("year", "month").
-- year/month are NOT inside the files: Hive reads them from the directory
-- names (year=2025/month=1), which is what makes partition pruning possible.
DROP TABLE IF EXISTS silver_trips;
CREATE EXTERNAL TABLE silver_trips (
  pickup_ts       TIMESTAMP,
  dropoff_ts      TIMESTAMP,
  duration_s      BIGINT,
  duration_min    DOUBLE,
  trip_distance   DOUBLE,
  speed_mph       DOUBLE,
  passenger_count DOUBLE,
  fare_amount     DOUBLE,
  tip_amount      DOUBLE,
  total_amount    DOUBLE,
  payment_type    BIGINT,
  is_card         BOOLEAN,
  tip_pct         DOUBLE,
  pickup_hour     INT,
  pickup_dow      INT,
  is_weekend      BOOLEAN,
  PULocationID    BIGINT,
  DOLocationID    BIGINT,
  pu_borough      STRING,
  pu_zone         STRING,
  do_borough      STRING,
  do_zone         STRING,
  dataset         STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/urbanflow/silver/yellow';
-- Discover the year=/month= folders already in HDFS and register them as partitions.
MSCK REPAIR TABLE silver_trips;

-- ---------------------------------------------------------------- gold (yellow, built by `make gold`)
DROP TABLE IF EXISTS gold_demand_by_zone_hour;
CREATE EXTERNAL TABLE gold_demand_by_zone_hour (
  pu_borough STRING, pu_zone STRING, pickup_hour INT, is_weekend BOOLEAN,
  trips BIGINT, avg_distance DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/demand_by_zone_hour';

DROP TABLE IF EXISTS gold_daily_kpis;
CREATE EXTERNAL TABLE gold_daily_kpis (
  d DATE, trips BIGINT, revenue DOUBLE, avg_fare DOUBLE,
  avg_distance DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/daily_kpis';

DROP TABLE IF EXISTS gold_tipping;
CREATE EXTERNAL TABLE gold_tipping (
  pu_borough STRING, is_card BOOLEAN, trips BIGINT, avg_tip_pct DOUBLE,
  avg_fare DOUBLE, pct_trips_with_tip DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/tipping';

DROP TABLE IF EXISTS gold_speed_by_hour;
CREATE EXTERNAL TABLE gold_speed_by_hour (
  pu_borough STRING, pickup_hour INT, is_weekend BOOLEAN, trips BIGINT,
  avg_speed_mph DOUBLE, median_speed_mph DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/speed_by_hour';

DROP TABLE IF EXISTS gold_od_matrix;
CREATE EXTERNAL TABLE gold_od_matrix (
  pu_borough STRING, pu_zone STRING, do_borough STRING, do_zone STRING,
  trips BIGINT, avg_fare DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/od_matrix';

DROP TABLE IF EXISTS gold_airport_flows;
CREATE EXTERNAL TABLE gold_airport_flows (
  pickup_hour INT, is_weekend BOOLEAN, trips BIGINT, avg_fare DOUBLE, avg_distance DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/airport_flows';

DROP TABLE IF EXISTS gold_earnings_by_area_hour;
CREATE EXTERNAL TABLE gold_earnings_by_area_hour (
  pu_borough STRING, pickup_hour INT, is_weekend BOOLEAN, trips BIGINT,
  avg_fare DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/yellow/earnings_by_area_hour';

-- ---------------------------------------------------------------- gold (Tier 2: real FHVHV, 243.5M trips)
-- Loaded only when `make hadoop-load TIER2_GOLD=...` points at the real gold
-- tables; otherwise these tables exist but are empty.
DROP TABLE IF EXISTS tier2_daily_kpis;
CREATE EXTERNAL TABLE tier2_daily_kpis (
  d DATE, trips BIGINT, revenue DOUBLE, avg_fare DOUBLE,
  avg_distance DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/fhvhv/daily_kpis';

DROP TABLE IF EXISTS tier2_speed_by_hour;
CREATE EXTERNAL TABLE tier2_speed_by_hour (
  pu_borough STRING, pickup_hour INT, is_weekend BOOLEAN, trips BIGINT,
  avg_speed_mph DOUBLE, median_speed_mph DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/fhvhv/speed_by_hour';

DROP TABLE IF EXISTS tier2_od_matrix;
CREATE EXTERNAL TABLE tier2_od_matrix (
  pu_borough STRING, pu_zone STRING, do_borough STRING, do_zone STRING,
  trips BIGINT, avg_fare DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/fhvhv/od_matrix';

DROP TABLE IF EXISTS tier2_airport_flows;
CREATE EXTERNAL TABLE tier2_airport_flows (
  pickup_hour INT, is_weekend BOOLEAN, trips BIGINT, avg_fare DOUBLE, avg_distance DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/fhvhv/airport_flows';

DROP TABLE IF EXISTS tier2_demand_by_zone_hour;
CREATE EXTERNAL TABLE tier2_demand_by_zone_hour (
  pu_borough STRING, pu_zone STRING, pickup_hour INT, is_weekend BOOLEAN,
  trips BIGINT, avg_distance DOUBLE, avg_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/fhvhv/demand_by_zone_hour';

-- The trained duration model's predictions over a trip grid (the ML output).
DROP TABLE IF EXISTS tier2_duration_predictions;
CREATE EXTERNAL TABLE tier2_duration_predictions (
  pickup_hour BIGINT, pickup_dow BIGINT, trip_distance DOUBLE,
  pu_borough STRING, do_borough STRING, predicted_duration_min DOUBLE)
STORED AS PARQUET LOCATION '/urbanflow/gold/fhvhv/duration_predictions';

-- ---------------------------------------------------------------- MapReduce output
-- Written by `make hadoop-mr` (Hadoop Streaming): borough TAB zone TAB hour TAB trips.
DROP TABLE IF EXISTS mr_zone_hour_counts;
CREATE EXTERNAL TABLE mr_zone_hour_counts (
  pu_borough STRING, pu_zone STRING, pickup_hour INT, trips BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/urbanflow/mr/zone_hour_counts';

SHOW TABLES;
SHOW PARTITIONS silver_trips;
