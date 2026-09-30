-- 02_export_for_mapreduce.sql : a plain-text extract of the silver layer.
--
-- Hadoop Streaming feeds each input LINE to the Python mapper on stdin, so it
-- needs text, not Parquet. Hive writes that text extract straight into HDFS:
-- one line per trip, tab-separated: borough, zone, pickup hour.
-- A NULL zone (TLC ids 264/265, "unknown") is written as \N.
USE urbanflow;

INSERT OVERWRITE DIRECTORY '/urbanflow/staging/trips_tsv'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT pu_borough, pu_zone, pickup_hour
FROM silver_trips;
