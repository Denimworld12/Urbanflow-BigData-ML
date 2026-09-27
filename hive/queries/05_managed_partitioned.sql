-- 05_managed_partitioned.sql : a MANAGED Hive table, stored as ORC and
-- partitioned by borough with a dynamic-partition insert.
--
-- Contrast with 01_create_tables.sql: those EXTERNAL tables only point at
-- files Spark wrote. This one Hive owns - it writes the files itself under the
-- managed warehouse (/user/hive/warehouse/urbanflow.db/...), and DROP TABLE
-- deletes them. Since Hive 3 a managed table is transactional (ACID): Hive
-- tracks every write as a transaction in the metastore database, which needs
-- the DbTxnManager below.
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
USE urbanflow;

DROP TABLE IF EXISTS zone_hour_demand;
CREATE TABLE zone_hour_demand (
  pu_zone     STRING,
  pickup_hour INT,
  is_weekend  BOOLEAN,
  trips       BIGINT,
  avg_fare    DOUBLE
)
PARTITIONED BY (pu_borough STRING)
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

-- Dynamic partitioning: Hive creates one folder per distinct pu_borough value
-- it sees in the SELECT (the partition column must come last).
INSERT OVERWRITE TABLE zone_hour_demand PARTITION (pu_borough)
SELECT pu_zone, pickup_hour, is_weekend, COUNT(*), ROUND(AVG(total_amount), 2), pu_borough
FROM silver_trips
GROUP BY pu_borough, pu_zone, pickup_hour, is_weekend;

SHOW PARTITIONS zone_hour_demand;

-- Where Hive put it, and what kind of table it is.
DESCRIBE FORMATTED zone_hour_demand;

-- Partition pruning in action: only the pu_borough=Queens folder is read.
SELECT pu_zone, SUM(trips) AS trips, ROUND(SUM(trips * avg_fare) / SUM(trips), 2) AS avg_fare
FROM zone_hour_demand
WHERE pu_borough = 'Queens'
GROUP BY pu_zone
ORDER BY trips DESC
LIMIT 5;
