"""Spark reads UrbanFlow's silver layer back out of HDFS (`make hadoop-spark`).

The pipeline itself (src/urbanflow/) is unchanged and still reads local
files. This only shows that the same Spark session, pointed at an hdfs:// URL
instead of a local path, reads the same Parquet through the NameNode and
DataNode, and gets the same answer Hive and MapReduce got.
"""
from pyspark.sql import functions as F
from urbanflow.session import get_spark, describe

HDFS = "hdfs://namenode:8020"

spark = get_spark("UrbanFlow-read-hdfs")
print(describe(spark))

silver = spark.read.parquet(f"{HDFS}/urbanflow/silver/yellow")
print(f"\nsilver rows read from {HDFS}/urbanflow/silver/yellow: {silver.count():,}")
print("partitions discovered from the year=/month= folders:",
      [tuple(r) for r in silver.select("year", "month").distinct().orderBy("year", "month").collect()])

print("\ntrips per pickup borough (Spark over HDFS):")
silver.groupBy("pu_borough").count().orderBy(F.desc("count")).show(8, truncate=False)

gold = spark.read.parquet(f"{HDFS}/urbanflow/gold/yellow/daily_kpis")
print("gold daily_kpis total trips (read from HDFS):",
      f"{gold.agg(F.sum('trips')).first()[0]:,}")
spark.stop()
