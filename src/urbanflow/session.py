"""The one place a SparkSession is created. Tuned for an 8 GB laptop.

Why these settings (be ready to explain each in the viva):
  local[N]                 N worker threads. In local mode the driver IS the executor.
  spark.driver.memory      The only memory setting that matters locally. Never > half your RAM.
  spark.executor.memory    Deliberately NOT set — it does nothing in local mode.
  shuffle.partitions       Default is 200, sized for clusters. On 4 cores that means
                           200 tiny tasks and 200 tiny files. 32 is right for a laptop.
  adaptive.enabled         Lets Spark coalesce badly-sized partitions at runtime.
  local.dir                Where shuffle data spills. Needs real free disk.
  extra_conf               Job-specific additions applied last, e.g. the streaming
                           job adds the Kafka connector package and its metrics.
"""
from __future__ import annotations
import os
import sys
from pyspark.sql import SparkSession
from . import config

# Spark launches Python worker subprocesses via $PATH, not the driver's own
# interpreter. Without this, a worker can pick up a different Python (e.g. a
# system install) than the one the venv/pyspark was installed for, and fail
# with opaque py4j errors. Pin workers to the exact interpreter running here —
# this also makes the fix independent of python/python3/python.exe naming
# differences across Windows, macOS and Linux.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _default_cores() -> int:
    n = os.cpu_count() or 4
    return max(2, min(n, 8))


def get_spark(app: str = "UrbanFlow", cores: int | None = None,
              driver_mem: str | None = None, shuffle_parts: int | None = None,
              quiet: bool = True, extra_conf: dict[str, str] | None = None) -> SparkSession:
    cores = cores or int(os.environ.get("URBANFLOW_CORES", _default_cores()))
    driver_mem = driver_mem or os.environ.get("URBANFLOW_DRIVER_MEM", "4g")
    shuffle_parts = shuffle_parts or int(os.environ.get("URBANFLOW_SHUFFLE", cores * 8))

    builder = (
        SparkSession.builder
        .appName(app)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", str(shuffle_parts))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        # Static (Spark's default) means a "overwrite" write to a partitioned
        # path erases every partition, not just the ones in this write — silently
        # destroying every other month already curated. curate_month() writes
        # one month at a time into a partitionBy("year","month") tree, so this
        # must be dynamic or only the last month processed ever survives.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.local.dir", str(config.SPILL))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    if quiet:
        spark.sparkContext.setLogLevel("ERROR")
    return spark


def describe(spark: SparkSession) -> str:
    c = spark.sparkContext
    return (f"Spark {spark.version} | master={c.master} | "
            f"parallelism={c.defaultParallelism} | "
            f"driver.memory={spark.conf.get('spark.driver.memory')} | "
            f"shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
