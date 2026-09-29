"""Real-time path, step 2 — Spark Structured Streaming over the Kafka trips.

The same engine and the same cleaning code as the batch pipeline, run
incrementally. Every trigger (default 10 s) Spark reads the new Kafka
offsets, and:

  1. PARSE the JSON event into the curated column names.
  2. CLEAN with curate/clean.derive + curate/rules.rules — the identical seven
     rules the batch layer applies. This must happen BEFORE the watermark: one
     stray 2098 timestamp would otherwise push event time 73 years ahead and
     every real trip after it would be discarded as "late".
  3. ENRICH with a broadcast join to the zone lookup (stream-static join) —
     the same broadcast join curate uses.
  4. WINDOW by pickup time: trips, avg fare, avg duration per pickup zone per
     5-minute window. withWatermark("pickup_ts", "30 minutes") tells Spark how
     late an event may be and still count, which is also what lets it forget
     old windows so its state does not grow forever.
  5. WRITE two sinks, each its own streaming query with its own checkpoint:
       parquet  data/stream/zone_metrics — append mode: a window is written
                once, when the watermark passes its end, so it is final.
                Exactly-once via the checkpoint + _spark_metadata log.
       kafka    topic zone-metrics — update mode: every trigger emits the
                windows whose numbers changed, i.e. early running answers.

Spark tracks its Kafka position in the checkpoint, not in a consumer group,
so standard Kafka tools would not see its lag. A listener mirrors each
query's committed offsets to a consumer group (urbanflow-<query>) purely so
`kafka-consumer-groups.sh` and kafka-exporter can show lag. Restarting the
job still resumes from the checkpoint, which is the source of truth.

    python -m urbanflow.stream.job                   # run until Ctrl-C
    python -m urbanflow.stream.job --available-now   # drain the topic, then stop
"""
from __future__ import annotations
import argparse, json, os
import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import broadcast
from pyspark.sql.streaming import StreamingQueryListener
from .. import config
from ..curate.clean import derive, read_zones
from ..curate.rules import rules
from ..session import get_spark, describe

EVENT_SCHEMA = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("dataset", T.StringType()),
    T.StructField("pickup_ts", T.TimestampType()),
    T.StructField("dropoff_ts", T.TimestampType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("passenger_count", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("payment_type", T.IntegerType()),
])

METRICS_PATH = config.STREAM / "zone_metrics"
CHECKPOINTS = config.STREAM / "_checkpoints"


def parse(kafka_df: DataFrame) -> DataFrame:
    """Kafka records (binary key/value) -> one typed row per trip event."""
    return (kafka_df
            .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                    F.col("timestamp").alias("kafka_ts"))
            .select("e.*", "kafka_ts"))


def clean(events: DataFrame, year: int, month: int) -> DataFrame:
    """The batch layer's derive() + all seven rules, unchanged."""
    df = derive(events)
    keep = None
    for _, _, cond in rules(year, month):
        keep = cond if keep is None else (keep & cond)
    return df.filter(keep)


def zone_metrics(trips: DataFrame, zones: DataFrame, window: str, watermark: str) -> DataFrame:
    """Per pickup zone, per event-time window: the live metrics."""
    z = broadcast(zones.select(F.col("loc_id").alias("PULocationID"),
                               F.col("borough").alias("pu_borough"),
                               F.col("zone").alias("pu_zone")))
    return (trips
            .withWatermark("pickup_ts", watermark)
            .join(z, "PULocationID", "left")
            .groupBy(F.window("pickup_ts", window).alias("w"), "PULocationID", "pu_borough", "pu_zone")
            .agg(F.count("*").alias("trips"),
                 F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                 F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
                 F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                 F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                 # tips are only recorded for card payments — see schema/curated.md
                 F.round(F.avg(F.when(F.col("is_card"), F.col("tip_pct"))), 2).alias("avg_card_tip_pct"))
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                    "PULocationID", "pu_borough", "pu_zone", "trips", "avg_fare",
                    "avg_duration_min", "avg_distance", "total_revenue", "avg_card_tip_pct"))


class Progress(StreamingQueryListener):
    """Prints one line per micro-batch and mirrors offsets to a consumer group."""

    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap
        self._consumers: dict = {}

    def onQueryStarted(self, event):
        print(f"  started query '{event.name}' (id {event.id})")

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows == 0:
            return
        state = p.stateOperators[0] if p.stateOperators else None
        src = p.sources[0] if p.sources else None
        behind = (src.metrics or {}).get("maxOffsetsBehindLatest") if src else None
        print(f"  [{p.name}] batch {p.batchId}: {p.numInputRows:,} rows "
              f"({p.processedRowsPerSecond:,.0f} rows/s) "
              f"watermark={p.eventTime.get('watermark', '-')} "
              f"state_rows={state.numRowsTotal if state else '-'} "
              f"late_dropped={state.numRowsDroppedByWatermark if state else '-'} "
              f"offsets_behind={behind if behind is not None else '-'}")
        if src is not None:
            self._mirror(p.name, src.endOffset)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"  query {event.id} stopped" + (f": {event.exception}" if event.exception else ""))

    def _mirror(self, name: str, end_offset: str) -> None:
        """Commit Spark's processed offsets to Kafka group `urbanflow-<name>`
        so lag is visible to Kafka tooling. Best effort, never fails the job."""
        try:
            from kafka import KafkaConsumer, TopicPartition
            from kafka.structs import OffsetAndMetadata
            group = f"urbanflow-{name}"
            if group not in self._consumers:
                self._consumers[group] = KafkaConsumer(
                    bootstrap_servers=self.bootstrap, group_id=group, enable_auto_commit=False)
            offsets = {TopicPartition(topic, int(part)): OffsetAndMetadata(off, "", -1)
                       for topic, parts in json.loads(end_offset).items()
                       for part, off in parts.items()}
            self._consumers[group].commit(offsets)
        except Exception as e:                      # noqa: BLE001 — monitoring only
            print(f"  (offset mirror for {name} skipped: {e})")


def build_spark(driver_ui_port: int) -> SparkSession:
    # The Kafka source/sink is not bundled with pyspark; Spark downloads the
    # connector matching its own version (and Scala 2.13) on first run.
    pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    return get_spark(
        "UrbanFlow-stream",
        # Stateful streaming: shuffle partitions = state-store partitions, and
        # the number is frozen into the checkpoint on first run. A handful is
        # plenty for one laptop; the batch default (cores x 8) would mean 64+
        # tiny tasks every 10 seconds.
        shuffle_parts=4,
        extra_conf={
            "spark.jars.packages": pkg,
            "spark.ui.port": str(driver_ui_port),
            # Streaming metrics (input rate, processing rate, latency,
            # watermark) as Prometheus text at :<port>/metrics/prometheus.
            "spark.sql.streaming.metricsEnabled": "true",
            "spark.metrics.namespace": "urbanflow_stream",
            "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
            "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus",
            "spark.ui.prometheus.enabled": "true",
        },
    )


def run(bootstrap: str, topic: str, out_topic: str, year: int, month: int,
        window: str, watermark: str, trigger: str, available_now: bool,
        max_per_trigger: int, sinks: list[str], ui_port: int) -> None:
    spark = build_spark(ui_port)
    print(describe(spark))
    print(f"stream: {topic} @ {bootstrap} -> {', '.join(sinks)} | window={window} "
          f"watermark={watermark} trigger={'availableNow' if available_now else trigger} "
          f"| Spark UI + /metrics/prometheus on :{ui_port}")
    spark.streams.addListener(Progress(bootstrap))

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")        # first run only; then the checkpoint
              .option("maxOffsetsPerTrigger", max_per_trigger)  # bounded batches while catching up
              .load())
    metrics = zone_metrics(clean(parse(source), year, month), read_zones(spark), window, watermark)

    def _trigger(w):
        return w.trigger(availableNow=True) if available_now else w.trigger(processingTime=trigger)

    queries = []
    if "parquet" in sinks:
        queries.append(_trigger(
            metrics.writeStream.queryName("zone_metrics_parquet")
            .format("parquet").outputMode("append")
            .option("path", str(METRICS_PATH))
            .option("checkpointLocation", str(CHECKPOINTS / "zone_metrics_parquet"))).start())
    if "kafka" in sinks:
        queries.append(_trigger(
            metrics.select(F.col("PULocationID").cast("string").alias("key"),
                           F.to_json(F.struct(*metrics.columns)).alias("value"))
            .writeStream.queryName("zone_metrics_kafka")
            .format("kafka").outputMode("update")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("topic", out_topic)
            .option("checkpointLocation", str(CHECKPOINTS / "zone_metrics_kafka"))).start())

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("stopping streaming queries…")
        for q in queries:
            q.stop()
    spark.stop()
    print(f"stream: windowed metrics under {METRICS_PATH}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Spark Structured Streaming: live per-zone trip metrics from Kafka")
    ap.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    ap.add_argument("--topic", default=config.TRIPS_TOPIC)
    ap.add_argument("--out-topic", default=config.METRICS_TOPIC)
    ap.add_argument("--year", type=int, default=2025, help="month being replayed — the out_of_month rule needs it")
    ap.add_argument("--month", type=int, default=1)
    ap.add_argument("--window", default="5 minutes")
    ap.add_argument("--watermark", default="30 minutes", help="how late (in event time) a trip may arrive and still count")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--available-now", action="store_true", help="process everything already in the topic, then stop")
    ap.add_argument("--max-per-trigger", type=int, default=50_000)
    ap.add_argument("--sinks", default="parquet,kafka")
    ap.add_argument("--ui-port", type=int, default=int(os.environ.get("URBANFLOW_STREAM_UI_PORT", 4050)))
    a = ap.parse_args()
    run(a.bootstrap, a.topic, a.out_topic, a.year, a.month, a.window, a.watermark,
        a.trigger, a.available_now, a.max_per_trigger,
        [s.strip() for s in a.sinks.split(",") if s.strip()], a.ui_port)
