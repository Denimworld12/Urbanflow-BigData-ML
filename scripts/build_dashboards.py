"""Generate the Grafana dashboards in monitoring/grafana/dashboards/.

Grafana dashboard JSON is long and repetitive, so the dashboards are written
here as short panel lists and rendered to JSON. Grafana only ever reads the
JSON; this script is just how we keep it readable.

    python3 scripts/build_dashboards.py     # or: make monitor-dashboards

Standard library only. tests/test_monitoring.py fails if the committed JSON
is out of date with this file.
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "monitoring" / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}

# Compose project names of the opt-in stacks. Per-container CPU/memory panels
# on each stack dashboard filter cAdvisor metrics by these.
PROJECTS = {
    "hadoop": "urbanflow-hadoop",
    "hbase": "urbanflow-hbase",
    "streaming": "urbanflow-streaming",
}

UP_MAPPING = [{"type": "value", "options": {
    "0": {"text": "DOWN", "color": "red"},
    "1": {"text": "UP", "color": "green"}}}]


def _target(expr: str, legend: str = "", instant: bool = False, ref: str = "A") -> dict:
    t = {"datasource": DS, "expr": expr, "refId": ref, "legendFormat": legend or "__auto"}
    if instant:
        t.update(instant=True, range=False, format="table")
    return t


def _targets(queries) -> list[dict]:
    if isinstance(queries, str):
        queries = [(queries, "")]
    return [_target(q, legend, ref=chr(65 + i)) for i, (q, legend) in enumerate(queries)]


def stat(title, queries, unit="short", w=4, h=4, mappings=None, thresholds=None,
         desc="", text_mode="auto", color_mode="value"):
    return {
        "type": "stat", "title": title, "description": desc, "datasource": DS,
        "_w": w, "_h": h, "targets": _targets(queries),
        "fieldConfig": {"defaults": {
            "unit": unit, "mappings": mappings or [],
            "noValue": "stack not running",
            "thresholds": {"mode": "absolute", "steps": thresholds or [
                {"color": "blue", "value": None}]},
        }, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": color_mode, "graphMode": "none", "textMode": text_mode,
                    "justifyMode": "auto", "orientation": "auto"},
    }


def timeseries(title, queries, unit="short", w=12, h=8, desc="", stack=False):
    return {
        "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "_w": w, "_h": h, "targets": _targets(queries),
        "fieldConfig": {"defaults": {
            "unit": unit, "noValue": "stack not running",
            "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10,
                       "showPoints": "never", "spanNulls": True,
                       "stacking": {"mode": "normal" if stack else "none", "group": "A"}},
        }, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                               "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def table(title, expr, w=24, h=8, desc="", rename=None, mappings=None, hide=(), sort_by=""):
    excl = {"Time": True, "__name__": True, "job": True, "module": True}
    excl.update({k: True for k in hide})
    return {
        "type": "table", "title": title, "description": desc, "datasource": DS,
        "_w": w, "_h": h, "targets": [_target(expr, instant=True)],
        "fieldConfig": {"defaults": {"mappings": mappings or [],
                                     "custom": {"cellOptions": {"type": "color-text"}}},
                        "overrides": []},
        "transformations": [{"id": "organize", "options": {
            "excludeByName": excl, "renameByName": rename or {}}}],
        "options": {"showHeader": True, "cellHeight": "sm",
                    "sortBy": [{"displayName": sort_by}] if sort_by else []},
    }


def row(title):
    return {"type": "row", "title": title, "collapsed": False, "panels": [], "_w": 24, "_h": 1}


def containers(project_regex: str) -> list[dict]:
    """CPU + memory per compose service, from cAdvisor."""
    sel = f'compose_project=~"{project_regex}", compose_service!=""'
    return [
        timeseries("CPU per container (cores)",
                   [(f"sum by (compose_project, compose_service) "
                     f"(rate(container_cpu_usage_seconds_total{{{sel}}}[2m]))",
                     "{{compose_project}}/{{compose_service}}")],
                   unit="none",
                   desc="From cAdvisor. 1.0 = one full CPU core busy."),
        timeseries("Memory per container (working set)",
                   [(f"sum by (compose_project, compose_service) "
                     f"(container_memory_working_set_bytes{{{sel}}})",
                     "{{compose_project}}/{{compose_service}}")],
                   unit="bytes",
                   desc="From cAdvisor. Working set = memory the kernel cannot reclaim."),
    ]


def jvm_heap(stack: str) -> dict:
    return timeseries("JVM heap used per daemon",
                      [(f'jvm_heap_used_bytes{{stack="{stack}"}}', "{{component}}")],
                      unit="bytes", desc="java.lang:type=Memory HeapMemoryUsage.used from /jmx.")


def up_stats(stack: str, components: list[tuple[str, str]], w: int = 4) -> list[dict]:
    return [stat(label, f'max(up{{stack="{stack}", component="{comp}"}})', w=w,
                 mappings=UP_MAPPING, color_mode="background",
                 thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}])
            for comp, label in components]


def layout(panels: list[dict]) -> list[dict]:
    """Flow panels left-to-right, wrapping at Grafana's 24-column grid."""
    x = y = row_h = 0
    for i, p in enumerate(panels, start=1):
        w, h = p.pop("_w"), p.pop("_h")
        if x + w > 24:
            x, y, row_h = 0, y + row_h, 0
        p["gridPos"] = {"x": x, "y": y, "w": w, "h": h}
        p["id"] = i
        x += w
        row_h = max(row_h, h)
    return panels


def dashboard(uid, title, description, panels, variables=(), links=True):
    return {
        "uid": uid, "title": title, "description": description,
        "tags": ["urbanflow"], "timezone": "browser", "editable": False,
        "schemaVersion": 41, "version": 1, "refresh": "10s",
        "time": {"from": "now-30m", "to": "now"},
        "templating": {"list": list(variables)},
        "links": [{"type": "dashboards", "tags": ["urbanflow"], "asDropdown": True,
                   "title": "UrbanFlow dashboards", "includeVars": False,
                   "keepTime": True}] if links else [],
        "panels": layout(panels),
        "annotations": {"list": []},
    }


# --------------------------------------------------------------------------
def overview() -> dict:
    project_var = {
        "name": "project", "label": "Compose project", "type": "query", "datasource": DS,
        "query": {"query": "label_values(container_memory_working_set_bytes, compose_project)",
                  "refId": "project"},
        "regex": "/urbanflow.*/", "refresh": 2, "multi": True, "includeAll": True,
        "allValue": "urbanflow.*",
        "current": {"text": "All", "value": "$__all"}, "sort": 1,
    }
    panels = [
        row("Stacks: is each part of the platform answering?"),
        *[stat(title, [(f'sum(up{{stack="{s}"}})', "up"), (f'count(up{{stack="{s}"}})', "expected")],
               w=6, desc=f"Scrape targets answering (up) out of those configured (expected). {hint}",
               text_mode="value_and_name")
          for s, title, hint in [
              ("hadoop", "Hadoop core (HDFS, YARN, Hive)", "make hadoop-up"),
              ("hbase", "HBase + ZooKeeper", "make hbase-up"),
              ("streaming", "Kafka + Spark Streaming", "make stream-up"),
              ("monitoring", "Monitoring", "Prometheus, Grafana, cAdvisor"),
          ]],
        table("Every scrape target", "up", h=14, mappings=UP_MAPPING, sort_by="stack",
              desc="One row per endpoint Prometheus scrapes. DOWN just means that "
                   "stack is not started, unless you expected it to be.",
              rename={"Value": "state", "instance": "endpoint"}),
        row("Containers (cAdvisor)"),
        stat("Containers running", 'count(count by (name) (container_memory_working_set_bytes'
                                   '{compose_project=~"$project", compose_service!=""}))', w=4),
        stat("Total CPU (cores)", 'sum(rate(container_cpu_usage_seconds_total'
                                  '{compose_project=~"$project", compose_service!=""}[2m]))',
             unit="none", w=4),
        stat("Total memory", 'sum(container_memory_working_set_bytes'
                             '{compose_project=~"$project", compose_service!=""})',
             unit="bytes", w=4),
        table("Firing alerts", 'ALERTS{alertstate="firing"}', w=12, h=4,
              desc="Rules live in monitoring/prometheus/alerts.yml.",
              hide=("alertstate", "Value")),
        *containers("$project"),
        timeseries("Network received per container",
                   [('sum by (compose_project, compose_service) (rate(container_network_receive_bytes_total'
                     '{compose_project=~"$project", compose_service!=""}[2m]))',
                     "{{compose_project}}/{{compose_service}}")], unit="Bps"),
        timeseries("Network sent per container",
                   [('sum by (compose_project, compose_service) (rate(container_network_transmit_bytes_total'
                     '{compose_project=~"$project", compose_service!=""}[2m]))',
                     "{{compose_project}}/{{compose_service}}")], unit="Bps"),
    ]
    return dashboard("urbanflow-overview", "UrbanFlow: Overview",
                     "Which stacks are up, and CPU / memory / network per container.",
                     panels, variables=[project_var])


def hadoop() -> dict:
    panels = [
        row("Daemons"),
        *up_stats("hadoop", [("namenode", "HDFS NameNode"), ("datanode", "HDFS DataNode"),
                             ("resourcemanager", "YARN ResourceManager"),
                             ("nodemanager", "YARN NodeManager"),
                             ("hiveserver2", "HiveServer2")], w=4),
        stat("Live DataNodes", "hdfs_live_datanodes", w=4,
             thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}]),
        row("HDFS"),
        stat("HDFS used", "hdfs_capacity_used_bytes / hdfs_capacity_total_bytes",
             unit="percentunit", w=4,
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 0.7},
                         {"color": "red", "value": 0.85}]),
        stat("Capacity", "hdfs_capacity_total_bytes", unit="bytes", w=4),
        stat("Files + directories", "hdfs_files_total", w=4),
        stat("Blocks", "hdfs_blocks_total", w=4),
        stat("Under-replicated blocks", "hdfs_under_replicated_blocks", w=4,
             desc="Expected > 0 on a one-DataNode cluster when replication > 1.",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}]),
        stat("Missing blocks", "hdfs_missing_blocks", w=4,
             thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]),
        timeseries("HDFS capacity", [("hdfs_capacity_used_bytes", "used"),
                                     ("hdfs_capacity_remaining_bytes", "remaining")],
                   unit="bytes"),
        timeseries("HDFS files and blocks", [("hdfs_files_total", "files + dirs"),
                                             ("hdfs_blocks_total", "blocks")]),
        row("YARN"),
        stat("Active NodeManagers", "yarn_active_nodemanagers", w=4,
             thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}]),
        stat("Apps running", "yarn_apps_running", w=4),
        stat("Apps pending", "yarn_apps_pending", w=4),
        stat("Apps completed", "yarn_apps_completed_total", w=4),
        stat("Apps failed", "yarn_apps_failed_total", w=4,
             thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]),
        stat("Containers running", "yarn_nodemanager_containers_running", w=4),
        timeseries("YARN memory", [("yarn_allocated_mb * 1024 * 1024", "allocated"),
                                   ("yarn_available_mb * 1024 * 1024", "available")],
                   unit="bytes", stack=True,
                   desc="Memory YARN has handed to containers vs. what is still free."),
        timeseries("YARN applications", [("yarn_apps_running", "running"),
                                         ("yarn_apps_pending", "pending"),
                                         ("yarn_allocated_containers", "containers")]),
        row("JVMs and containers"),
        jvm_heap("hadoop"),
        *containers(PROJECTS["hadoop"])[:1],
    ]
    return dashboard("urbanflow-hadoop", "UrbanFlow: Hadoop (HDFS, YARN, Hive)",
                     "NameNode, DataNode, ResourceManager, NodeManager and HiveServer2, "
                     "read from each daemon's /jmx through json-exporter.", panels)


def hbase() -> dict:
    panels = [
        row("Daemons"),
        *up_stats("hbase", [("hbase-master", "HBase Master"),
                            ("hbase-regionserver", "RegionServer"),
                            ("zookeeper", "ZooKeeper")], w=4),
        stat("Live RegionServers", "hbase_master_region_servers", w=4,
             thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}]),
        stat("Dead RegionServers", "hbase_master_dead_region_servers", w=4,
             thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}]),
        stat("Regions in transition", "hbase_master_regions_in_transition", w=4),
        row("Tables and requests"),
        stat("Regions", "max(hbase_regionserver_regions)", w=4),
        stat("Store files", "max(hbase_regionserver_store_files)", w=4),
        stat("Data on disk", "max(hbase_regionserver_store_file_size_bytes)", unit="bytes", w=4),
        stat("MemStore", "max(hbase_regionserver_memstore_size_bytes)", unit="bytes", w=4,
             desc="Writes buffered in memory before they are flushed to HFiles."),
        stat("Reads / s", "max(rate(hbase_regionserver_read_requests_total[1m]))",
             unit="reqps", w=4),
        stat("Writes / s", "max(rate(hbase_regionserver_write_requests_total[1m]))",
             unit="reqps", w=4),
        timeseries("Request rate", [
            ("max(rate(hbase_regionserver_read_requests_total[1m]))", "reads"),
            ("max(rate(hbase_regionserver_write_requests_total[1m]))", "writes")],
            unit="reqps"),
        timeseries("MemStore vs. store files", [
            ("max(hbase_regionserver_memstore_size_bytes)", "memstore"),
            ("max(hbase_regionserver_store_file_size_bytes)", "store files")], unit="bytes"),
        row("ZooKeeper"),
        stat("znodes", "zookeeper_znodes", w=6),
        stat("Client connections", "zookeeper_alive_connections", w=6),
        stat("Outstanding requests", "zookeeper_outstanding_requests", w=6),
        stat("Average latency", "zookeeper_avg_latency_ms", unit="ms", w=6),
        row("JVMs and containers"),
        jvm_heap("hbase"),
        *containers(PROJECTS["hbase"])[:1],
    ]
    return dashboard("urbanflow-hbase", "UrbanFlow: HBase + ZooKeeper",
                     "HBase Master / RegionServer from /jmx and ZooKeeper from its "
                     "AdminServer, through json-exporter.", panels)


def streaming() -> dict:
    panels = [
        row("Kafka"),
        *up_stats("streaming", [("kafka-exporter", "kafka-exporter")], w=4),
        stat("Brokers", "kafka_brokers", w=4,
             thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}]),
        stat("Messages in / s (all topics)",
             "sum(rate(kafka_topic_partition_current_offset[1m]))", unit="short", w=4),
        stat("Messages retained (all topics)",
             "sum(kafka_topic_partition_current_offset - kafka_topic_partition_oldest_offset)",
             w=4),
        stat("Consumer lag (all groups)", "sum(kafka_consumergroup_lag)", w=4,
             desc="Only groups that commit offsets to Kafka appear here.",
             thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1000},
                         {"color": "red", "value": 10000}]),
        stat("Under-replicated partitions",
             "sum(kafka_topic_partition_under_replicated_partition)", w=4),
        timeseries("Throughput per topic (messages / s)",
                   [("sum by (topic) (rate(kafka_topic_partition_current_offset"
                     '{topic!~"__.*"}[1m]))', "{{topic}}")],
                   desc="Rate of the newest offset: how fast producers are appending."),
        timeseries("Consumer lag per group and topic",
                   [("sum by (consumergroup, topic) (kafka_consumergroup_lag)",
                     "{{consumergroup}} on {{topic}}")],
                   desc="Messages produced but not yet consumed by that group."),
        timeseries("Messages retained per topic",
                   [("sum by (topic) (kafka_topic_partition_current_offset"
                     ' - kafka_topic_partition_oldest_offset{topic!~"__.*"})', "{{topic}}")]),
        timeseries("Consumer progress per group (messages / s)",
                   [("sum by (consumergroup, topic) (rate(kafka_consumergroup_current_offset[1m]))",
                     "{{consumergroup}} on {{topic}}")]),
        row("Containers"),
        *containers(PROJECTS["streaming"]),
    ]
    return dashboard("urbanflow-streaming", "UrbanFlow: Kafka + Spark Streaming",
                     "Kafka throughput and consumer lag from kafka-exporter, plus "
                     "per-container CPU / memory for the streaming stack.", panels)


DASHBOARDS = {"overview": overview, "hadoop": hadoop, "hbase": hbase, "streaming": streaming}


def render() -> dict[str, str]:
    return {f"{name}.json": json.dumps(build(), indent=2) + "\n"
            for name, build in DASHBOARDS.items()}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, text in render().items():
        (OUT / name).write_text(text)
        print(f"wrote {OUT / name}")


if __name__ == "__main__":
    main()
