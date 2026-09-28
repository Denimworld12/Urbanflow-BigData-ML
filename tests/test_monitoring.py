"""Checks for the monitoring stack's config files. No Docker, no network.

The live check is `make monitor-up && make monitor-status`; these catch the
cheap mistakes first: stale dashboard JSON, and dashboard queries asking for a
metric no exporter produces (a typo there shows as an empty panel, not an
error).
"""
import importlib.util
import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
DASHBOARDS = ROOT / "monitoring" / "grafana" / "dashboards"
JSON_EXPORTER = ROOT / "monitoring" / "json-exporter" / "config.yml"


def _builder():
    spec = importlib.util.spec_from_file_location(
        "build_dashboards", ROOT / "scripts" / "build_dashboards.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _json_exporter_metrics() -> set[str]:
    """Metric names json-exporter emits: `name` for value metrics,
    `name_<key>` for each key under `values:` of an object metric."""
    names = set()
    for module in yaml.safe_load(JSON_EXPORTER.read_text())["modules"].values():
        for metric in module["metrics"]:
            if metric.get("type") == "object":
                names.update(f"{metric['name']}_{key}" for key in metric["values"])
            else:
                names.add(metric["name"])
    return names


def _exprs():
    for path in sorted(DASHBOARDS.glob("*.json")):
        for panel in json.loads(path.read_text())["panels"]:
            for target in panel.get("targets", []):
                yield path.name, panel["title"], target["expr"]


def test_committed_dashboards_match_generator():
    rendered = _builder().render()
    committed = {p.name: p.read_text() for p in DASHBOARDS.glob("*.json")}
    assert committed == rendered, "run: make monitor-dashboards"


def test_dashboard_uids_unique_and_datasource_fixed():
    uids = []
    for path in DASHBOARDS.glob("*.json"):
        dash = json.loads(path.read_text())
        uids.append(dash["uid"])
        for panel in dash["panels"]:
            if panel["type"] != "row":
                assert panel["datasource"]["uid"] == "prometheus", (path.name, panel["title"])
    assert len(uids) == len(set(uids)) == 4


def test_every_queried_metric_has_a_source():
    known = _json_exporter_metrics()
    assert {"hdfs_live_datanodes", "yarn_apps_running", "jvm_heap_used_bytes",
            "hbase_master_region_servers", "zookeeper_znodes"} <= known
    # Everything else comes from exporters that name their own metrics.
    external = ("container_", "kafka_", "jvm_memory_used_bytes", "ALERTS")
    keywords = {"sum", "max", "min", "count", "rate", "by", "or", "vector", "label_values",
                "label_replace"}
    for dash, title, expr in _exprs():
        stripped = re.sub(r"\{[^}]*\}|\[[^]]*\]|\"[^\"]*\"", "", expr)
        for word in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", stripped):
            if word in keywords or word in {"compose_project", "compose_service", "topic",
                                            "consumergroup", "name", "component"}:
                continue
            assert word in known or word == "up" or word.startswith(external), (
                dash, title, word)
