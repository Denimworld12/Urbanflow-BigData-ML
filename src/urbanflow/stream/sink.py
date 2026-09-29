"""Which Parquet files the streaming file sink has committed.

Spark's file sink writes part files straight into the output directory and
records each batch's files in <output>/_spark_metadata/: one file per batch
(named by batch id, `N.compact` every few batches holding everything so far),
each a "v1" header line followed by one JSON entry per file. Only files listed
there with action "add" belong to committed batches; a batch that crashed
mid-write can leave extra part files behind. Stdlib only, so the dashboard can
use it without Spark.
"""
from __future__ import annotations
import json
from pathlib import Path
from urllib.parse import unquote, urlparse


def committed_files(output: Path) -> list[Path] | None:
    """Committed part files of a file-sink output, or None if it has no log."""
    log = output / "_spark_metadata"
    batches = {}
    for f in log.glob("*") if log.is_dir() else []:
        stem = f.name.removesuffix(".compact")
        if stem.isdigit():
            batches[int(stem)] = f
    if not batches:
        return None
    compacts = [b for b, f in batches.items() if f.suffix == ".compact"]
    start = max(compacts, default=min(batches))
    live: dict[str, bool] = {}
    for b in sorted(b for b in batches if b >= start):
        for line in batches[b].read_text().splitlines()[1:]:
            if line.strip():
                entry = json.loads(line)
                live[entry["path"]] = entry.get("action") == "add"
    paths = (Path(unquote(urlparse(p).path)) for p, added in live.items() if added)
    return [p for p in paths if p.exists()]
