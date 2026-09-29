"""Reading the streaming file sink's commit log (stream/sink.py). Run: make test"""
import json
from urbanflow.stream.sink import committed_files


def _part(out, name):
    p = out / name
    p.write_bytes(b"PAR1")
    return p


def _log(out, name, entries):
    log = out / "_spark_metadata"
    log.mkdir(exist_ok=True)
    lines = ["v1"] + [json.dumps({"path": p.as_uri(), "size": 4, "isDir": False,
                                  "modificationTime": 0, "blockReplication": 1,
                                  "blockSize": 4, "action": action}) for p, action in entries]
    (log / name).write_text("\n".join(lines) + "\n")


def test_no_log_returns_none(tmp_path):
    _part(tmp_path, "part-0.parquet")
    assert committed_files(tmp_path) is None
    assert committed_files(tmp_path / "missing") is None


def test_uncommitted_part_files_are_skipped(tmp_path):
    a = _part(tmp_path, "part-a.parquet")
    b = _part(tmp_path, "part-b.parquet")
    _part(tmp_path, "part-crashed.parquet")
    _log(tmp_path, "0", [(a, "add")])
    _log(tmp_path, "1", [(b, "add")])
    assert sorted(committed_files(tmp_path)) == [a, b]


def test_compact_file_supersedes_earlier_batches(tmp_path):
    a = _part(tmp_path, "part-a.parquet")
    b = _part(tmp_path, "part-b.parquet")
    c = _part(tmp_path, "part-c.parquet")
    stale = _part(tmp_path, "part-stale.parquet")
    _log(tmp_path, "0", [(stale, "add")])
    _log(tmp_path, "1.compact", [(a, "add"), (b, "add")])
    _log(tmp_path, "2", [(c, "add"), (b, "delete")])
    (tmp_path / "_spark_metadata" / ".3.tmp").write_text("v1\n")
    assert sorted(committed_files(tmp_path)) == [a, c]
