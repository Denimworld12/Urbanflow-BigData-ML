"""Connection to HBase through its Thrift gateway, plus the column-family layout.

The tables themselves are created by scripts/hbase/create_tables.rb (HBase
shell DDL); this module only needs to know which family each column lives in.
"""
from __future__ import annotations
import os
import time

from . import keys

# One-letter family names on purpose: HBase stores the family and qualifier
# with every single cell, so long names are paid for on every value.
FAMILIES = {
    keys.DEMAND:   {"m": "metrics read by lookups (trips, averages)",
                    "z": "descriptive zone attributes, rarely read"},
    keys.DURATION: {"p": "model prediction"},
    keys.DAILY:    {"k": "daily KPIs"},
}


def connect(retries: int = 30, wait_s: float = 2.0):
    """happybase connection, retrying while the Thrift gateway comes up."""
    import happybase                          # imported lazily: tests don't need it
    host = os.environ.get("HBASE_THRIFT_HOST", "localhost")
    port = int(os.environ.get("HBASE_THRIFT_PORT", "19090"))
    last = None
    for _ in range(retries):
        try:
            conn = happybase.Connection(host=host, port=port, timeout=30000)
            conn.tables()                     # forces a round trip
            return conn
        except Exception as e:                # noqa: BLE001 — any failure means "not up yet"
            last = e
            time.sleep(wait_s)
    raise ConnectionError(f"HBase Thrift gateway at {host}:{port} not reachable: {last}")


def require_tables(conn) -> None:
    present = {t.decode() for t in conn.tables()}
    missing = [t for t in FAMILIES if t not in present]
    if missing:
        raise RuntimeError(f"tables {missing} missing — run `make hbase-load` "
                           "(it runs scripts/hbase/create_tables.rb first)")


def decode(row: dict[bytes, bytes]) -> dict[str, str]:
    """{b'm:trips': b'123'} -> {'m:trips': '123'}"""
    return {k.decode(): v.decode() for k, v in row.items()}
