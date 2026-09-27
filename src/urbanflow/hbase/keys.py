"""Row-key design for the HBase serving tables. Pure functions, no HBase needed.

HBase keeps every table sorted by row key and can only look rows up by key
(a point `get`) or by a contiguous key range (a `scan`). So the key is the
query plan: we put the fields we filter on first, in the order we narrow by,
and zero-pad numbers so that byte order equals numeric order.

    urbanflow:demand         {zone}#{daytype}#{HH}
    urbanflow:duration_pred  {pu_borough}#{do_borough}#{daytype}#{HH}#{DD.D}
    urbanflow:daily_kpis     {YYYY-MM-DD}

See docs/hadoop/hbase.md for the reasoning behind each one.
"""
from __future__ import annotations
from datetime import date

SEP = "#"

# Namespaces are HBase's equivalent of a database/schema.
NAMESPACE = "urbanflow"
DEMAND = f"{NAMESPACE}:demand"
DURATION = f"{NAMESPACE}:duration_pred"
DAILY = f"{NAMESPACE}:daily_kpis"

# pickup_dow in the prediction grid uses Spark's dayofweek (1=Sun..7=Sat); the
# grid only scores one representative day of each kind (see predict_grid.py).
DOW_TO_DAYTYPE = {4: "weekday", 7: "weekend"}
DAYTYPES = ("weekday", "weekend")


def _part(value: str) -> str:
    value = str(value)
    if SEP in value:
        raise ValueError(f"{value!r} contains the key separator {SEP!r}")
    return value


def daytype(is_weekend: bool) -> str:
    return "weekend" if is_weekend else "weekday"


def hour(h: int) -> str:
    h = int(h)
    if not 0 <= h <= 23:
        raise ValueError(f"hour {h} out of range 0-23")
    return f"{h:02d}"


def distance(miles: float) -> str:
    miles = float(miles)
    if not 0 <= miles < 100:
        raise ValueError(f"distance {miles} out of range 0-99.9")
    return f"{miles:04.1f}"          # 5 -> "05.0", 30 -> "30.0": sorts numerically


def join(*parts: str) -> str:
    return SEP.join(_part(p) for p in parts)


def demand_key(zone: str, day: str, h: int) -> str:
    return join(zone, day, hour(h))


def demand_prefix(zone: str, day: str | None = None) -> str:
    """All rows for a zone (48), or one day type of it (24 hourly rows)."""
    return join(zone, day) + SEP if day else join(zone) + SEP


def duration_key(pu_borough: str, do_borough: str, day: str, h: int, miles: float) -> str:
    return join(pu_borough, do_borough, day, hour(h), distance(miles))


def duration_prefix(pu_borough: str, do_borough: str, day: str | None = None,
                    h: int | None = None) -> str:
    parts = [pu_borough, do_borough]
    if day is not None:
        parts.append(day)
        if h is not None:
            parts.append(hour(h))
    return join(*parts) + SEP


def daily_key(d: date | str) -> str:
    return d.isoformat() if isinstance(d, date) else _part(d)
