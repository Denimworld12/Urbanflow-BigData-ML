"""The dashboard's Live tab — read-only view over the streaming sink.

Reads data/stream/zone_metrics (written by `make stream-run`) through the same
DuckDB connection as every other tab, so there is still no Spark in the
dashboard process. It refreshes itself every 10 seconds and shows setup
instructions instead of an error when the streaming path has never run.
"""
from __future__ import annotations
import datetime
import duckdb
import plotly.express as px
import streamlit as st
from urbanflow import config
from urbanflow.stream.sink import committed_files

METRICS = config.STREAM / "zone_metrics"


@st.fragment(run_every="10s")
def render(con: duckdb.DuckDBPyConnection) -> None:
    files = committed_files(METRICS)
    if files is None:
        files = list(METRICS.glob("*.parquet")) if METRICS.exists() else []
    if not files:
        st.info("No streaming output yet. The real-time path is optional — to see it here, run "
                "in three terminals:\n\n"
                "```\nmake stream-up        # Kafka + kafka-exporter\n"
                "make stream-run       # Spark Structured Streaming job\n"
                "make stream-produce   # replay trips into Kafka\n```\n\n"
                "See docs/hadoop/streaming.md.")
        return

    src = "read_parquet([" + ", ".join("'" + str(f).replace("'", "''") + "'" for f in files) + "])"
    try:
        totals = con.execute(f"SELECT count(*) AS windows, sum(trips) AS trips, "
                             f"max(window_end) AS latest FROM {src}").df().iloc[0]
        per_window = con.execute(
            f"SELECT window_start, sum(trips) AS trips, round(avg(avg_fare), 2) AS avg_fare "
            f"FROM {src} GROUP BY window_start ORDER BY window_start DESC LIMIT 144").df()
        top = con.execute(
            f"SELECT pu_borough AS borough, pu_zone AS zone, trips, avg_fare, avg_duration_min "
            f"FROM {src} WHERE window_start = (SELECT max(window_start) FROM {src}) "
            f"ORDER BY trips DESC LIMIT 10").df()
    except duckdb.Error as e:          # a file mid-write can be briefly unreadable
        st.warning(f"Streaming output is being written — retrying shortly ({e.__class__.__name__}).")
        return

    written = datetime.datetime.fromtimestamp(max(f.stat().st_mtime for f in files))
    c1, c2, c3 = st.columns(3)
    c1.metric("Zone-windows written", f"{int(totals.windows):,}")
    c2.metric("Trips counted", f"{int(totals.trips):,}")
    c3.metric("Latest closed window (event time)", f"{totals.latest:%Y-%m-%d %H:%M}")
    st.caption(f"Last batch written at {written:%H:%M:%S} on this machine. A 5-minute window "
               "appears once the watermark (30 min of event time by default) has passed its end, so the "
               "numbers here are final — they never change after being written.")

    st.plotly_chart(px.line(per_window.sort_values("window_start"), x="window_start", y="trips",
                            title="Trips per 5-minute window, all zones (latest 12 hours of event time)",
                            labels={"window_start": "pickup window", "trips": "trips"}),
                    use_container_width=True)
    st.markdown("**Busiest pickup zones in the latest window**")
    st.dataframe(top, use_container_width=True, hide_index=True)
