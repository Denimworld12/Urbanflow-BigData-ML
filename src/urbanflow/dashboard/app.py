"""Stage S6 — the dashboard you demo.

Deliberately there is NO SPARK in this process. Streamlit reads the gold
Parquet tables through DuckDB, which opens them in milliseconds. That is why
the app is instant while the curated scan takes a minute and a half — and
explaining that contrast live is the strongest moment in the demo.

    streamlit run src/urbanflow/dashboard/app.py
"""
from __future__ import annotations
import json
from pathlib import Path
import duckdb, pandas as pd, streamlit as st
import plotly.express as px
import plotly.io as pio

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from urbanflow import config                                     # noqa: E402

# ---- one restrained palette, used everywhere ----
INK, MUTED, GRID = "#1B1B18", "#6E6E66", "#E2E2DA"
SEQ = ["#F0E2C4", "#DFC189", "#C79C4E", "#A87A22", "#7E5A0E"]     # sequential, one hue
CAT = ["#8A5600", "#0B5A55", "#34406B", "#8A2B2B", "#5E6E33"]     # categorical
pio.templates["uf"] = pio.templates["plotly_white"]
pio.templates["uf"].layout.update(font=dict(family="Helvetica, Arial", size=13, color=INK),
                                  colorway=CAT, margin=dict(l=8, r=8, t=36, b=8))
pio.templates.default = "uf"

st.set_page_config(page_title="UrbanFlow", page_icon="🚕", layout="wide")
con = duckdb.connect()


@st.cache_data(show_spinner=False)
def gold(name: str) -> pd.DataFrame:
    p = config.GOLD / name
    if not p.exists():
        return pd.DataFrame()
    return con.execute(f"SELECT * FROM read_parquet('{p}/*.parquet')").df()


@st.cache_data(show_spinner=False)
def js(name: str) -> dict | list:
    p = config.GOLD / name
    return json.loads(p.read_text()) if p.exists() else {}


st.title("UrbanFlow")
st.caption("Batch analytics over NYC trip records · Apache Spark → Parquet → DuckDB")

kpi, geo, beh, perf = st.tabs(["Overview", "Geography", "Behaviour", "Performance"])

# ------------------------------------------------------------------ overview
with kpi:
    d = gold("daily_kpis")
    if d.empty:
        st.warning("No gold tables yet. Run:  make gold")
    else:
        c = st.columns(4)
        c[0].metric("Trips", f"{int(d.trips.sum()):,}")
        c[1].metric("Revenue", f"${d.revenue.sum():,.0f}")
        c[2].metric("Avg fare", f"${d.avg_fare.mean():.2f}")
        c[3].metric("Avg duration", f"{d.avg_duration_min.mean():.1f} min")
        st.plotly_chart(px.line(d, x="d", y="trips", title="Trips per day",
                                labels={"d": "", "trips": "trips"}), use_container_width=True)
        st.plotly_chart(px.line(d, x="d", y="revenue", title="Revenue per day",
                                labels={"d": "", "revenue": "revenue ($)"}), use_container_width=True)

# ------------------------------------------------------------------ geography
with geo:
    z = gold("demand_by_zone_hour")
    if z.empty:
        st.warning("Run:  make gold")
    else:
        piv = (z.groupby(["pu_borough", "pickup_hour"], as_index=False)["trips"].sum()
                 .pivot(index="pu_borough", columns="pickup_hour", values="trips").fillna(0))
        st.plotly_chart(px.imshow(piv, aspect="auto", color_continuous_scale=SEQ,
                                  labels=dict(x="hour of day", y="", color="trips"),
                                  title="Demand by borough and hour"), use_container_width=True)
        od = gold("od_matrix")
        if not od.empty:
            st.subheader("Busiest origin–destination pairs")
            st.dataframe(od.head(25), use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ behaviour
with beh:
    t = gold("tipping")
    if not t.empty:
        t = t.copy(); t["payment"] = t.is_card.map({True: "Card", False: "Cash"})
        st.plotly_chart(px.bar(t, x="pu_borough", y="pct_trips_with_tip", color="payment",
                               barmode="group", title="Share of trips with a recorded tip",
                               labels={"pu_borough": "", "pct_trips_with_tip": "% of trips"}),
                        use_container_width=True)
        st.info("**Cash tips are never recorded.** Any 'average tip' over all trips is biased downward. "
                "Reporting card and cash separately is the honest treatment — and a finding worth a "
                "paragraph in the report.")
    s = gold("speed_by_hour")
    if not s.empty:
        st.plotly_chart(px.line(s.groupby("pickup_hour", as_index=False).avg_speed_mph.mean(),
                                x="pickup_hour", y="avg_speed_mph",
                                title="Average speed by hour — the city slowing down",
                                labels={"pickup_hour": "hour of day", "avg_speed_mph": "mph"}),
                        use_container_width=True)

# ------------------------------------------------------------------ performance
with perf:
    b = js("benchmarks.json")
    if not b:
        st.warning("Run:  make bench")
    else:
        for r in b:
            st.subheader(r["experiment"])
            if r["experiment"] == "cores":
                sp = pd.DataFrame({"cores": list(r["speedup_vs_1"]), "speedup": list(r["speedup_vs_1"].values())})
                sp["ideal"] = sp.cores.astype(int)
                f = px.line(sp, x="cores", y=["speedup", "ideal"], markers=True,
                            title="Speedup vs cores — measured against linear")
                st.plotly_chart(f, use_container_width=True)
            else:
                st.write({k: v for k, v in r.items() if k not in ("experiment", "note")})
            st.caption(r["note"])
    m = js("model_results.json")
    if m:
        st.subheader("Trip-duration model")
        c = st.columns(3)
        c[0].metric("Baseline RMSE", f"{m['baseline']['rmse_min']:.2f} min")
        c[1].metric("GBT RMSE", f"{m['gbt']['rmse_min']:.2f} min")
        c[2].metric("Improvement", f"{m['improvement_pct']:.1f}%")
        fi = pd.DataFrame(m["feature_importance"])
        st.plotly_chart(px.bar(fi, x="importance", y="feature", orientation="h",
                               title="Feature importance"), use_container_width=True)
