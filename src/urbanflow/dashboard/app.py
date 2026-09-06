"""Stage S6 — the dashboard you demo.

Deliberately there is NO SPARK in this process. Streamlit reads the gold
Parquet tables through DuckDB, which opens them in milliseconds. That is why
the app is instant while the curated scan takes a minute and a half — and
explaining that contrast live is the strongest moment in the demo.

    streamlit run src/urbanflow/dashboard/app.py
"""
from __future__ import annotations
import hashlib, json, os
from pathlib import Path
import duckdb, pandas as pd, streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from urbanflow import config                                     # noqa: E402


def _session_token() -> str:
    """Deterministic, not random — so it survives a page refresh AND a
    container restart with zero server-side session storage. It's a hash of
    the shared password, not the password itself, sitting in the URL as a
    'remember me' token. Fine for a single-shared-password local/demo tool;
    would need real per-user sessions before this touched the open internet."""
    pwd = os.environ.get("ADMIN_PASSWORD", "urbanflow")
    return hashlib.sha256(pwd.encode()).hexdigest()[:16]


def _require_login() -> None:
    """Single shared password, not per-user accounts — this is a viva demo
    behind one machine's port, not a multi-tenant app. Set ADMIN_USER /
    ADMIN_PASSWORD env vars to change the default; don't ship the default
    password anywhere it's actually exposed to the internet."""
    if st.session_state.get("authenticated"):
        return
    if st.query_params.get("t") == _session_token():
        st.session_state.authenticated = True
        return
    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        st.title("🚕 UrbanFlow")
        with st.container(border=True):
            st.caption("Sign in to view the dashboard")
            with st.form("login"):
                user = st.text_input("Username")
                pwd = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Sign in", use_container_width=True)
            if submitted:
                if (user == os.environ.get("ADMIN_USER", "admin")
                        and pwd == os.environ.get("ADMIN_PASSWORD", "urbanflow")):
                    st.session_state.authenticated = True
                    st.query_params["t"] = _session_token()
                    st.rerun()
                else:
                    st.error("Wrong username or password")
    st.stop()

# ---- one restrained palette, used everywhere ----
INK, MUTED, GRID = "#1B1B18", "#6E6E66", "#E2E2DA"
SEQ = ["#F0E2C4", "#DFC189", "#C79C4E", "#A87A22", "#7E5A0E"]     # sequential, one hue
CAT = ["#8A5600", "#0B5A55", "#34406B", "#8A2B2B", "#5E6E33"]     # categorical

# Approximate centroids — the pipeline only carries borough-level geography
# (the 265-zone shapefile is a separate TLC download this project never
# pulls), and every chart in this dashboard already operates at that same
# borough granularity, so this is enough for a real map, not a placeholder.
BOROUGH_COORDS = {
    "Manhattan":      (40.7831, -73.9712),
    "Brooklyn":       (40.6782, -73.9442),
    "Queens":         (40.7282, -73.7949),
    "Bronx":          (40.8448, -73.8648),
    "Staten Island":  (40.5795, -74.1502),
    "EWR":            (40.6895, -74.1745),
}
pio.templates["uf"] = pio.templates["plotly_white"]
pio.templates["uf"].layout.update(font=dict(family="IBM Plex Sans, sans-serif", size=12, color=INK),
                                  colorway=CAT, margin=dict(l=8, r=8, t=36, b=8),
                                  height=320)  # one fixed height everywhere — charts stop
                                               # jumping between tiny and huge tab to tab
pio.templates.default = "uf"

st.set_page_config(page_title="UrbanFlow", page_icon="🚕", layout="wide")

# Same type system as the project's written docs (Fraunces + IBM Plex Sans/Mono)
# — the dashboard is otherwise the one deliverable left on Streamlit's stock
# system font, which is what makes it read as a different, unfinished product
# next to everything else. .streamlit/config.toml only reaches color, not type.
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:wght@500;600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@500&display=swap');

html, body, [class*="css"], [data-testid="stAppViewContainer"] {
    font-family: 'IBM Plex Sans', sans-serif;
}
h1, h2, h3 {
    font-family: 'Fraunces', Georgia, serif !important;
    font-weight: 600 !important;
}
[data-testid="stMetricValue"], [data-testid="stMetricDelta"] {
    font-family: 'IBM Plex Mono', monospace;
}
[data-testid="stSidebar"] {
    border-right: 1px solid #DDD6C6;
}
[data-testid="stSidebar"] h1 {
    font-size: 1.1rem;
}
/* consistent, slightly smaller everywhere — was drifting between tabs */
h1 { font-size: 1.7rem !important; }
h2 { font-size: 1.25rem !important; }
h3 { font-size: 1.05rem !important; }
p, li, [data-testid="stMarkdownContainer"] { font-size: 0.92rem; }
[data-testid="stMetricValue"] { font-size: 1.35rem !important; }
[data-testid="stMetricLabel"] p { font-size: 0.78rem !important; }
[data-testid="stCaptionContainer"] { font-size: 0.82rem !important; }
</style>
""", unsafe_allow_html=True)

_require_login()
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


def _last_built() -> str:
    p = config.GOLD / "daily_kpis"
    if not p.exists():
        return "—"
    files = list(p.glob("*.parquet"))
    if not files:
        return "—"
    import datetime
    ts = max(f.stat().st_mtime for f in files)
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def _area(fig):
    """Fill under the line and emphasise the last point — a chart read at a
    glance should say 'here's where things stand right now', not just trace
    a shape."""
    fig.update_traces(fill="tozeroy", line=dict(width=2))
    return fig


def _dataset_label() -> str:
    if not config.CURATED.exists():
        return "—"
    names = sorted(p.name for p in config.CURATED.iterdir() if p.is_dir())
    return ", ".join(names) if names else "—"


@st.cache_data(show_spinner=False, ttl=3600)
def road_route(pu_lat: float, pu_lon: float, do_lat: float, do_lon: float):
    """A real driving route between two points, from OSRM's free public
    demo router — actual roads and turns, not a straight line pretending to
    be one. Returns (list of (lat, lon), True) for a real route, or
    ([(pu),(do)], False) as an honest straight-line fallback if the service
    is unreachable — never silently fakes a curve to look plausible."""
    import urllib.request
    url = (f"https://router.project-osrm.org/route/v1/driving/"
          f"{pu_lon},{pu_lat};{do_lon},{do_lat}?overview=full&geometries=geojson")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "urbanflow-dashboard"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        coords = data["routes"][0]["geometry"]["coordinates"]  # [[lon, lat], ...]
        return [(lat, lon) for lon, lat in coords], True
    except Exception:
        return [(pu_lat, pu_lon), (do_lat, do_lon)], False


def groq_chat(messages: list[dict]) -> str | None:
    """One HTTPS call to Groq's OpenAI-compatible chat endpoint via stdlib
    urllib — no SDK dependency for what is otherwise a single POST request.
    `messages` is the full conversation so far (system + history + new
    question) — that's what gives it "memory": each call resends everything
    said before, since Groq itself is stateless between calls.
    Returns None (caller shows setup instructions) if no key is configured."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return None
    import urllib.request
    model = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b")
    body = json.dumps({
        "model": model,
        "messages": messages,
        "temperature": 0.3,
        # gpt-oss models spend some of this budget on an internal reasoning
        # pass before the visible answer — too low and content comes back
        # empty even though the request "succeeds".
        "max_tokens": 600,
    }).encode()
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions", data=body, method="POST",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
                 # Cloudflare (fronting Groq's API) blocks urllib's default
                 # "Python-urllib/x.y" User-Agent as bot traffic (error 1010).
                 "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        data = json.loads(resp.read())
    return data["choices"][0]["message"]["content"].strip()


def _findings_facts() -> str:
    """Compile the real numbers already on screen elsewhere in this dashboard
    into a compact fact list — the only thing the AI is allowed to talk
    about. Shown to the user verbatim in an expander too, so "what is it
    even summarizing" has a direct, checkable answer."""
    d, t, s = gold("daily_kpis"), gold("tipping"), gold("speed_by_hour")
    m, b = js("model_results.json"), js("benchmarks.json")
    facts = []
    if not d.empty:
        facts.append(f"Total trips: {int(d.trips.sum()):,}. Total revenue: ${d.revenue.sum():,.0f}. "
                     f"Average fare: ${d.avg_fare.mean():.2f}. Average trip duration: "
                     f"{d.avg_duration_min.mean():.1f} minutes.")
    if not t.empty and "is_card" in t.columns:
        card = t[t.is_card].pct_trips_with_tip.mean()
        cash = t[~t.is_card].pct_trips_with_tip.mean()
        facts.append(f"Share of trips with a recorded tip: {card:.0f}% for card payments vs "
                     f"{cash:.0f}% for cash (cash tips are never recorded in the source data, "
                     f"which biases any all-trips tip average downward).")
    if not s.empty:
        by_hr = s.groupby("pickup_hour").avg_speed_mph.mean()
        facts.append(f"Average speed ranges from {by_hr.min():.1f} mph at hour {int(by_hr.idxmin())} "
                     f"(slowest) to {by_hr.max():.1f} mph at hour {int(by_hr.idxmax())} (fastest).")
    if m:
        facts.append(f"A trained model predicts trip duration {m['improvement_pct']:.0f}% more "
                     f"accurately than a naive baseline (baseline error {m['baseline']['rmse_min']:.1f} "
                     f"min vs model error {m['gbt']['rmse_min']:.1f} min). The strongest predictor is "
                     f"{m['feature_importance'][0]['feature']}.")
    if b:
        for r in b:
            if r.get("experiment") == "format" and "speedup" in r:
                facts.append(f"Reading the columnar Parquet format instead of CSV was {r['speedup']}x faster.")
            if r.get("experiment") == "join" and "speedup" in r:
                facts.append(f"Broadcasting the small lookup table instead of a full join was {r['speedup']}x faster.")
            if r.get("experiment") == "cores" and "speedup_vs_1" in r:
                last = list(r["speedup_vs_1"].values())[-1]
                facts.append(f"Speedup from 1 core to the max tested was {last}x.")
    return "\n".join(facts)


with st.sidebar:
    st.caption("THIS RUN")
    d0 = gold("daily_kpis")
    st.caption("Dataset")
    st.write(_dataset_label())
    st.caption("Trips in gold layer")
    st.write(f"{int(d0.trips.sum()):,}" if not d0.empty else "—")
    st.caption("Gold last built")
    st.write(_last_built())
    st.divider()
    def _logout():
        st.session_state.authenticated = False
        st.query_params.clear()   # otherwise the URL's token logs you right back in
    st.button("Log out", on_click=_logout, use_container_width=True)

st.title("UrbanFlow")
st.caption("Batch analytics over NYC trip records · Apache Spark → Parquet → DuckDB")

with st.container(border=True):
    st.subheader("Ask about the data (AI, Groq)")
    with st.expander("What is it actually allowed to talk about? (the exact facts, nothing else)"):
        st.code(_findings_facts() or "No gold tables yet.", language=None)
    st.caption("It remembers this conversation — ask a follow-up and it keeps context. It can only "
              "use the facts above; it's told never to invent a number.")

    if "chat" not in st.session_state:
        st.session_state.chat = []

    for msg in st.session_state.chat:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    def _ask(user_text: str) -> None:
        st.session_state.chat.append({"role": "user", "content": user_text})
        system = {"role": "system", "content": (
            "You explain data-analysis findings in plain English for a non-technical reader. "
            "No bullet lists, no jargon. Only use the facts below — never invent a number. If "
            "asked something these facts don't cover, say so plainly rather than guessing.\n\n"
            "Facts:\n" + _findings_facts())}
        try:
            reply = groq_chat([system] + st.session_state.chat)
        except Exception as e:
            reply = f"Couldn't reach Groq: {e}"
        else:
            if reply is None:
                reply = "No `GROQ_API_KEY` is set, so this can't reach the model — set it and try again."
        st.session_state.chat.append({"role": "assistant", "content": reply})

    if not st.session_state.chat:
        if st.button("Summarize the key findings"):
            _ask("Summarize the most interesting findings in 3-5 plain-English sentences.")
            st.rerun()
    question = st.chat_input("Ask your own question about these findings")
    if question:
        _ask(question)
        st.rerun()

predict, explore, kpi, geo, beh, perf = st.tabs(
    ["Predict", "Explore", "Overview", "Geography", "Behaviour", "Performance"])

# ------------------------------------------------------------------ explore
with explore:
    z0 = gold("demand_by_zone_hour")
    if z0.empty:
        st.warning("No gold tables yet. Run:  make gold")
    else:
        st.caption("Pick what you want to see, then press Search. This filters trips by where "
                   "they **start** — it does not look at where they end up. For a specific "
                   "start → end trip, use the **Predict** tab instead.")
        f1, f2, f3 = st.columns([1.2, 1, 2])
        borough_choice = f1.selectbox("Pickup area", ["All"] + sorted(z0.pu_borough.unique()))
        day_choice = f2.selectbox("Day type", ["All", "Weekday", "Weekend"])
        hour_range = f3.slider("Hour of day", 0, 23, (0, 23))

        if st.button("Search", type="primary"):
            st.session_state.explore_searched = True

        if st.session_state.get("explore_searched"):
            filt = z0.copy()
            if borough_choice != "All":
                filt = filt[filt.pu_borough == borough_choice]
            if day_choice != "All":
                filt = filt[filt.is_weekend == (day_choice == "Weekend")]
            filt = filt[(filt.pickup_hour >= hour_range[0]) & (filt.pickup_hour <= hour_range[1])]

            if filt.empty:
                st.warning("No trips match that filter — try widening it.")
            else:
                total_trips = int(filt.trips.sum())
                avg_dist = (filt.avg_distance * filt.trips).sum() / total_trips
                avg_dur = (filt.avg_duration_min * filt.trips).sum() / total_trips
                cc = st.columns(3)
                for col, (label, value) in zip(cc, [
                    ("Trips picked up here", f"{total_trips:,}"),
                    ("Avg distance traveled (any destination)", f"{avg_dist:.1f} mi"),
                    ("Avg time on the road (any destination)", f"{avg_dur:.1f} min"),
                ]):
                    with col.container(border=True):
                        st.metric(label, value)
                st.caption("These trips can end anywhere — distance and duration are averaged "
                          "across every destination, not one specific route.")

                by_hour = filt.groupby("pickup_hour", as_index=False).trips.sum().sort_values("pickup_hour")
                st.plotly_chart(px.bar(by_hour, x="pickup_hour", y="trips",
                                       title="Trips by hour, for this filter",
                                       labels={"pickup_hour": "hour of day", "trips": "trips"}),
                                use_container_width=True)
        else:
            st.info("Choose an area, day type, and hour range above, then press **Search**.")

# ------------------------------------------------------------------ overview
with kpi:
    d = gold("daily_kpis")
    if d.empty:
        st.warning("No gold tables yet. Run:  make gold")
    else:
        st.caption("Totals across every trip in the curated data — every area, every hour, "
                  "the whole time range. Not filtered to anything.")
        c = st.columns(4)
        stats = [("Trips", f"{int(d.trips.sum()):,}"),
                  ("Revenue", f"${d.revenue.sum():,.0f}"),
                  ("Avg fare", f"${d.avg_fare.mean():.2f}"),
                  ("Avg duration", f"{d.avg_duration_min.mean():.1f} min")]
        for col, (label, value) in zip(c, stats):
            with col.container(border=True):
                st.metric(label, value)
        st.plotly_chart(_area(px.line(d, x="d", y="trips", title="Trips per day",
                                      labels={"d": "", "trips": "trips"})), use_container_width=True)
        st.caption("How many trips happened on each day — flat means steady demand, spikes usually "
                  "mean a specific event or day-of-week pattern.")
        st.plotly_chart(_area(px.line(d, x="d", y="revenue", title="Revenue per day",
                                      labels={"d": "", "revenue": "revenue ($)"})), use_container_width=True)
        st.caption("Total fares collected each day — tracks trips per day closely unless fares "
                  "themselves are changing (e.g. surge pricing, longer trips).")

# ------------------------------------------------------------------ geography
with geo:
    z = gold("demand_by_zone_hour")
    od = gold("od_matrix")
    if z.empty:
        st.warning("Run:  make gold")
    else:
        st.subheader("One map, pick what it shows you")
        st.caption("Same six areas every time — only the color changes. Real NYC geography, "
                  "drag to pan, scroll to zoom.")

        by_area = z.groupby("pu_borough", as_index=False).agg(trips=("trips", "sum"),
                                                                avg_distance=("avg_distance", "mean"))
        weekend = (z.groupby(["pu_borough", "is_weekend"])["trips"].sum().unstack(fill_value=0))
        by_area["weekend_pct"] = by_area.pu_borough.map(
            lambda b: round(100 * weekend.loc[b, True] / weekend.loc[b].sum(), 1) if b in weekend.index else None)
        if not od.empty:
            od_w = od.assign(w=od.trips)
            fare_by_area = (od_w.groupby("pu_borough")
                            .apply(lambda g: (g.avg_fare * g.w).sum() / g.w.sum(), include_groups=False)
                            .rename("avg_fare"))
            by_area = by_area.merge(fare_by_area, on="pu_borough", how="left")
        by_area["lat"] = by_area.pu_borough.map(lambda b: BOROUGH_COORDS.get(b, (None, None))[0])
        by_area["lon"] = by_area.pu_borough.map(lambda b: BOROUGH_COORDS.get(b, (None, None))[1])
        by_area = by_area.dropna(subset=["lat", "lon"])

        metrics = {"Trips picked up": "trips", "Avg fare ($)": "avg_fare",
                   "Avg distance (mi)": "avg_distance", "Weekend share (%)": "weekend_pct"}
        metrics = {k: v for k, v in metrics.items() if v in by_area.columns}
        metric_label = st.selectbox("Color the map by", list(metrics))
        metric_col = metrics[metric_label]

        # scatter_map — real street/place-name tiles (Carto's free basemap,
        # no API key), the actual Google-Maps-like look. Trade-off, stated
        # plainly: this needs your browser to fetch image tiles from Carto's
        # server; if a network/proxy/ad-blocker blocks that, it renders
        # blank instead of failing loudly. If that happens again, the fix
        # is the vector map (no network needed, but a plainer look) this
        # replaced — tell me and I'll switch back.
        fig_map = px.scatter_map(by_area, lat="lat", lon="lon", size="trips", color=metric_col,
                                 hover_name="pu_borough",
                                 hover_data={"trips": True, "lat": False, "lon": False},
                                 color_continuous_scale=SEQ, size_max=45, zoom=8.3,
                                 center={"lat": by_area.lat.mean(), "lon": by_area.lon.mean()},
                                 title=f"{metric_label}, by area")
        fig_map.update_layout(map_style="carto-positron", height=420)
        st.plotly_chart(fig_map, use_container_width=True)

        st.caption("Darker cells = more trips picked up in that area during that hour. "
                  "Read across a row to see one area's rush-hour pattern; read down a "
                  "column to compare areas at the same hour.")
        piv = (z.groupby(["pu_borough", "pickup_hour"], as_index=False)["trips"].sum()
                 .pivot(index="pu_borough", columns="pickup_hour", values="trips").fillna(0))
        st.plotly_chart(px.imshow(piv, aspect="auto", color_continuous_scale=SEQ,
                                  labels=dict(x="hour of day", y="", color="trips"),
                                  title="Demand by area and hour"), use_container_width=True)

        eah = gold("earnings_by_area_hour")
        if not eah.empty:
            st.subheader("For drivers: which area pays best, right now?")
            st.caption("Same grid, the driver's question: not \"where are the most rides\" but "
                      "\"where does an hour of driving earn the most.\" Combinations with under "
                      "1,000 recorded trips are dropped — a single lucky fare isn't a pattern.")
            # the gold table has separate weekday/weekend rows per area+hour —
            # combine them (trip-weighted, not a naive average of the two
            # rates) before pivoting, or pivot() hits duplicate index entries
            combined = (eah.groupby(["pu_borough", "pickup_hour"])
                        .apply(lambda g: pd.Series({
                            "trips": g.trips.sum(),
                            "avg_fare": (g.avg_fare * g.trips).sum() / g.trips.sum(),
                            "avg_duration_min": (g.avg_duration_min * g.trips).sum() / g.trips.sum(),
                        }), include_groups=False)
                        .reset_index())
            eah_trust = combined[combined.trips >= 1000].copy()
            eah_trust["earn_per_hour"] = (eah_trust.avg_fare / (eah_trust.avg_duration_min / 60)).round(2)
            piv_earn = (eah_trust.pivot(index="pu_borough", columns="pickup_hour", values="earn_per_hour"))
            st.plotly_chart(px.imshow(piv_earn, aspect="auto", color_continuous_scale=SEQ,
                                      labels=dict(x="hour of day", y="", color="$/hour"),
                                      title="Average $/hour by area and hour"), use_container_width=True)
        if not od.empty:
            st.subheader("Busiest origin–destination pairs")
            st.caption("The specific pickup→dropoff zone pairs with the most trips, and what a "
                      "trip on that exact route costs and takes on average — the customer's "
                      "question: where do most rides actually go, and what should I expect to pay.")
            st.dataframe(od.head(25), use_container_width=True, hide_index=True)

            st.subheader("For drivers: the highest-earning routes")
            st.caption("Same data, the driver's question instead: not \"what does a ride cost\" but "
                      "\"where do I actually make money.\" Ranked by $/hour, not total fare — a "
                      "route that pays more per ride isn't better if it also takes much longer.")
            od_drv = od[od.avg_duration_min > 0].copy()
            od_drv["earn_per_hour"] = (od_drv.avg_fare / (od_drv.avg_duration_min / 60)).round(2)
            od_drv["route"] = od_drv.pu_zone + " → " + od_drv.do_zone
            top_drv = od_drv.sort_values("earn_per_hour", ascending=False).head(10)
            st.plotly_chart(px.bar(top_drv, x="earn_per_hour", y="route", orientation="h",
                                   title="Top 10 routes by earnings per hour on the road",
                                   labels={"earn_per_hour": "$ per hour", "route": ""}),
                            use_container_width=True)
            st.dataframe(top_drv[["route", "avg_fare", "avg_duration_min", "earn_per_hour", "trips"]]
                        .rename(columns={"avg_fare": "avg fare ($)", "avg_duration_min": "avg minutes",
                                         "earn_per_hour": "$/hour", "trips": "how often this happens"}),
                        use_container_width=True, hide_index=True)

            st.subheader("For drivers: which area should I start my shift in?")
            st.caption("Not one lucky route, but an overall area — every pickup from here, "
                      "averaged, weighted by how often each route actually happens.")
            area_earn = (od_drv.assign(w=od_drv.trips)
                        .groupby("pu_borough")
                        .apply(lambda g: (g.avg_fare * g.w).sum() / (g.avg_duration_min / 60 * g.w).sum(),
                               include_groups=False)
                        .reset_index(name="earn_per_hour").sort_values("earn_per_hour", ascending=False))
            st.plotly_chart(px.bar(area_earn, x="pu_borough", y="earn_per_hour",
                                   title="Average $/hour by pickup area (all routes from there, combined)",
                                   labels={"pu_borough": "", "earn_per_hour": "$ per hour"}),
                            use_container_width=True)

            st.subheader("For customers: the cheapest routes")
            st.caption("The other side of the same table — routes that happen often enough to "
                      "trust (50+ recorded trips) and cost the least.")
            cheap = od[od.trips >= 50].sort_values("avg_fare").head(10).copy()
            cheap["route"] = cheap.pu_zone + " → " + cheap.do_zone
            st.dataframe(cheap[["route", "avg_fare", "avg_duration_min", "trips"]]
                        .rename(columns={"avg_fare": "avg fare ($)", "avg_duration_min": "avg minutes",
                                         "trips": "how often this happens"}),
                        use_container_width=True, hide_index=True)

            st.subheader("Weekday vs weekend: where's the demand?")
            st.caption("For drivers deciding where to position, and customers curious if their "
                      "area gets quiet on weekends. Same area can swing either way — a business "
                      "district empties out on Saturday; a nightlife area does the opposite.")
            wk = z.groupby(["pu_borough", "is_weekend"], as_index=False).trips.sum()
            wk["Day type"] = wk.is_weekend.map({True: "Weekend", False: "Weekday"})
            st.plotly_chart(px.bar(wk, x="pu_borough", y="trips", color="Day type", barmode="group",
                                   title="Trips by area — weekday vs weekend",
                                   labels={"pu_borough": "", "trips": "trips"}),
                            use_container_width=True)

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
        st.plotly_chart(_area(px.line(s.groupby("pickup_hour", as_index=False).avg_speed_mph.mean(),
                                      x="pickup_hour", y="avg_speed_mph",
                                      title="Average speed by hour — the city slowing down",
                                      labels={"pickup_hour": "hour of day", "avg_speed_mph": "mph"})),
                        use_container_width=True)
        st.caption("Average speed of every trip that started in that hour, across all areas. "
                  "Dips are rush hour — the city's traffic physically slowing every vehicle down, "
                  "not a data artifact.")

    af = gold("airport_flows")
    d_all = gold("daily_kpis")
    if not af.empty:
        st.subheader("Airport trips are a different kind of trip entirely")
        st.caption("Same city, same dataset — but a trip to/from an airport doesn't behave like "
                  "a normal city trip. Compare the numbers below to the Overview tab's city-wide "
                  "averages.")
        c1, c2 = st.columns(2)
        with c1.container(border=True):
            st.metric("Avg airport-trip fare", f"${af.avg_fare.mean():.2f}",
                     delta=(f"${af.avg_fare.mean() - d_all.avg_fare.mean():+.2f} vs city-wide"
                            if not d_all.empty else None))
        with c2.container(border=True):
            st.metric("Avg airport-trip distance", f"{af.avg_distance.mean():.1f} mi")
        st.plotly_chart(px.bar(af.groupby("pickup_hour", as_index=False).trips.sum(),
                               x="pickup_hour", y="trips", title="Airport trips by hour",
                               labels={"pickup_hour": "hour of day", "trips": "trips"}),
                        use_container_width=True)
        st.caption("Worth separating in any model or report — averaging airport trips in with "
                  "everything else quietly distorts the city-wide fare and distance numbers.")

# ------------------------------------------------------------------ performance
with perf:
    st.info("**Why this tab exists:** anyone can compute averages from a CSV. This tab is the "
           "evidence that the *distributed processing* itself was understood, not just Spark's "
           "syntax — it's usually worth more of the grade than another chart of the same trips.")
    st.caption("This tab is about the computer, not the trips — how fast different ways of "
              "storing and querying the same data actually are. Bigger × means bigger win.")
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
            elif r.get("skipped"):
                st.info(f"Skipped — only {r.get('partitions_present', '?')} partition present, nothing to compare.")
            else:
                # generic before/after/speedup card for format & join experiments
                timing_keys = [k for k in r if k.endswith("_s")]
                if len(timing_keys) == 2 and "speedup" in r:
                    cc = st.columns(3)
                    for col, k in zip(cc, timing_keys):
                        with col.container(border=True):
                            st.metric(k[:-2].replace("_", " ").title(), f"{r[k]:.3f}s")
                    with cc[2].container(border=True):
                        st.metric("Speedup", f"{r['speedup']}×")
                else:
                    st.write({k: v for k, v in r.items() if k not in ("experiment", "note")})
            st.caption(r["note"])
    m = js("model_results.json")
    if m:
        st.subheader("Trip-duration model")
        st.caption("How well a trained model predicts how long a trip will take, compared to a "
                  "simple guess (distance ÷ average speed). RMSE is the model's typical error in "
                  "minutes — lower is better; a bigger improvement % means the model actually "
                  "learned something a naive guess couldn't.")
        c = st.columns(3)
        model_stats = [("Baseline RMSE (simple guess)", f"{m['baseline']['rmse_min']:.2f} min"),
                        ("GBT RMSE (trained model)", f"{m['gbt']['rmse_min']:.2f} min"),
                        ("Improvement", f"{m['improvement_pct']:.1f}%")]
        for col, (label, value) in zip(c, model_stats):
            with col.container(border=True):
                st.metric(label, value)
        fi = pd.DataFrame(m["feature_importance"])
        st.plotly_chart(px.bar(fi, x="importance", y="feature", orientation="h",
                               title="Feature importance"), use_container_width=True)
        st.caption("Which inputs the model actually relied on to make its predictions — longer "
                  "bars mean that input mattered more. Distance dominating makes intuitive sense; "
                  "it's here to show the model isn't relying on something it shouldn't.")

# ------------------------------------------------------------------ predict
with predict:
    dp = gold("duration_predictions")
    route_dist = gold("route_avg_distance")
    if dp.empty:
        st.warning("Run:  make predict-grid  (after make model)")
    else:
        st.caption("**What you get: how many minutes that trip is predicted to take** — nothing "
                   "else. Pick the pieces of a trip below (not a real fare or route, just the "
                   "shape of one), then press Predict. The number comes from the trained GBT "
                   "model, scored ahead of time over this exact grid — no Spark runs in this "
                   "dashboard process, this is a lookup, not a live model call.")
        st.caption("Not sure what to pick? Try one:")
        preset_cols = st.columns(3)
        for col, (label, ppu, pdo) in zip(preset_cols, [
            ("✈️ Airport run", "Manhattan", "EWR"),
            ("🏙️ Everyday commute", "Brooklyn", "Manhattan"),
            ("🚕 Local hop", "Queens", "Queens"),
        ]):
            if col.button(label, use_container_width=True):
                st.session_state["predict_pu"] = ppu
                st.session_state["predict_do"] = pdo
                st.rerun()

        c1, c2 = st.columns(2)
        pu = c1.selectbox("Pickup area", sorted(dp.pu_borough.unique()), key="predict_pu")
        do = c2.selectbox("Dropoff area", sorted(dp.do_borough.unique()), key="predict_do")

        # auto-suggest a realistic distance for this exact route, from the real
        # curated data — instead of making someone guess a number cold
        available_dist = sorted(dp.trip_distance.unique())
        default_dist = available_dist[len(available_dist) // 2]
        route_hint = None
        if not route_dist.empty:
            m = route_dist[(route_dist.pu_borough == pu) & (route_dist.do_borough == do)]
            if not m.empty:
                route_hint = float(m.iloc[0].avg_distance)
                default_dist = min(available_dist, key=lambda x: abs(x - route_hint))

        c3, c4, c5 = st.columns(3)
        hour = c3.selectbox("Pickup hour", sorted(dp.pickup_hour.unique()))
        day_type = c4.selectbox("Day type", ["Weekday", "Weekend"])
        dow = 4 if day_type == "Weekday" else 7
        # key changes with the route so switching pu/do resets to that route's
        # own typical distance; picking within the same route keeps your choice
        dist = c5.selectbox("Trip distance (mi)", available_dist,
                            index=available_dist.index(default_dist), key=f"dist_{pu}_{do}")
        if route_hint is not None:
            c5.caption(f"Typical for this route: ~{route_hint:.0f} mi")

        if st.button("Predict duration", type="primary"):
            match = dp[(dp.pu_borough == pu) & (dp.do_borough == do) &
                       (dp.pickup_hour == hour) & (dp.pickup_dow == dow) &
                       (dp.trip_distance == dist)]
            if match.empty:
                st.error("No prediction for that combination — the grid may be incomplete.")
            else:
                with st.container(border=True):
                    st.metric("Predicted duration", f"{match.iloc[0].predicted_duration_min:.1f} min")

                if pu in BOROUGH_COORDS and do in BOROUGH_COORDS:
                    (pu_lat, pu_lon), (do_lat, do_lon) = BOROUGH_COORDS[pu], BOROUGH_COORDS[do]
                    path, is_real = road_route(pu_lat, pu_lon, do_lat, do_lon)
                    path_lat = [p[0] for p in path]
                    path_lon = [p[1] for p in path]

                    # Scattermap — real street tiles (Carto, no API key), same
                    # basemap as the Geography map, for one consistent look.
                    route = go.Figure()
                    route.add_trace(go.Scattermap(
                        lat=path_lat, lon=path_lon, mode="lines",
                        line=dict(width=3, color=CAT[0]), showlegend=False))
                    route.add_trace(go.Scattermap(
                        lat=[pu_lat, do_lat], lon=[pu_lon, do_lon], mode="markers+text",
                        marker=dict(size=16, color=[CAT[1], CAT[3]]),
                        text=[f"Pickup: {pu}", f"Dropoff: {do}"], textposition="top center",
                        showlegend=False))
                    route.update_layout(
                        map=dict(style="carto-positron", zoom=9,
                                center=dict(lat=sum(path_lat) / len(path_lat),
                                           lon=sum(path_lon) / len(path_lon))),
                        margin=dict(l=0, r=0, t=0, b=0), height=380, showlegend=False)
                    st.plotly_chart(route, use_container_width=True)
                    if is_real:
                        st.caption("An actual driving route (OSRM), following real roads — not a "
                                  "straight line between the two areas.")
                    else:
                        st.caption("⚠️ Couldn't reach the routing service just now, so this is a "
                                  "straight line between the two areas, not a real road route.")

                by_hour = (dp[(dp.pu_borough == pu) & (dp.do_borough == do) &
                              (dp.pickup_dow == dow) & (dp.trip_distance == dist)]
                           .sort_values("pickup_hour"))
                fig = px.line(by_hour, x="pickup_hour", y="predicted_duration_min",
                             title=f"Predicted duration across the day — {pu} → {do}, "
                                   f"{dist:.0f} mi, {day_type}",
                             labels={"pickup_hour": "hour of day", "predicted_duration_min": "minutes"})
                fig.add_scatter(x=[hour], y=[match.iloc[0].predicted_duration_min], mode="markers",
                                marker=dict(size=13, color=CAT[3]), name="Your pick", showlegend=False)
                st.plotly_chart(_area(fig), use_container_width=True)

                best = by_hour.loc[by_hour.predicted_duration_min.idxmin()]
                if int(best.pickup_hour) != hour:
                    saved = match.iloc[0].predicted_duration_min - best.predicted_duration_min
                    st.info(f"**If the time is flexible:** {int(best.pickup_hour):02d}:00 is the "
                           f"fastest hour for this exact route — about {saved:.0f} minutes quicker "
                           f"than your {hour:02d}:00 pick.")
