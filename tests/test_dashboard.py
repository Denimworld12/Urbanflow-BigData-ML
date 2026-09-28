"""The dashboard renders with no gold layer at all, and the AI box falls
back to a setup message (not an error) when no GROQ_API_KEY is set."""
from pathlib import Path
import pytest
from streamlit.testing.v1 import AppTest
from urbanflow import config

APP = str(Path(__file__).resolve().parents[1] / "src" / "urbanflow" / "dashboard" / "app.py")


@pytest.fixture
def app(tmp_path, monkeypatch):
    for name in ("GOLD", "CURATED"):
        d = tmp_path / name.lower()
        d.mkdir()
        monkeypatch.setattr(config, name, d)
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    # the repo's own .env must not leak a real key into this test
    monkeypatch.setattr("urbanflow.dashboard.ai.load_dotenv", lambda path: None)
    import streamlit as st
    st.cache_data.clear()
    at = AppTest.from_file(APP, default_timeout=30)
    at.session_state["authenticated"] = True
    return at.run()


def test_renders_empty_gold_without_errors(app):
    assert not app.exception
    warnings = " ".join(w.value for w in app.warning)
    assert "make predict-grid" in warnings and "make gold" in warnings


def test_ai_summary_without_key_explains_setup(app):
    summarize = next(b for b in app.button if b.label == "Summarize the key findings")
    summarize.click().run()
    assert not app.exception
    replies = [m for m in app.chat_message if m.name == "assistant"]
    assert replies and "GROQ_API_KEY" in replies[0].markdown[0].value


def _write_gold(name, df):
    out = config.GOLD / name
    out.mkdir()
    df.to_parquet(out / "part-0.parquet")


def test_ai_facts_without_card_cash_split_have_no_nan(app):
    """FHVHV has no payment type, so is_card is null on every tipping row;
    the facts the AI is given must not turn that into 'nan%'."""
    import pandas as pd
    import streamlit as st
    _write_gold("tipping", pd.DataFrame({
        "pu_borough": ["Manhattan", "Queens"], "is_card": pd.array([None, None], dtype="boolean"),
        "trips": [300, 100], "avg_tip_pct": [5.0, 4.0], "avg_fare": [30.0, 35.0],
        "pct_trips_with_tip": [20.0, 40.0]}))
    st.cache_data.clear()
    at = AppTest.from_file(APP, default_timeout=30)
    at.session_state["authenticated"] = True
    at.run()
    assert not at.exception
    facts = at.code[0].value
    assert "nan" not in facts.lower()
    assert "Share of trips with a tip: 25%" in facts       # trip-weighted: (300*20 + 100*40) / 400
