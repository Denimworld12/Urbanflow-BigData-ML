"""The dashboard's AI summary + chat, kept out of app.py so it can be tested
without starting Streamlit.

One HTTPS call to Groq's OpenAI-compatible chat endpoint via stdlib urllib —
no SDK dependency for what is otherwise a single POST request. The model
never sees the data itself, only the short fact list the dashboard compiles
from the gold tables (see app.py _findings_facts).
"""
from __future__ import annotations
import json, os
import urllib.error
import urllib.request
from pathlib import Path

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-oss-20b"
# Cloudflare (fronting Groq's API) blocks urllib's default
# "Python-urllib/x.y" User-Agent as bot traffic (error 1010).
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def load_dotenv(path: Path) -> None:
    """Read KEY=value lines from a .env file into os.environ, without
    overriding anything already set. Docker Compose reads .env by itself;
    `make dash` does not, so without this a key in .env never reaches the
    dashboard on the native path."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip().strip('"').strip("'")
        if key and value:
            os.environ.setdefault(key, value)


def build_request(messages: list[dict], api_key: str, model: str) -> urllib.request.Request:
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.3,
        # gpt-oss models spend some of this budget on an internal reasoning
        # pass before the visible answer — too low and content comes back
        # empty even though the request "succeeds".
        "max_tokens": 600,
    }
    if model.startswith("openai/gpt-oss"):
        # a plain-English summary of a few facts needs little reasoning; low
        # effort leaves more of max_tokens for the visible answer
        payload["reasoning_effort"] = "low"
    return urllib.request.Request(
        GROQ_URL, data=json.dumps(payload).encode(), method="POST",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
                 "User-Agent": USER_AGENT})


def groq_chat(messages: list[dict]) -> str | None:
    """`messages` is the full conversation so far (system + history + new
    question) — that's what gives it "memory": each call resends everything
    said before, since Groq itself is stateless between calls.
    Returns None (caller shows setup instructions) if no key is configured;
    raises RuntimeError with Groq's own error message if the call fails."""
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        return None
    req = build_request(messages, api_key, os.environ.get("GROQ_MODEL", DEFAULT_MODEL))
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # the status line alone ("HTTP Error 401") hides the useful part —
        # Groq says "Invalid API Key" or "model decommissioned" in the body
        try:
            detail = json.loads(e.read())["error"]["message"]
        except Exception:
            detail = e.reason
        raise RuntimeError(f"HTTP {e.code}: {detail}") from e
    content = (data["choices"][0]["message"].get("content") or "").strip()
    if not content:
        raise RuntimeError("the model returned an empty answer (its token budget ran out "
                           "before it finished) — try asking again")
    return content
