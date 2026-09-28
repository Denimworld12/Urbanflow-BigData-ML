"""The dashboard's Groq call, without the network: request shape, the
no-key fallback, error reporting, and .env loading."""
import io, json
import urllib.error
import pytest
from urbanflow.dashboard import ai


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def _reply(content):
    return _Resp(json.dumps({"choices": [{"message": {"role": "assistant", "content": content}}]}).encode())


@pytest.fixture
def captured(monkeypatch):
    sent = {}

    def fake_urlopen(req, timeout):
        sent["req"], sent["timeout"] = req, timeout
        return sent.pop("reply", _reply("  A plain answer.  "))

    monkeypatch.setattr(ai.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setenv("GROQ_API_KEY", "gsk_test")
    monkeypatch.delenv("GROQ_MODEL", raising=False)
    return sent


def test_no_key_returns_none_without_calling_out(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.setattr(ai.urllib.request, "urlopen", lambda *a, **k: pytest.fail("called Groq"))
    assert ai.groq_chat([{"role": "user", "content": "hi"}]) is None


def test_request_shape(captured):
    msgs = [{"role": "system", "content": "Facts: x"}, {"role": "user", "content": "hi"}]
    assert ai.groq_chat(msgs) == "A plain answer."
    req = captured["req"]
    assert req.full_url == "https://api.groq.com/openai/v1/chat/completions"
    assert req.get_method() == "POST"
    assert req.get_header("Authorization") == "Bearer gsk_test"
    assert req.get_header("Content-type") == "application/json"
    assert not req.get_header("User-agent").startswith("Python-urllib")
    body = json.loads(req.data)
    assert body["model"] == "openai/gpt-oss-20b"
    assert body["messages"] == msgs          # the whole history is resent: that's the memory
    assert body["reasoning_effort"] == "low"
    assert body["max_tokens"] >= 600


def test_model_override_drops_gpt_oss_only_params(captured, monkeypatch):
    monkeypatch.setenv("GROQ_MODEL", "some/other-model")
    ai.groq_chat([{"role": "user", "content": "hi"}])
    body = json.loads(captured["req"].data)
    assert body["model"] == "some/other-model"
    assert "reasoning_effort" not in body


def test_http_error_surfaces_groqs_message(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "gsk_bad")

    def fail(req, timeout):
        body = io.BytesIO(json.dumps({"error": {"message": "Invalid API Key"}}).encode())
        raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, body)

    monkeypatch.setattr(ai.urllib.request, "urlopen", fail)
    with pytest.raises(RuntimeError, match="HTTP 401: Invalid API Key"):
        ai.groq_chat([{"role": "user", "content": "hi"}])


@pytest.mark.parametrize("content", [None, "", "   "])
def test_empty_answer_is_an_error_not_a_blank_reply(captured, content):
    captured["reply"] = _reply(content)
    with pytest.raises(RuntimeError, match="empty answer"):
        ai.groq_chat([{"role": "user", "content": "hi"}])


def test_load_dotenv(tmp_path, monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("UF_EMPTY", raising=False)
    monkeypatch.setenv("UF_PRESET", "from-shell")
    env = tmp_path / ".env"
    env.write_text('# comment\nexport GROQ_API_KEY="gsk_file"\nUF_EMPTY=\nUF_PRESET=from-file\nnot a pair\n')
    ai.load_dotenv(env)
    assert ai.os.environ["GROQ_API_KEY"] == "gsk_file"
    assert "UF_EMPTY" not in ai.os.environ               # blank template line sets nothing
    assert ai.os.environ["UF_PRESET"] == "from-shell"     # the real environment wins
    ai.load_dotenv(tmp_path / "missing.env")               # absent file is fine
