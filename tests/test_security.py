"""Tests for security.py: Basic auth middleware and cross-site request blocking."""
import asyncio
import base64

import pytest

import security
from security import (
    AuthConfig,
    BasicAuthMiddleware,
    MIN_PASSWORD_LENGTH,
    _cross_site,
    _parse_basic,
    load_auth_config,
)

GOOD = AuthConfig("admin", "correct-horse-battery")


@pytest.fixture(autouse=True)
def no_failed_auth_delay(monkeypatch):
    """The middleware sleeps 1s on every failed login; skip that in tests."""
    monkeypatch.setattr(security, "FAILED_AUTH_DELAY_S", 0)


def basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def run(config, *, method="GET", headers=None, scope_type="http", problem=None):
    """Send one request through the middleware. Returns (messages_sent, inner_app_calls)."""
    inner_calls = []
    sent = []

    async def inner(scope, receive, send):
        inner_calls.append(scope)
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    scope = {
        "type": scope_type,
        "method": method,
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    }
    asyncio.run(BasicAuthMiddleware(inner, config, problem)(scope, receive, send))
    return sent, inner_calls


def status(sent):
    return sent[0]["status"]


# ---------- load_auth_config ----------

def test_config_accepts_valid_credentials():
    cfg, problem = load_auth_config({"ADMIN_USER": "admin", "ADMIN_PASSWORD": "x" * MIN_PASSWORD_LENGTH})
    assert problem is None
    assert cfg == AuthConfig("admin", "x" * MIN_PASSWORD_LENGTH)


@pytest.mark.parametrize("env", [
    {},
    {"ADMIN_USER": "admin"},
    {"ADMIN_PASSWORD": "x" * 20},
    {"ADMIN_USER": "   ", "ADMIN_PASSWORD": "x" * 20},
])
def test_config_rejects_missing_values(env):
    cfg, problem = load_auth_config(env)
    assert cfg is None
    assert "must both be set" in problem


def test_config_rejects_short_password():
    cfg, problem = load_auth_config({"ADMIN_USER": "admin", "ADMIN_PASSWORD": "x" * (MIN_PASSWORD_LENGTH - 1)})
    assert cfg is None
    assert str(MIN_PASSWORD_LENGTH) in problem


def test_config_rejects_colon_in_username():
    cfg, problem = load_auth_config({"ADMIN_USER": "ad:min", "ADMIN_PASSWORD": "x" * 20})
    assert cfg is None
    assert "':'" in problem


# ---------- _parse_basic ----------

def test_parse_basic_valid():
    assert _parse_basic(basic("admin", "secret")) == ("admin", "secret")


def test_parse_basic_keeps_colons_in_password():
    assert _parse_basic(basic("admin", "pa:ss:word")) == ("admin", "pa:ss:word")


@pytest.mark.parametrize("header", [
    None,
    "",
    "Bearer abc",
    "Basic ",
    "Basic !!!not-base64!!!",
    "Basic " + base64.b64encode(b"no-colon-here").decode(),
])
def test_parse_basic_rejects_bad_headers(header):
    assert _parse_basic(header) is None


# ---------- _cross_site ----------

@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
def test_safe_methods_are_never_blocked(method):
    assert _cross_site(method, {"origin": "https://evil.example", "host": "lrcr.example"}) is False


def test_post_without_origin_is_allowed_non_browser_client():
    assert _cross_site("POST", {"host": "lrcr.example"}) is False


def test_post_from_same_origin_is_allowed():
    assert _cross_site("POST", {"origin": "https://lrcr.example", "host": "lrcr.example"}) is False


def test_post_origin_host_comparison_ignores_case():
    assert _cross_site("POST", {"origin": "https://LRCR.example", "host": "lrcr.EXAMPLE"}) is False


def test_post_from_other_origin_is_blocked():
    assert _cross_site("POST", {"origin": "https://evil.example", "host": "lrcr.example"}) is True


def test_post_with_null_origin_is_blocked():
    assert _cross_site("POST", {"origin": "null", "host": "lrcr.example"}) is True


def test_post_marked_cross_site_by_browser_is_blocked():
    assert _cross_site("DELETE", {"sec-fetch-site": "cross-site", "host": "lrcr.example"}) is True


# ---------- BasicAuthMiddleware ----------

def test_missing_credentials_get_401_and_never_reach_the_app():
    sent, calls = run(GOOD)
    assert status(sent) == 401
    assert (b"www-authenticate", b'Basic realm="LRCR", charset="UTF-8"') in sent[0]["headers"]
    assert calls == []


def test_wrong_password_gets_401():
    sent, calls = run(GOOD, headers={"Authorization": basic("admin", "wrong-password!")})
    assert status(sent) == 401
    assert calls == []


def test_wrong_username_gets_401():
    sent, calls = run(GOOD, headers={"Authorization": basic("someone", GOOD.password)})
    assert status(sent) == 401
    assert calls == []


def test_valid_credentials_reach_the_app_with_user_set():
    sent, calls = run(GOOD, headers={"Authorization": basic(GOOD.username, GOOD.password)})
    assert status(sent) == 200
    assert calls[0]["auth_user"] == "admin"


def test_valid_credentials_but_cross_site_post_gets_403():
    headers = {
        "Authorization": basic(GOOD.username, GOOD.password),
        "Origin": "https://evil.example",
        "Host": "lrcr.example",
    }
    sent, calls = run(GOOD, method="POST", headers=headers)
    assert status(sent) == 403
    assert calls == []


def test_valid_credentials_same_origin_post_is_allowed():
    headers = {
        "Authorization": basic(GOOD.username, GOOD.password),
        "Origin": "https://lrcr.example",
        "Host": "lrcr.example",
    }
    sent, calls = run(GOOD, method="POST", headers=headers)
    assert status(sent) == 200
    assert len(calls) == 1


def test_fails_closed_when_auth_is_not_configured():
    sent, calls = run(None, headers={"Authorization": basic("admin", "anything-at-all")}, problem="not set")
    assert status(sent) == 503
    assert calls == []


def test_websocket_connections_are_refused():
    sent, calls = run(GOOD, scope_type="websocket")
    assert sent[0]["type"] == "websocket.close"
    assert calls == []


def test_lifespan_events_pass_through_without_auth():
    sent, calls = run(GOOD, scope_type="lifespan")
    assert len(calls) == 1