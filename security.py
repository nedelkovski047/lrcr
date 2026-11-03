"""
HTTP Basic authentication for the whole LRCR API.

    ADMIN_USER        username (required)
    ADMIN_PASSWORD    password, at least 12 characters (required)

BasicAuthMiddleware wraps the entire ASGI app, so EVERY route is protected,
including /docs, /redoc, /openapi.json and /healthz, which per-route dependencies
would miss. Behaviour:

  * Missing or invalid credentials -> 401 with a WWW-Authenticate header (browsers
    show a login prompt), after a 1 second delay that slows password guessing.
  * Credentials are compared in constant time.
  * If ADMIN_USER / ADMIN_PASSWORD are missing or too weak, the server fails
    CLOSED: every request gets 503, and main.py refuses to start.
  * State-changing requests (anything but GET/HEAD/OPTIONS) that a browser marks as
    cross-site (Origin different from Host, Origin "null", or Sec-Fetch-Site:
    cross-site) get 403. Browsers attach cached Basic credentials to cross-site
    requests automatically, so without this a hostile web page could make your browser
    "approve" a draft. Non-browser clients (curl, httpx) send no Origin and are unaffected.
  * WebSocket connections are refused.

Basic auth sends the password with every request: only expose the server over HTTPS
(Render terminates TLS for you). Use a long random password.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import os
import secrets
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Optional
from urllib.parse import urlsplit

logger = logging.getLogger("lrcr.security")

MIN_PASSWORD_LENGTH = 12
FAILED_AUTH_DELAY_S = 1.0
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


@dataclass(frozen=True)
class AuthConfig:
    username: str
    password: str


def load_auth_config(env: Mapping[str, str] = os.environ) -> tuple[Optional[AuthConfig], Optional[str]]:
    """Return (config, None) when ADMIN_USER / ADMIN_PASSWORD are usable, else (None, reason)."""
    username = (env.get("ADMIN_USER") or "").strip()
    password = env.get("ADMIN_PASSWORD") or ""
    if not username or not password:
        return None, "ADMIN_USER and ADMIN_PASSWORD must both be set"
    if ":" in username:
        return None, "ADMIN_USER must not contain ':' (Basic auth cannot represent it)"
    if len(password) < MIN_PASSWORD_LENGTH:
        return None, f"ADMIN_PASSWORD must be at least {MIN_PASSWORD_LENGTH} characters"
    return AuthConfig(username, password), None


def _headers(scope: Scope) -> dict[str, str]:
    return {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}


def _parse_basic(header: Optional[str]) -> Optional[tuple[str, str]]:
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "basic" or not token.strip():
        return None
    try:
        decoded = base64.b64decode(token.strip(), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    user, sep, password = decoded.partition(":")
    return (user, password) if sep else None


def _cross_site(method: str, headers: Mapping[str, str]) -> bool:
    if method in _SAFE_METHODS:
        return False
    if headers.get("sec-fetch-site", "").lower() == "cross-site":
        return True
    origin = headers.get("origin")
    if origin is None:
        return False                       # not a browser request (curl, httpx, ...)
    if origin == "null":
        return True
    return urlsplit(origin).netloc.lower() != headers.get("host", "").lower()


async def _respond(send: Send, status: int, payload: dict[str, object], extra: Optional[list[tuple[bytes, bytes]]] = None) -> None:
    body = json.dumps(payload).encode()
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode()),
               (b"cache-control", b"no-store")] + (extra or [])
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


class BasicAuthMiddleware:
    """Pure ASGI middleware (no BaseHTTPMiddleware: it must not buffer or break streaming)."""

    def __init__(self, app: ASGIApp, config: Optional[AuthConfig] = None, problem: Optional[str] = None) -> None:
        self.app = app
        self.config = config
        self.problem = problem or ("authentication is not configured" if config is None else None)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        kind = scope["type"]
        if kind == "lifespan":
            await self.app(scope, receive, send)
            return
        if kind == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        if kind != "http":
            return

        if self.config is None:
            await _respond(send, 503, {"detail": f"server authentication is not configured: {self.problem}"})
            return

        headers = _headers(scope)
        creds = _parse_basic(headers.get("authorization"))
        # Both comparisons always run (no short-circuit) so timing does not reveal which field was wrong.
        user_ok = secrets.compare_digest((creds[0] if creds else "").encode(), self.config.username.encode())
        pass_ok = secrets.compare_digest((creds[1] if creds else "").encode(), self.config.password.encode())
        if not (creds and user_ok and pass_ok):
            await asyncio.sleep(FAILED_AUTH_DELAY_S)
            await _respond(send, 401, {"detail": "authentication required"},
                           [(b"www-authenticate", b'Basic realm="LRCR", charset="UTF-8"')])
            return

        if _cross_site(scope.get("method", "GET"), headers):
            await _respond(send, 403, {"detail": "cross-site state-changing requests are not allowed"})
            return

        scope["auth_user"] = self.config.username
        await self.app(scope, receive, send)
