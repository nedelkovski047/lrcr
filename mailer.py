"""
Outgoing FOIA e-mail through Resend (https://resend.com), replacing the SMTP code.

Environment variables
    RESEND_API_KEY        required for real sending (Resend dashboard -> API Keys). Never put it in code.
    LRCR_EMAIL_DRY_RUN    default "true": dispatch_foia_email() only logs, nothing is sent.
                          Set to "false" to send for real.
    LRCR_MAIL_FROM        optional override of the sender. Default is the Resend test sender below.
    LRCR_REPLY_TO         optional Reply-To address. Defaults to LRCR_SENDER_EMAIL. The test sender cannot
                          receive mail, so this must be a mailbox you read, otherwise the institution's answer
                          to a FOIA request goes nowhere.

Requester identity (printed in the FOIA letter by foia_generator.py, which imports these constants; unchanged
from the SMTP version)
    LRCR_SENDER_NAME, LRCR_SENDER_EMAIL, LRCR_SENDER_ADDRESS, LRCR_SENDER_PHONE
    While name, e-mail or address is still an unset placeholder, live sending is refused.

Two entry points:

    send_foia_email(to_email, subject, body) -> bool
        Blocking, one attempt, prints the error and returns False on failure. Sends for real; it does NOT
        look at the dry-run switch. Use it from scripts.

    await dispatch_foia_email(to, subject, body, message_id) -> MailResult
        What main.py calls (only from POST /review/{id}/approve). Honours dry-run, runs the blocking SDK call in
        a worker thread so the event loop keeps serving requests, never raises, and returns the error text
        so the review queue can show why a send failed.

IMPORTANT (Resend sandbox): "onboarding@resend.dev" can only deliver to the e-mail address of your own Resend
account. Sending to an institution fails (HTTP 403) until you verify a domain in Resend and set
LRCR_MAIL_FROM to an address on it.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sys
from dataclasses import dataclass
from html import escape
from typing import Optional

import resend

logger = logging.getLogger("lrcr.mailer")

def _sender(var: str, default: str = "") -> str:
    return (os.environ.get(var) or "").strip() or default


# foia_generator.py reads these at import time (`name: str = mailer.SENDER_NAME` ...). They must exist even in dry-run.
SENDER_NAME: str = _sender("LRCR_SENDER_NAME", "[SET LRCR_SENDER_NAME]")
_PLACEHOLDER_EMAIL = "set-lrcr-sender-email@example.com"   # looks like an e-mail so a validating model still accepts it
SENDER_EMAIL: str = _sender("LRCR_SENDER_EMAIL", _sender("LRCR_REPLY_TO", _PLACEHOLDER_EMAIL))
SENDER_ADDRESS: str = _sender("LRCR_SENDER_ADDRESS", "[SET LRCR_SENDER_ADDRESS]")
SENDER_PHONE: str = _sender("LRCR_SENDER_PHONE", "")


def sender_is_placeholder() -> Optional[str]:
    """Return the name of the first required identity variable that is still unset, else None."""
    for var, value in (("LRCR_SENDER_NAME", SENDER_NAME), ("LRCR_SENDER_EMAIL", SENDER_EMAIL),
                       ("LRCR_SENDER_ADDRESS", SENDER_ADDRESS)):
        if not value or value.startswith("[SET ") or value == _PLACEHOLDER_EMAIL:
            return var
    return None


def __getattr__(name: str) -> str:
    # Safety net: any other SENDER_* constant the (unseen) foia_generator.py asks for resolves to
    # LRCR_<name> or "" instead of crashing the whole app at import time.
    if name.startswith("SENDER_"):
        logger.warning("mailer.%s was requested but is not defined; using env LRCR_%s (or empty)", name, name)
        return _sender(f"LRCR_{name}")
    raise AttributeError(f"module 'mailer' has no attribute {name!r}")


DEFAULT_FROM = "Граѓанска Иницијатива Радар <onboarding@resend.dev>"
FROM_ADDRESS: str = (os.environ.get("LRCR_MAIL_FROM") or "").strip() or DEFAULT_FROM
REPLY_TO: Optional[str] = _sender("LRCR_REPLY_TO") or (SENDER_EMAIL if SENDER_EMAIL != _PLACEHOLDER_EMAIL else None)
SEND_TIMEOUT_S = 60.0

resend.api_key = os.environ.get("RESEND_API_KEY")


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


EMAIL_DRY_RUN: bool = _env_flag("LRCR_EMAIL_DRY_RUN", True)


@dataclass(frozen=True)
class MailResult:
    sent: bool
    dry_run: bool
    error: Optional[str] = None
    provider_id: Optional[str] = None      # Resend's id for the e-mail, useful when checking the Resend dashboard


def _html_body(body: str) -> str:
    """
    Plain text -> small HTML e-mail. The text comes from an LLM-drafted template, so it is escaped first (a stray
    "<" must not become markup), then line breaks become <br>.
    """
    text = escape(body.replace("\r\n", "\n").replace("\r", "\n"))
    return (
        '<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5;color:#111;">'
        f'{text.replace(chr(10), "<br>")}'
        "</div>"
    )


def _idempotency_key(message_id: str) -> str:
    # Same draft + same sender => Resend treats a repeated request (for example an approve retried after a
    # timeout) as the same e-mail for 24 hours. The sender is part of the key because Resend rejects a reused
    # key whose payload changed, which would otherwise happen after you switch LRCR_MAIL_FROM.
    tag = hashlib.sha1(FROM_ADDRESS.encode("utf-8")).hexdigest()[:8]
    return f"foia-{message_id}-{tag}"[:250]


def _send(to_email: str, subject: str, body: str, message_id: str = "") -> tuple[bool, Optional[str], Optional[str]]:
    """One blocking attempt. Returns (ok, error, provider_id). Never raises."""
    if not resend.api_key:
        return False, "RESEND_API_KEY is not set", None
    to_email = (to_email or "").strip()
    if "@" not in to_email:
        return False, f"invalid recipient address {to_email!r}", None
    params: resend.Emails.SendParams = {
        "from": FROM_ADDRESS,
        "to": [to_email],
        "subject": subject,
        "html": _html_body(body),
        "text": body,                       # plain-text alternative: better deliverability, readable everywhere
    }
    if REPLY_TO:
        params["reply_to"] = REPLY_TO
    options: resend.Emails.SendOptions = {"idempotency_key": _idempotency_key(message_id)} if message_id else {}
    try:
        response = resend.Emails.send(params, options) if options else resend.Emails.send(params)
    except Exception as exc:  # noqa: BLE001 — the SDK raises several error types plus network errors
        return False, f"{type(exc).__name__}: {exc}", None
    provider_id = response.get("id") if isinstance(response, dict) else getattr(response, "id", None)
    return True, None, provider_id


def send_foia_email(to_email: str, subject: str, body: str) -> bool:
    """Send one FOIA e-mail through Resend. True on success; on failure print the error and return False."""
    ok, error, provider_id = _send(to_email, subject, body)
    if ok:
        print(f"[mailer] sent to {to_email} (resend id {provider_id})", file=sys.stderr, flush=True)
        return True
    print(f"[mailer] send to {to_email} FAILED: {error}", file=sys.stderr, flush=True)
    return False


async def dispatch_foia_email(to: str, subject: str, body: str, message_id: str) -> MailResult:
    """Async entry point used by the approve endpoint. See the module docstring."""
    if EMAIL_DRY_RUN:
        logger.info("DRY RUN: would send %r to %s (message_id=%s); nothing was sent", subject, to, message_id)
        return MailResult(sent=False, dry_run=True)
    missing = sender_is_placeholder()
    if missing:
        return MailResult(sent=False, dry_run=False, error=f"requester identity is still a placeholder: set {missing} (and the other LRCR_SENDER_* variables)")
    try:
        ok, error, provider_id = await asyncio.wait_for(
            asyncio.to_thread(_send, to, subject, body, message_id), timeout=SEND_TIMEOUT_S
        )
    except asyncio.TimeoutError:
        # The worker thread may still finish. Approving again is safe within 24 h because of the idempotency key.
        error, ok, provider_id = f"no answer from Resend within {SEND_TIMEOUT_S:.0f}s (the e-mail may still have been sent)", False, None
    if ok:
        logger.info("sent to %s (message_id=%s, resend id %s)", to, message_id, provider_id)
        return MailResult(sent=True, dry_run=False, provider_id=provider_id)
    logger.error("send to %s failed (message_id=%s): %s", to, message_id, error)
    return MailResult(sent=False, dry_run=False, error=error)
