"""Tests for review_page.py: untrusted text (gazette text, LLM output, scraped pages) must be escaped."""
import importlib
import sys
import types
from datetime import datetime
from types import SimpleNamespace

import pytest

SCRIPT = "<script>alert(1)</script>"
ATTR_BREAKOUT = '"><img src=x onerror=alert(1)>'


@pytest.fixture
def review_page(monkeypatch):
    """Import review_page with a stub `persistence`, so these tests need no database or env vars."""
    monkeypatch.setitem(sys.modules, "persistence", types.SimpleNamespace(ReviewItem=object))
    monkeypatch.delitem(sys.modules, "review_page", raising=False)
    module = importlib.import_module("review_page")
    yield module
    sys.modules.pop("review_page", None)


def make_item(**overrides):
    draft = SimpleNamespace(
        id=7,
        status="pending",
        institution="Ministry of Finance",
        decision_number="12/2026",
        created_at=datetime(2026, 11, 4, 9, 30),
        last_error=None,
        to_email="info@example.gov",
        subject="Request for documents",
        body="Dear Sir or Madam,",
    )
    for key, value in overrides.items():
        if key in vars(draft):
            setattr(draft, key, value)
    return SimpleNamespace(
        draft=draft,
        gazette_id="G-100",
        signal_title="New regulation",
        expected_content="Annual report",
    )


def test_every_untrusted_field_is_escaped(review_page):
    item = make_item(
        institution=SCRIPT,
        decision_number=SCRIPT,
        last_error=SCRIPT,
        status="failed",
        subject=SCRIPT,
        body=SCRIPT,
        to_email=ATTR_BREAKOUT,
    )
    item.gazette_id = SCRIPT
    item.signal_title = SCRIPT
    item.expected_content = SCRIPT

    html = review_page._card(item)

    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    # The e-mail address goes into an attribute, so a quote must not be able to break out of it.
    assert '"><img' not in html
    assert "<img" not in html


def test_missing_optional_fields_render_placeholders(review_page):
    item = make_item(to_email=None, subject=None, body=None)
    item.expected_content = None
    item.gazette_id = None
    item.signal_title = None

    html = review_page._card(item)

    assert html.count("(none)") == 3
    assert "What Gemini says" not in html


def test_failed_draft_shows_error_and_retry_label(review_page):
    html = review_page._card(make_item(status="failed", last_error="SMTP 550"))
    assert "send failed, approve to retry" in html
    assert "Last send error: SMTP 550" in html


def test_pending_draft_has_no_error_block(review_page):
    html = review_page._card(make_item())
    assert "pending review" in html
    assert "Last send error" not in html


def test_page_escapes_username_and_uses_nonce_on_style_and_script(review_page):
    page = review_page.render_review_page([make_item()], dry_run=False, user="<b>admin</b>", nonce="abc123")

    assert "<b>admin</b>" not in page
    assert "&lt;b&gt;admin&lt;/b&gt;" in page
    assert '<style nonce="abc123">' in page
    assert '<script nonce="abc123">' in page


def test_page_has_no_inline_event_handlers(review_page):
    """The CSP blocks inline handlers, so buttons must be wired with addEventListener."""
    page = review_page.render_review_page([make_item()], dry_run=False, user="admin", nonce="n")
    assert "onclick=" not in page.lower()
    assert "addEventListener" in page


def test_empty_queue_message(review_page):
    page = review_page.render_review_page([], dry_run=False, user="admin", nonce="n")
    assert "No drafts are waiting for review." in page
    assert "(0)" in page


def test_dry_run_and_live_banners(review_page):
    dry = review_page.render_review_page([], dry_run=True, user="admin", nonce="n")
    live = review_page.render_review_page([], dry_run=False, user="admin", nonce="n")
    assert "Dry-run is ON" in dry
    assert "Live mode" in live
    assert "Dry-run is ON" not in live