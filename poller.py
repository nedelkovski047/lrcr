"""
Autonomous gazette poller for the Local Regulatory Change Radar (LRCR).

Fetches https://www.bitola.gov.mk/sluzbeni-glasnici/ (every gazette since
2011, on one page, grouped under "Службени гласници <YEAR> година"
headings), finds gazettes that are not in the database yet, and runs them
through Tier 1 -> Tier 2 -> Tier 3 -> persistence.

gazette_id is derived from the *link text and year heading*, not the file
name: file names on that site are inconsistent ("Sluzben-glasnik_3.pdf",
"O.Bitola-SG-br.-11_1.pdf"), link text is always "Службен гласник бр N".
Result: "bitola-2026-11". The file name is only a fallback.

Safety rails (all env-configurable):
    LRCR_POLL_MAX_NEW_PER_RUN   default 3   never push 200 old gazettes through Gemini
    LRCR_POLL_MIN_YEAR          default current year - 1
    LRCR_POLL_MAX_FAILURES      default 3   stop retrying a gazette that keeps failing
    LRCR_POLL_INTERVAL_HOURS    default 12
    LRCR_POLL_STARTUP_DELAY_S   default 15  first run shortly after boot (0 disables)
    LRCR_POLL_GAZETTE_DELAY_S   default 60  pause between gazettes in a run so Gemini's
                                             per-minute token quota resets (0 disables)
    LRCR_POLL_ENABLED           default 1

The pipeline function and the shared aiohttp session are injected by
main.py to avoid a circular import (main -> poller -> main).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable, Final, Optional
from urllib.parse import unquote, urljoin, urlparse

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from bs4 import BeautifulSoup, Tag
from pypdf.errors import PdfReadError

from pdf_parser import GazetteTextEmpty, LLMAnalysisError, analyze_gazette_with_llm, extract_text_from_pdf
from persistence import is_gazette_processed, persist_run_result, poll_failure_count, record_poll_attempt
from schemas import GazetteAnalysisResult

logger = logging.getLogger("lrcr.poller")

# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

GAZETTE_INDEX_URL: Final[str] = os.environ.get(
    "LRCR_GAZETTE_INDEX_URL", "https://www.bitola.gov.mk/sluzbeni-glasnici/"
)
USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36 LRCR-Radar/1.0"
)
HTTP_TIMEOUT_S: Final[float] = 60.0
MAX_PDF_BYTES: Final[int] = 80 * 1024 * 1024
POLL_ENABLED: Final[bool] = os.environ.get("LRCR_POLL_ENABLED", "1") == "1"
POLL_INTERVAL_HOURS: Final[float] = float(os.environ.get("LRCR_POLL_INTERVAL_HOURS", "12"))
POLL_STARTUP_DELAY_S: Final[float] = float(os.environ.get("LRCR_POLL_STARTUP_DELAY_S", "15"))
POLL_MAX_NEW_PER_RUN: Final[int] = int(os.environ.get("LRCR_POLL_MAX_NEW_PER_RUN", "3"))
POLL_MIN_YEAR: Final[int] = int(os.environ.get("LRCR_POLL_MIN_YEAR", str(datetime.now().year - 1)))
POLL_MAX_FAILURES: Final[int] = int(os.environ.get("LRCR_POLL_MAX_FAILURES", "3"))
# Gemini bills against a per-minute (not per-request) token quota. A pause between gazettes — after
# every attempt, success or failure, since a failure can still happen only after the tokens were
# already sent to the API — gives that window time to clear before the next big document lands on it.
POLL_GAZETTE_DELAY_S: Final[float] = float(os.environ.get("LRCR_POLL_GAZETTE_DELAY_S", "60"))

_YEAR_RE = re.compile(r"\b(20[1-4]\d)\b")
_NUMBER_IN_TEXT_RE = re.compile(r"(?:бр|br|број|broj)\.?\s*-?\s*(\d{1,3})", re.IGNORECASE)
_NUMBER_IN_FILE_RE = re.compile(r"(?:br\.?-?|_|-)(\d{1,3})(?=[_.\-\s]|$)", re.IGNORECASE)
_SAFE_RE = re.compile(r"[^A-Za-z0-9Ѐ-ӿ._-]+")

PipelineFn = Callable[[GazetteAnalysisResult], Awaitable[object]]


# --------------------------------------------------------------------------- #
# Data classes
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class GazetteLink:
    gazette_id: str
    url: str
    year: Optional[int]
    number: Optional[int]
    link_text: str

    @property
    def filename(self) -> str:
        return Path(unquote(urlparse(self.url).path)).name


@dataclass
class PollSummary:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    links_found: int = 0
    candidates: int = 0
    processed: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    skipped_existing: int = 0
    skipped_capped: int = 0
    skipped_old: int = 0
    skipped_failed_before: int = 0
    error: Optional[str] = None
    duration_ms: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "started_at": self.started_at.isoformat(),
            "duration_ms": self.duration_ms,
            "links_found": self.links_found,
            "candidates": self.candidates,
            "processed": self.processed,
            "failed": self.failed,
            "skipped": {
                "already_processed": self.skipped_existing,
                "older_than_min_year": self.skipped_old,
                "over_per_run_cap": self.skipped_capped,
                "failed_too_often": self.skipped_failed_before,
            },
            "error": self.error,
        }


# --------------------------------------------------------------------------- #
# Index page parsing
# --------------------------------------------------------------------------- #


def _year_from_context(anchor: Tag) -> Optional[int]:
    """Year from the link text, else the nearest preceding heading-like text with a year."""
    m = _YEAR_RE.search(anchor.get_text(" ", strip=True))
    if m:
        return int(m.group(1))
    for node in anchor.find_all_previous(["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b", "p"]):
        txt = node.get_text(" ", strip=True)
        if node.find("a") is not None and node.name == "p":
            continue  # a paragraph that itself holds gazette links is not a heading
        m = _YEAR_RE.search(txt)
        if m and len(txt) <= 60:  # "Службени гласници 2026 година", not a long paragraph
            return int(m.group(1))
    return None


def _year_from_url(url: str) -> Optional[int]:
    m = re.search(r"/uploads/(20[1-4]\d)/", url)
    return int(m.group(1)) if m else None


def _number_from(text: str, filename: str) -> Optional[int]:
    m = _NUMBER_IN_TEXT_RE.search(text)
    if m:
        return int(m.group(1))
    m = _NUMBER_IN_FILE_RE.search(Path(filename).stem)
    return int(m.group(1)) if m else None


def _fallback_id(filename: str) -> str:
    stem = _SAFE_RE.sub("-", Path(filename).stem).strip("-.").lower()
    return f"bitola-file-{stem or int(time.time())}"


def parse_gazette_index(html: str, base_url: str) -> list[GazetteLink]:
    """
    All gazette PDF links on the index page, newest first, deduplicated by id.

    Ordering: (year desc, number desc, document order) — the page lists each
    year ascending by number, so within a year the later links are newer.
    """
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    out: list[tuple[int, int, int, GazetteLink]] = []
    for pos, a in enumerate(soup.find_all("a", href=True)):
        href = a["href"].strip()
        if not re.search(r"\.pdf(?:$|[?#])", href, re.IGNORECASE):
            continue
        url = urljoin(base_url, href)
        text = a.get_text(" ", strip=True)
        filename = Path(unquote(urlparse(url).path)).name
        year = _year_from_context(a) or _year_from_url(url)
        number = _number_from(text, filename)
        if year and number:
            gid = f"bitola-{year}-{number:02d}"
        else:
            gid = _fallback_id(filename)
            logger.warning("could not derive year/number for %r (%s) — using %s", text, filename, gid)
        if gid in seen:
            continue
        seen.add(gid)
        out.append((year or 0, number or 0, pos, GazetteLink(gid, url, year, number, text)))
    out.sort(key=lambda t: (-t[0], -t[1], -t[2]))
    return [t[3] for t in out]


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #


async def fetch_index_html(client: httpx.AsyncClient) -> tuple[str, str]:
    """Returns (html, final_url) — final_url is the base for relative links after redirects."""
    resp = await client.get(GAZETTE_INDEX_URL)
    resp.raise_for_status()
    return resp.text, str(resp.url)


async def download_pdf(client: httpx.AsyncClient, url: str) -> str:
    """Stream a PDF to a temp file. Returns the path; caller deletes it."""
    fd, path = tempfile.mkstemp(prefix="lrcr_poll_", suffix=".pdf")
    size = 0
    try:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with os.fdopen(fd, "wb") as fh:
                first = True
                async for chunk in resp.aiter_bytes(256 * 1024):
                    if first:
                        if not chunk.startswith(b"%PDF-"):
                            raise ValueError("response is not a PDF (missing %PDF- header)")
                        first = False
                    size += len(chunk)
                    if size > MAX_PDF_BYTES:
                        raise ValueError(f"PDF exceeds {MAX_PDF_BYTES // (1024 * 1024)} MB")
                    fh.write(chunk)
        if size == 0:
            raise ValueError("empty response")
        return path
    except Exception:
        _unlink_quietly(path)
        raise


def _unlink_quietly(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# One gazette end to end
# --------------------------------------------------------------------------- #


async def process_gazette_link(
    link: GazetteLink,
    client: httpx.AsyncClient,
    run_pipeline: PipelineFn,
) -> None:
    """
    Download -> Tier 1 -> Tier 2/3 -> persist. Raises on failure so the
    caller can record it; always removes the temp file.
    """
    logger.info("poller: processing %s <- %s", link.gazette_id, link.url)
    tmp_path = await download_pdf(client, link.url)
    try:
        text = await asyncio.to_thread(extract_text_from_pdf, tmp_path)   # Tier 1a
    finally:
        _unlink_quietly(tmp_path)  # nothing below needs the file
    analysis = await analyze_gazette_with_llm(link.gazette_id, text)       # Tier 1b

    response = await run_pipeline(analysis)                                # Tier 2 + 3
    actions = getattr(response, "actions", [])
    summary = getattr(response, "summary", None)
    outcome = await persist_run_result(
        analysis, actions, source="poller", filename=link.filename,
        extracted_chars=len(text), summary=summary,
    )
    if outcome.status == "error":
        raise RuntimeError(f"persist failed: {outcome.error}")
    if outcome.status == "duplicate":
        logger.warning("poller: %s was stored by someone else meanwhile", link.gazette_id)
    logger.info("poller: %s done — %s", link.gazette_id, summary)


# --------------------------------------------------------------------------- #
# The poll job
# --------------------------------------------------------------------------- #

_poll_lock = asyncio.Lock()
last_summary: Optional[PollSummary] = None


async def poll_bitola_gazettes(
    run_pipeline: PipelineFn,
    *,
    max_new: int = POLL_MAX_NEW_PER_RUN,
    min_year: int = POLL_MIN_YEAR,
    max_failures: int = POLL_MAX_FAILURES,
) -> PollSummary:
    """
    One polling pass. Never raises — everything lands in the returned
    PollSummary (also kept in `poller.last_summary` for /healthz).
    Overlapping runs are skipped, not queued.
    """
    global last_summary
    summary = PollSummary()
    t0 = time.perf_counter()

    if _poll_lock.locked():
        summary.error = "previous poll still running"
        logger.warning("poller: skipped — previous run still in progress")
        summary.duration_ms = int((time.perf_counter() - t0) * 1000)
        return summary

    async with _poll_lock:
        try:
            async with httpx.AsyncClient(
                timeout=HTTP_TIMEOUT_S, follow_redirects=True, headers={"User-Agent": USER_AGENT}
            ) as client:
                html, base_url = await fetch_index_html(client)
                links = parse_gazette_index(html, base_url)
                summary.links_found = len(links)
                logger.info("poller: %d gazette links on index page", len(links))

                todo: list[GazetteLink] = []
                for link in links:
                    if link.year is not None and link.year < min_year:
                        summary.skipped_old += 1
                        continue
                    if await is_gazette_processed(link.gazette_id):
                        summary.skipped_existing += 1
                        continue
                    if await poll_failure_count(link.gazette_id) >= max_failures:
                        summary.skipped_failed_before += 1
                        continue
                    todo.append(link)
                summary.candidates = len(todo)
                if len(todo) > max_new:
                    summary.skipped_capped = len(todo) - max_new
                    logger.info("poller: %d new gazettes, capped to %d this run", len(todo), max_new)
                    todo = todo[:max_new]

                for index, link in enumerate(todo):
                    try:
                        await process_gazette_link(link, client, run_pipeline)
                        summary.processed.append(link.gazette_id)
                        await record_poll_attempt(link.gazette_id, link.url, "processed")
                    except (GazetteTextEmpty, PdfReadError, ValueError) as exc:
                        # Scanned / broken PDF: will not fix itself. Log and move on.
                        msg = f"{type(exc).__name__}: {exc}"
                        summary.failed[link.gazette_id] = msg
                        logger.warning("poller: %s unusable — %s", link.gazette_id, msg)
                        await record_poll_attempt(link.gazette_id, link.url, "failed", msg)
                    except (LLMAnalysisError, httpx.HTTPError, RuntimeError) as exc:
                        msg = f"{type(exc).__name__}: {exc}"
                        summary.failed[link.gazette_id] = msg
                        logger.error("poller: %s failed — %s", link.gazette_id, msg)
                        await record_poll_attempt(link.gazette_id, link.url, "failed", msg)
                    except Exception as exc:  # noqa: BLE001 — one gazette must not kill the run
                        msg = f"{type(exc).__name__}: {exc}"
                        summary.failed[link.gazette_id] = msg
                        logger.exception("poller: %s crashed", link.gazette_id)
                        await record_poll_attempt(link.gazette_id, link.url, "failed", msg)
                    # Pause so Gemini's per-minute token quota has time to reset before the next
                    # gazette. Runs after every attempt (success or failure both already spent
                    # tokens once the request reached the API) and is skipped after the last
                    # gazette in this run, since nothing is left to protect it from.
                    if POLL_GAZETTE_DELAY_S > 0 and index < len(todo) - 1:
                        await asyncio.sleep(POLL_GAZETTE_DELAY_S)
        except httpx.HTTPError as exc:
            summary.error = f"index fetch failed: {exc}"
            logger.error("poller: %s", summary.error)
        except Exception as exc:  # noqa: BLE001
            summary.error = f"{type(exc).__name__}: {exc}"
            logger.exception("poller: run crashed")

    summary.duration_ms = int((time.perf_counter() - t0) * 1000)
    logger.info("poller: done in %d ms — processed=%s failed=%s existing=%d old=%d capped=%d",
                summary.duration_ms, summary.processed, list(summary.failed), summary.skipped_existing,
                summary.skipped_old, summary.skipped_capped)
    last_summary = summary
    return summary


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #

JOB_ID: Final[str] = "poll_bitola_gazettes"


def build_scheduler(run_pipeline: PipelineFn) -> AsyncIOScheduler:
    """
    AsyncIOScheduler with the poll job every POLL_INTERVAL_HOURS and, if
    POLL_STARTUP_DELAY_S > 0, one extra run shortly after boot.

    max_instances=1 + coalesce=True: a run that overlaps the next tick is
    not doubled up; misfire_grace_time lets a tick that fired while the
    laptop was asleep still run once it wakes.
    """
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        poll_bitola_gazettes, IntervalTrigger(hours=POLL_INTERVAL_HOURS),
        args=[run_pipeline], id=JOB_ID, name="Poll bitola.gov.mk for new gazettes",
        max_instances=1, coalesce=True, misfire_grace_time=3600, replace_existing=True,
    )
    if POLL_STARTUP_DELAY_S > 0:
        scheduler.add_job(
            poll_bitola_gazettes, DateTrigger(run_date=datetime.now(timezone.utc) + timedelta(seconds=POLL_STARTUP_DELAY_S)),
            args=[run_pipeline], id=f"{JOB_ID}_startup", name="Initial poll after startup",
            max_instances=1, misfire_grace_time=600,
        )
    return scheduler
