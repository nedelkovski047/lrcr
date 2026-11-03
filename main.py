"""
Local Regulatory Change Radar (LRCR) — pipeline orchestrator & HTTP API.

Human-in-the-loop: the pipeline NEVER sends e-mail. When Tier 2 cannot find a
document, Tier 3 drafts a FOIA request and stores it as `pending_review`; only a
person approving it through /review triggers the e-mail send.

Every endpoint (including /docs and /healthz) requires HTTP Basic authentication
with ADMIN_USER / ADMIN_PASSWORD (see security.py). The server refuses to start
without them.

Endpoints:
    POST /api/v1/upload-gazette    PDF upload -> Tier 1 (pypdf + Gemini)
                                   -> Tier 2 (scrapers) -> Tier 3 (FOIA drafts, queued)
    POST /api/v1/process-gazette   Pre-analysed GazetteAnalysisResult
                                   -> Tier 2 -> Tier 3
    GET  /api/v1/gazettes/{id}     Stored result of an earlier run
    POST /api/v1/poll-now          Trigger one poller pass by hand
    POST /api/v1/retry-now         Trigger one retry pass (failed scrapes) by hand
    GET  /healthz                  Poller / retry status and number of drafts awaiting review

    GET  /review                   Drafts awaiting review (HTML in a browser, JSON for API clients)
    POST /review/{id}/approve      Send that draft now, mark it sent
    POST /review/{id}/reject       Mark it rejected; it is never drafted or sent again

A background poller (poller.py, APScheduler) checks bitola.gov.mk every
12 hours for new gazettes and runs them through the same pipeline. It only ever
calls the shared pipeline, so it can queue drafts but cannot send anything.

Sending goes through mailer.py (Resend SDK, RESEND_API_KEY), and only from the approve endpoint.
LRCR_EMAIL_DRY_RUN defaults to true: approving a draft then only logs the message
and leaves it in the queue, until that is set to false and RESEND_API_KEY is set.

Every run is persisted (database.py / models.py: PostgreSQL via DATABASE_URL, SQLite locally).

Hardening for unattended 24/7 operation:
  * One draft per (institution, decision) through the foia_requests table
    (persistence.create_foia_draft): the same request is never queued twice and
    never sent twice, across gazettes, restarts or concurrent workers.
  * Failed scrapes are retried automatically by retry_worker.RetryWorker (backoff,
    capped attempts). Re-uploading a gazette that has retryable failures retries
    them instead of answering 409; a gazette with nothing left to retry still gets 409.

Run:
    set GEMINI_API_KEY=...            (PowerShell: $env:GEMINI_API_KEY="...")
    set ADMIN_USER=...  &  set ADMIN_PASSWORD=...   (12+ characters)
    uvicorn main:app --reload --port 8000
    # interactive docs at http://127.0.0.1:8000/docs  (asks for the same login)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import AsyncIterator, Literal, Optional

import aiohttp
from fastapi import Body, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from pypdf.errors import PdfReadError

import mailer
import poller
from database import SessionLocal, dispose_db, init_db
from foia_generator import UnknownInstitutionError, foia_from_scraper_result
from pdf_parser import (
    GazetteTextEmpty,
    LLMAnalysisError,
    analyze_gazette_with_llm,
    extract_text_from_pdf,
)
from persistence import (
    DraftOutcome,
    DraftView,
    RejectOutcome,
    SendOutcome,
    backfill_foia_requests_from_history,
    begin_send,
    count_pending_drafts,
    create_foia_draft,
    finish_send,
    gazette_exists,
    get_draft,
    list_review_items,
    load_gazette,
    persist_run_result,
    record_review_outcome,
    reject_draft,
)
from retry_worker import RETRY_ENABLED, RETRY_INTERVAL_MINUTES, RETRY_MAX_ATTEMPTS, RetryWorker
from review_page import render_review_page
from schemas import GazetteAnalysisResult, MissingDocumentRef, RegulatorySignal
from scrapers import USER_AGENT, ScraperResult, dispatch_scraper, resolve_scraper
from security import BasicAuthMiddleware, load_auth_config

__all__ = ["app", "GazetteAnalysisResult", "MissingDocumentRef", "RegulatorySignal"]

logger = logging.getLogger("lrcr.api")

# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

# Scrapes running at once, per request. Each Playwright scrape can hold a Chromium page in
# memory; on a small (512 MB) instance set LRCR_MAX_CONCURRENT_SCRAPES=1.
try:
    MAX_CONCURRENT_SCRAPES = max(1, int(os.environ.get("LRCR_MAX_CONCURRENT_SCRAPES", "3")))
except ValueError:
    MAX_CONCURRENT_SCRAPES = 3
SCRAPE_TIMEOUT_S = 180          # hard ceiling per reference (Playwright can be slow)
HTTP_TIMEOUT_S = 25
MAX_UPLOAD_BYTES = 50 * 1024 * 1024
UPLOAD_CHUNK_BYTES = 1024 * 1024

# --------------------------------------------------------------------------- #
# Outbound schemas
# --------------------------------------------------------------------------- #


class Action(str, Enum):
    SCRAPED_SUCCESSFULLY = "scraped_successfully"       # document found online
    FOIA_PENDING_REVIEW = "foia_pending_review"         # FOIA drafted and queued; NOTHING sent, waiting for a person
    FOIA_ALREADY_REQUESTED = "foia_already_requested"   # same institution+decision already queued / sent / rejected
    SCRAPE_FAILED = "scrape_failed"                     # site unreachable or draft could not be stored; retried automatically
    UNKNOWN_INSTITUTION = "unknown_institution"         # no scraper and/or no FOIA contact
    SKIPPED_DUPLICATE = "skipped_duplicate"             # same institution+decision earlier in payload
    # Recorded by the review endpoints after a person decides (the pipeline never produces these):
    FOIA_SENT = "foia_sent"                             # approved and accepted by the mail provider (Resend)
    FOIA_SEND_FAILED = "foia_send_failed"               # approved but the provider refused or failed; approve again to retry
    FOIA_REJECTED = "foia_rejected"                     # rejected in review


class DocumentActionReport(BaseModel):
    signal_index: int
    institution: str                      # canonical scraper name once resolved
    source_institution: str = ""          # as written in the gazette (DB matching key)
    decision: str
    action: Action
    url: Optional[str] = None
    evidence_location: Optional[str] = None
    foia_to: Optional[str] = None
    foia_message_id: Optional[str] = None
    pages_visited: int = 0
    pdfs_checked: int = 0
    duration_ms: int = 0
    errors: list[str] = Field(default_factory=list)


class ProcessGazetteResponse(BaseModel):
    gazette_id: str
    processed_at: datetime
    signals_total: int
    signals_processed: int
    signals_skipped_actionable: int
    references_total: int
    summary: dict[str, int]
    actions: list[DocumentActionReport]
    analysis: Optional[GazetteAnalysisResult] = Field(
        default=None, description="Tier 1 output (only populated by /upload-gazette)."
    )
    tier1: Optional[dict[str, object]] = Field(
        default=None, description="Tier 1 diagnostics: file name, size, extracted chars, timings."
    )
    persisted: bool = Field(default=False, description="True when the run was written to the database.")
    persistence_error: Optional[str] = None
    mode: Literal["new", "retry"] = Field(
        default="new",
        description="'retry' when the gazette had been processed before and only its failed references "
                    "were re-run; `actions`/`summary` then describe that retry, not the original run.",
    )


# --------------------------------------------------------------------------- #
# App & lifespan
# --------------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """DB tables, one shared aiohttp session, and the background poller."""
    if AUTH_CONFIG is None:
        # Fail closed: without credentials every request would be answered 503 anyway.
        raise RuntimeError(f"refusing to start: {AUTH_PROBLEM}. Set ADMIN_USER and ADMIN_PASSWORD.")
    await init_db()
    await backfill_foia_requests_from_history()  # requests sent before the ledger existed must not be re-sent
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_S)
    app.state.http = aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": USER_AGENT})
    if not os.environ.get("GEMINI_API_KEY"):
        logger.warning("GEMINI_API_KEY is not set — Tier 1 (upload + poller) will fail")
    if mailer.EMAIL_DRY_RUN:
        logger.info("Tier 3 e-mail: DRY RUN. Drafts are queued for review; approving one only logs it "
                    "(set LRCR_EMAIL_DRY_RUN=false to send for real)")
    else:
        logger.warning("Tier 3 e-mail: LIVE. Approved drafts WILL be sent via Resend from %s%s",
                       mailer.FROM_ADDRESS, "" if os.environ.get("RESEND_API_KEY") else " (RESEND_API_KEY is NOT set: every send will fail)")

    # The poller needs Tier 2/3; bind the shared session in so it runs the
    # exact same code path as the HTTP endpoints.
    async def _pipeline_for_poller(analysis: GazetteAnalysisResult) -> ProcessGazetteResponse:
        return await run_pipeline(analysis, app.state.http)

    app.state.pipeline_for_poller = _pipeline_for_poller
    app.state.scheduler = None
    if poller.POLL_ENABLED:
        app.state.scheduler = poller.build_scheduler(_pipeline_for_poller)
        app.state.scheduler.start()
        logger.info("poller scheduled every %.1f h (first run in %.0f s, max %d new/run, min year %d)",
                    poller.POLL_INTERVAL_HOURS, poller.POLL_STARTUP_DELAY_S,
                    poller.POLL_MAX_NEW_PER_RUN, poller.POLL_MIN_YEAR)
    else:
        logger.info("poller disabled (LRCR_POLL_ENABLED=0)")

    # Retry worker: re-runs failed scrapes from the database (it never sends e-mail).
    # The worker object always exists (manual triggers use it); only the
    # background loop is optional.
    app.state.retry_worker = RetryWorker(process_reference, app.state.http, concurrency=MAX_CONCURRENT_SCRAPES)
    if RETRY_ENABLED:
        app.state.retry_worker.start()
        logger.info("retry worker started (every %.0f min, max %d attempts per document)",
                    RETRY_INTERVAL_MINUTES, RETRY_MAX_ATTEMPTS)
    else:
        logger.info("retry worker loop disabled (LRCR_RETRY_ENABLED=0)")

    logger.info("LRCR API started")
    try:
        yield
    finally:
        await app.state.retry_worker.stop()
        if app.state.scheduler is not None:
            # wait=False: do not block shutdown on a poll that is mid-Gemini-call;
            # the job's own error handling records the interrupted attempt.
            app.state.scheduler.shutdown(wait=False)
        await app.state.http.close()
        await dispose_db()
        logger.info("LRCR API stopped")


app = FastAPI(
    title="Local Regulatory Change Radar",
    version="0.6.0",
    description="Gazette PDF -> LLM analysis -> targeted scraping -> FOIA drafts queued for human review.",
    lifespan=lifespan,
)

# HTTP Basic auth on EVERYTHING (also /docs, /openapi.json, /healthz). Added as ASGI
# middleware rather than a dependency so no route, present or future, can be missed.
AUTH_CONFIG, AUTH_PROBLEM = load_auth_config()
app.add_middleware(BasicAuthMiddleware, config=AUTH_CONFIG, problem=AUTH_PROBLEM)


# --------------------------------------------------------------------------- #
# Core per-reference logic (Tier 2 -> Tier 3)
# --------------------------------------------------------------------------- #


async def process_reference(
    signal_index: int,
    ref: MissingDocumentRef,
    session: aiohttp.ClientSession,
) -> DocumentActionReport:
    """Tier 2 then, if warranted, Tier 3 for one missing document. Never raises."""
    started = time.perf_counter()
    report = DocumentActionReport(
        signal_index=signal_index, institution=ref.institution, source_institution=ref.institution,
        decision=ref.decision_number, action=Action.SCRAPE_FAILED,
    )

    if resolve_scraper(ref.institution) is None:
        report.action = Action.UNKNOWN_INSTITUTION
        report.errors.append(f"no scraper registered for {ref.institution!r}")
        logger.error("signal %d: %s", signal_index, report.errors[-1])
        return _finish(report, started)

    # ---- Tier 2
    try:
        result: ScraperResult = await asyncio.wait_for(
            dispatch_scraper(ref.institution, ref.decision_number, session=session),
            timeout=SCRAPE_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        report.errors.append(f"scraper timed out after {SCRAPE_TIMEOUT_S}s")
        logger.error("signal %d: scraper timeout for %s %s", signal_index, ref.institution, ref.decision_number)
        return _finish(report, started)
    except Exception as exc:  # noqa: BLE001 — dispatch_scraper shouldn't raise, but never trust that
        report.errors.append(f"scraper crashed: {exc!r}")
        logger.exception("signal %d: scraper crashed", signal_index)
        return _finish(report, started)

    report.institution = result.institution or ref.institution
    report.pages_visited = result.pages_visited
    report.pdfs_checked = result.pdfs_checked
    report.errors.extend(result.errors)

    if result.found:
        report.action = Action.SCRAPED_SUCCESSFULLY
        report.url = str(result.document_url) if result.document_url else result.metadata.get("document_url_raw")  # type: ignore[assignment]
        report.evidence_location = result.evidence[0].location if result.evidence else None
        return _finish(report, started)

    if result.pages_visited == 0:
        # Nothing was fetched: the site was down or blocked. "Not found" is
        # not proven, so a FOIA request would be premature — flag for retry.
        report.action = Action.SCRAPE_FAILED
        logger.warning("signal %d: %s unreachable for %s — FOIA withheld",
                       signal_index, report.institution, ref.decision_number)
        return _finish(report, started)

    # ---- Tier 3a: draft the FOIA request
    try:
        queued = await foia_from_scraper_result(result, expected_content=ref.expected_content)
    except UnknownInstitutionError as exc:
        report.action = Action.UNKNOWN_INSTITUTION
        report.errors.append(str(exc))
        logger.error("signal %d: %s", signal_index, exc)
        return _finish(report, started)
    except Exception as exc:  # noqa: BLE001
        report.action = Action.SCRAPE_FAILED
        report.errors.append(f"FOIA generation crashed: {exc!r}")
        logger.exception("signal %d: FOIA generation crashed", signal_index)
        return _finish(report, started)

    if queued is None:  # cannot happen when found=False, but keep the contract explicit
        report.errors.append("FOIA generator returned nothing for a not-found result")
        return _finish(report, started)

    # ---- Tier 3b: queue the draft for a person. NOTHING is sent from the pipeline.
    # One draft exists per (institution, decision) (UNIQUE constraint), so the same request
    # is never queued twice however many gazettes cite it; a pair that was already sent or
    # rejected is not drafted again. If the draft cannot be stored, the reference is marked
    # retryable and the retry worker tries again later.
    draft = await create_foia_draft(
        institution=report.institution,           # canonical scraper name, identical for every spelling
        decision_number=ref.decision_number,
        to_email=queued.to_email,
        subject=queued.subject,
        body=queued.body,
        message_id=queued.message_id,
    )
    if draft.outcome is DraftOutcome.ERROR:
        report.action = Action.SCRAPE_FAILED
        report.errors.append(f"could not store the FOIA draft: {draft.detail}")
        logger.error("signal %d: %s", signal_index, report.errors[-1])
        return _finish(report, started)
    if draft.outcome is not DraftOutcome.CREATED:
        report.action = Action.FOIA_ALREADY_REQUESTED
        report.foia_to = draft.to_email
        report.foia_message_id = draft.message_id
        report.errors.append(draft.detail)
        logger.info("signal %d: %s %s: %s", signal_index, report.institution, ref.decision_number, draft.detail)
        return _finish(report, started)

    report.action = Action.FOIA_PENDING_REVIEW
    report.foia_to = queued.to_email
    report.foia_message_id = queued.message_id
    return _finish(report, started)


def _finish(report: DocumentActionReport, started: float) -> DocumentActionReport:
    report.duration_ms = int((time.perf_counter() - started) * 1000)
    logger.info("signal %d: %s %s -> %s (%d ms)", report.signal_index, report.institution,
                report.decision, report.action.value, report.duration_ms)
    return report


async def run_pipeline(payload: GazetteAnalysisResult, session: aiohttp.ClientSession) -> ProcessGazetteResponse:
    """
    Tier 2 + Tier 3 for every missing reference in an analysis result.

    Shared by both endpoints. References are processed concurrently
    (bounded), and the same institution + decision number appearing in
    several signals is only scraped once.
    """
    logger.info("pipeline %s: %d signals", payload.gazette_id, len(payload.signals))

    sem = asyncio.Semaphore(MAX_CONCURRENT_SCRAPES)
    seen: set[tuple[str, str]] = set()
    jobs: list[tuple[int, MissingDocumentRef]] = []
    duplicates: list[DocumentActionReport] = []
    skipped_actionable = 0

    for idx, signal in enumerate(payload.signals):
        if signal.is_actionable_immediately or not signal.missing_references:
            skipped_actionable += 1
            continue
        for ref in signal.missing_references:
            key = (ref.institution.casefold(), ref.decision_number.casefold())
            if key in seen:
                duplicates.append(DocumentActionReport(
                    signal_index=idx, institution=ref.institution, source_institution=ref.institution,
                    decision=ref.decision_number, action=Action.SKIPPED_DUPLICATE,
                ))
                continue
            seen.add(key)
            jobs.append((idx, ref))

    async def _bounded(idx: int, ref: MissingDocumentRef) -> DocumentActionReport:
        async with sem:
            return await process_reference(idx, ref, session)

    reports = list(await asyncio.gather(*(_bounded(i, r) for i, r in jobs)))
    reports.extend(duplicates)
    reports.sort(key=lambda r: (r.signal_index, r.decision))

    summary: dict[str, int] = {a.value: 0 for a in Action}
    for r in reports:
        summary[r.action.value] += 1

    logger.info("pipeline %s done: %s", payload.gazette_id, summary)
    return ProcessGazetteResponse(
        gazette_id=payload.gazette_id,
        processed_at=datetime.now(timezone.utc),
        signals_total=len(payload.signals),
        signals_processed=len(payload.signals) - skipped_actionable,
        signals_skipped_actionable=skipped_actionable,
        references_total=len(jobs) + len(duplicates),
        summary=summary,
        actions=reports,
    )


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #


async def retry_if_already_processed(gazette_id: str, request: Request) -> Optional[ProcessGazetteResponse]:
    """
    Cheap check, call before any real work (PDF parsing, Gemini, scraping).

    * gazette not stored yet          -> None (caller continues with a normal run)
    * stored, has failed references   -> re-run ONLY those (scrape_failed; backoff
      ignored because a person asked, attempt cap still applies) straight from the
      database, and return that outcome with mode="retry".
      No PDF processing and no Gemini call, so it costs nothing extra.
    * stored, nothing left to retry   -> 409 (complete, or out of attempts)
    """
    async with SessionLocal() as session:
        existing = await gazette_exists(session, gazette_id)
    if existing is None:
        return None

    worker: RetryWorker = request.app.state.retry_worker
    summary = await worker.run_pass(gazette_id=gazette_id, respect_backoff=False)
    if summary.retried == 0:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "gazette already processed and nothing is left to retry",
                "gazette_id": gazette_id,
                "processed_at": existing.processed_at.isoformat(),
                "retries_exhausted": summary.exhausted,
                "hint": ("GET /api/v1/gazettes/{gazette_id} to see the stored result"
                         + (f"; {summary.exhausted} document(s) already used all {RETRY_MAX_ATTEMPTS} attempts"
                            if summary.exhausted else "")),
            },
        )

    async with SessionLocal() as session:
        stored = await load_gazette(session, gazette_id)
    assert stored is not None  # it existed a moment ago; gazettes are never deleted
    reports: list[DocumentActionReport] = list(summary.reports)
    reports.sort(key=lambda r: (r.signal_index, r.decision))
    counts: dict[str, int] = {a.value: 0 for a in Action}
    for r in reports:
        counts[r.action.value] += 1
    skipped_actionable = sum(1 for s in stored.signals if s.is_actionable_immediately or not s.missing_documents)
    logger.info("re-submitted gazette %s: retried %d failed reference(s): %s", gazette_id, summary.retried, counts)
    return ProcessGazetteResponse(
        gazette_id=gazette_id,
        processed_at=datetime.now(timezone.utc),
        signals_total=len(stored.signals),
        signals_processed=len(stored.signals) - skipped_actionable,
        signals_skipped_actionable=skipped_actionable,
        references_total=sum(len(s.missing_documents) for s in stored.signals),
        summary=counts,
        actions=reports,
        persisted=summary.stored == summary.retried,
        persistence_error=None if summary.stored == summary.retried else "some retry outcomes could not be stored",
        mode="retry",
    )


async def persist_run(
    analysis: GazetteAnalysisResult,
    response: ProcessGazetteResponse,
    *,
    source: str,
    filename: Optional[str] = None,
    extracted_chars: Optional[int] = None,
) -> None:
    """
    Write the run in one transaction and mark the response accordingly.

    A persistence failure is reported in the response, not raised: the
    scraping already happened and the FOIA drafts were already printed, so
    the caller still deserves the result. A concurrent duplicate (two uploads
    of the same gazette racing past the pre-check) surfaces as IntegrityError
    on the unique gazette_id and is turned into a 409.
    """
    outcome = await persist_run_result(
        analysis, response.actions, source=source, filename=filename,
        extracted_chars=extracted_chars, summary=response.summary,
    )
    if outcome.status == "duplicate":
        raise HTTPException(status_code=409, detail={
            "error": "gazette already processed (concurrent request)",
            "gazette_id": analysis.gazette_id,
        })
    if outcome.status == "error":
        response.persistence_error = outcome.error
        return
    response.persisted = True


# --------------------------------------------------------------------------- #
# Upload helpers
# --------------------------------------------------------------------------- #

_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9Ѐ-ӿ._-]+")


def _gazette_id_from_filename(filename: Optional[str]) -> str:
    stem = Path(filename or "gazette").stem
    stem = _SAFE_ID_RE.sub("-", stem).strip("-.")
    return stem or f"gazette-{int(time.time())}"


async def _save_upload_to_temp(upload: UploadFile) -> tuple[str, int]:
    """Stream the upload to a temp file with a size cap. Returns (path, bytes)."""
    fd, path = tempfile.mkstemp(prefix="lrcr_", suffix=".pdf")
    size = 0
    try:
        with os.fdopen(fd, "wb") as fh:
            while True:
                chunk = await upload.read(UPLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413,
                                        detail=f"PDF larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB")
                if size == len(chunk) and not chunk.startswith(b"%PDF-"):
                    raise HTTPException(status_code=415, detail="file is not a PDF (missing %PDF- header)")
                fh.write(chunk)
    except Exception:
        _unlink_quietly(path)
        raise
    if size == 0:
        _unlink_quietly(path)
        raise HTTPException(status_code=422, detail="uploaded file is empty")
    return path, size


def _unlink_quietly(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #


@app.get("/healthz", tags=["ops"])
async def healthz(request: Request) -> dict[str, object]:
    scheduler = request.app.state.scheduler
    next_run = None
    if scheduler is not None:
        job = scheduler.get_job(poller.JOB_ID)
        next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    retry_worker: RetryWorker = request.app.state.retry_worker
    return {
        "status": "ok",
        "gemini_key": "set" if os.environ.get("GEMINI_API_KEY") else "missing",
        "poller": {
            "enabled": scheduler is not None,
            "next_run": next_run,
            "last_run": poller.last_summary.as_dict() if poller.last_summary else None,
        },
        "retry": {
            "enabled": retry_worker.running,
            "last_run": retry_worker.last_summary.as_dict() if retry_worker.last_summary else None,
        },
        "review": {"drafts_pending": await count_pending_drafts(), "dry_run": mailer.EMAIL_DRY_RUN},
    }


@app.post("/api/v1/poll-now", tags=["ops"])
async def poll_now(request: Request, max_new: int = poller.POLL_MAX_NEW_PER_RUN) -> dict[str, object]:
    """Run one polling pass immediately (same code the scheduler runs)."""
    summary = await poller.poll_bitola_gazettes(request.app.state.pipeline_for_poller, max_new=max_new)
    return summary.as_dict()


# --------------------------------------------------------------------------- #
# Human review of drafted FOIA requests (the ONLY place e-mail is ever sent)
# --------------------------------------------------------------------------- #


class ReviewDecision(BaseModel):
    """Optional body of approve / reject. The reviewer is always the authenticated user."""

    note: Optional[str] = Field(default=None, max_length=1000, description="Kept in the audit trail.")


def _reviewer(request: Request) -> str:
    return str(request.scope.get("auth_user") or "unknown")


def _draft_json(d: DraftView) -> dict[str, object]:
    return {
        "id": d.id, "status": d.status, "institution": d.institution, "decision_number": d.decision_number,
        "to": d.to_email, "subject": d.subject, "body": d.body, "message_id": d.message_id,
        "attempts": d.attempts, "last_error": d.last_error, "reviewed_by": d.reviewed_by,
        "review_note": d.review_note, "created_at": d.created_at.isoformat(),
        "sent_at": d.sent_at.isoformat() if d.sent_at else None,
    }


@app.get("/review", tags=["review"], response_model=None)
async def review_queue(request: Request, format: Optional[str] = None) -> HTMLResponse | JSONResponse:
    """
    Drafts a person can still act on: `pending_review`, plus `failed` ones whose send
    did not go through. HTML for browsers; JSON for API clients (send `Accept: application/json`
    or use `?format=json`).
    """
    items = await list_review_items()
    wants_html = format != "json" and "text/html" in request.headers.get("accept", "")
    if not wants_html:
        return JSONResponse(
            {
                "count": len(items),
                "dry_run": mailer.EMAIL_DRY_RUN,
                "drafts": [
                    {**_draft_json(i.draft), "source": {
                        "gazette_id": i.gazette_id, "signal_title": i.signal_title,
                        "expected_content": i.expected_content,
                    }}
                    for i in items
                ],
            },
            headers={"Cache-Control": "no-store"},
        )
    nonce = secrets.token_urlsafe(16)
    page = render_review_page(items, dry_run=mailer.EMAIL_DRY_RUN, user=_reviewer(request), nonce=nonce)
    return HTMLResponse(page, headers={
        "Cache-Control": "no-store",
        "Content-Security-Policy": (f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
                                    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"),
        "X-Content-Type-Options": "nosniff",
    })


@app.post("/review/{draft_id}/approve", tags=["review"])
async def approve_draft(
    draft_id: int, request: Request, decision: Optional[ReviewDecision] = Body(default=None)
) -> JSONResponse:
    """
    Send the draft NOW through mailer.py and mark it `sent`.

    Safe against double clicks: the draft is moved pending_review -> sending in one
    conditional UPDATE before anything is sent, so only one caller can win.
    If the send fails the draft becomes `failed` and stays in /review so it can be
    approved again (502). With LRCR_EMAIL_DRY_RUN=true nothing is sent and the draft
    stays queued (200, `dry_run: true`).
    """
    reviewer, note = _reviewer(request), (decision.note if decision else None)

    existing = await get_draft(draft_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"draft {draft_id} not found")

    if mailer.EMAIL_DRY_RUN:
        if existing.status not in ("pending_review", "failed"):
            raise HTTPException(status_code=409, detail=f"draft {draft_id} is {existing.status}, not awaiting review")
        # The mailer logs the message in dry-run mode. The draft is left untouched so it is not lost.
        await mailer.dispatch_foia_email(existing.to_email or "", existing.subject or "", existing.body or "",
                                         existing.message_id or "")
        logger.info("approve draft %d by %s: DRY RUN, nothing sent", draft_id, reviewer)
        return JSONResponse({"draft_id": draft_id, "status": existing.status, "dry_run": True, "sent": False,
                             "detail": "LRCR_EMAIL_DRY_RUN is true: the message was only logged; the draft stays in the queue"})

    ticket = await begin_send(draft_id, reviewer=reviewer, note=note)
    if ticket.outcome is not SendOutcome.STARTED or ticket.draft is None:
        code = {SendOutcome.NOT_FOUND: 404, SendOutcome.NO_DRAFT: 422, SendOutcome.ERROR: 500}.get(ticket.outcome, 409)
        raise HTTPException(status_code=code, detail=ticket.detail or ticket.outcome.value)
    draft = ticket.draft

    logger.warning("approve draft %d by %s: sending %s / %s to %s", draft_id, reviewer, draft.institution,
                   draft.decision_number, draft.to_email)
    try:
        mail_result = await mailer.dispatch_foia_email(
            draft.to_email or "", draft.subject or "", draft.body or "", draft.message_id or ""
        )
    except Exception as exc:  # noqa: BLE001 — dispatch_foia_email should not raise, but never trust that
        logger.exception("draft %d: mailer crashed", draft_id)
        await finish_send(draft_id, sent=False, error=f"mailer crashed: {exc!r}")
        await record_review_outcome(draft.message_id or "", Action.FOIA_SEND_FAILED.value, to_email=draft.to_email,
                                    error=f"mailer crashed: {exc!r}")
        raise HTTPException(status_code=502, detail=f"sending failed: mailer crashed: {exc!r}") from exc

    if mail_result.dry_run:   # configuration changed between the check above and the send
        await finish_send(draft_id, sent=False, released=True)
        return JSONResponse({"draft_id": draft_id, "status": "pending_review", "dry_run": True, "sent": False,
                             "detail": "the mailer is in dry-run mode: the message was only logged"})
    if not mail_result.sent:
        error = f"send failed: {mail_result.error}"
        await finish_send(draft_id, sent=False, error=error)
        await record_review_outcome(draft.message_id or "", Action.FOIA_SEND_FAILED.value, to_email=draft.to_email, error=error)
        raise HTTPException(status_code=502, detail=f"{error}. The draft stays in /review; approve it again to retry.")

    await finish_send(draft_id, sent=True)
    await record_review_outcome(draft.message_id or "", Action.FOIA_SENT.value, to_email=draft.to_email)
    return JSONResponse({"draft_id": draft_id, "status": "sent", "dry_run": False, "sent": True,
                         "to": draft.to_email, "message_id": draft.message_id, "reviewed_by": reviewer})


@app.post("/review/{draft_id}/reject", tags=["review"])
async def reject_draft_endpoint(
    draft_id: int, request: Request, decision: Optional[ReviewDecision] = Body(default=None)
) -> JSONResponse:
    """Mark a pending (or failed) draft `rejected`. It is never sent and never drafted again."""
    result = await reject_draft(draft_id, reviewer=_reviewer(request), note=decision.note if decision else None)
    if result.outcome is RejectOutcome.NOT_FOUND:
        raise HTTPException(status_code=404, detail=result.detail)
    if result.outcome is RejectOutcome.CONFLICT:
        raise HTTPException(status_code=409, detail=result.detail)
    if result.outcome is RejectOutcome.ERROR or result.draft is None:
        raise HTTPException(status_code=500, detail=result.detail or "could not reject the draft")
    await record_review_outcome(result.draft.message_id or "", Action.FOIA_REJECTED.value, to_email=result.draft.to_email)
    logger.info("draft %d rejected by %s", draft_id, _reviewer(request))
    return JSONResponse({"draft_id": draft_id, "status": "rejected", "reviewed_by": _reviewer(request)})


@app.post("/api/v1/retry-now", tags=["ops"])
async def retry_now(request: Request, ignore_backoff: bool = False) -> dict[str, object]:
    """
    Run one retry pass immediately (same code the background loop runs): re-run
    failed scrapes that are due. `ignore_backoff=true` retries
    everything that still has attempts left, due or not.
    """
    worker: RetryWorker = request.app.state.retry_worker
    summary = await worker.run_pass(respect_backoff=not ignore_backoff)
    return summary.as_dict()


@app.post("/api/v1/process-gazette", response_model=ProcessGazetteResponse, tags=["pipeline"])
async def process_gazette(payload: GazetteAnalysisResult, request: Request) -> ProcessGazetteResponse:
    """
    Run Tier 2 + Tier 3 for a gazette analysis produced elsewhere, then persist.
    A gazette that was stored before has its failed references retried instead
    (mode="retry"); 409 only when nothing is left to retry.
    """
    if not payload.signals:
        raise HTTPException(status_code=422, detail="payload contains no signals")
    retried = await retry_if_already_processed(payload.gazette_id, request)
    if retried is not None:
        return retried
    response = await run_pipeline(payload, request.app.state.http)
    await persist_run(payload, response, source="api")
    return response


@app.post("/api/v1/upload-gazette", response_model=ProcessGazetteResponse, tags=["pipeline"])
async def upload_gazette(
    request: Request,
    file: UploadFile = File(..., description="Gazette PDF (Службен гласник на Општина Битола)"),
    gazette_id: Optional[str] = Form(default=None, description="Defaults to the file name without extension"),
) -> ProcessGazetteResponse:
    """
    Full autonomous run: PDF -> text (pypdf) -> Gemini analysis -> Tier 2 -> Tier 3.

    If this gazette_id was processed before, the PDF is ignored and only its
    failed references are retried (response `mode` = "retry"); 409 when nothing
    is left to retry.

    The Tier 1 result is returned in `analysis` so you can audit what the
    model extracted before trusting the downstream actions.
    """
    started = time.perf_counter()
    gid = (gazette_id or "").strip() or _gazette_id_from_filename(file.filename)
    logger.info("upload-gazette %s: file=%r content-type=%s", gid, file.filename, file.content_type)

    # Before spending money on Gemini: a stored gazette is never re-analysed.
    # Its failed references are retried from the database, or 409 if none are left.
    retried = await retry_if_already_processed(gid, request)
    if retried is not None:
        await file.close()
        return retried
    tmp_path, size = await _save_upload_to_temp(file)
    try:
        # ---- Tier 1a: text extraction (CPU-bound -> worker thread)
        t0 = time.perf_counter()
        try:
            text = await asyncio.to_thread(extract_text_from_pdf, tmp_path)
        except GazetteTextEmpty as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except PdfReadError as exc:
            raise HTTPException(status_code=415, detail=f"unreadable PDF: {exc}") from exc
        extract_ms = int((time.perf_counter() - t0) * 1000)

        # ---- Tier 1b: LLM analysis
        t0 = time.perf_counter()
        try:
            analysis = await analyze_gazette_with_llm(gid, text)
        except LLMAnalysisError as exc:
            logger.error("upload-gazette %s: Tier 1 failed: %s", gid, exc)
            raise HTTPException(status_code=502, detail=f"LLM analysis failed: {exc}") from exc
        llm_ms = int((time.perf_counter() - t0) * 1000)
    finally:
        _unlink_quietly(tmp_path)
        await file.close()

    # ---- Tier 2 + 3
    response = await run_pipeline(analysis, request.app.state.http)
    response.analysis = analysis
    response.tier1 = {
        "filename": file.filename,
        "bytes": size,
        "extracted_chars": len(text),
        "extract_ms": extract_ms,
        "llm_ms": llm_ms,
        "total_ms": int((time.perf_counter() - started) * 1000),
    }
    await persist_run(analysis, response, source="upload", filename=file.filename, extracted_chars=len(text))
    return response


@app.get("/api/v1/gazettes/{gazette_id}", tags=["pipeline"])
async def get_gazette(gazette_id: str) -> dict[str, object]:
    """Stored result for one gazette: signals -> missing documents -> actions."""
    async with SessionLocal() as session:
        g = await load_gazette(session, gazette_id)
    if g is None:
        raise HTTPException(status_code=404, detail=f"gazette {gazette_id!r} not found")
    return {
        "gazette_id": g.gazette_id,
        "filename": g.filename,
        "source": g.source,
        "published_on": g.published_on.isoformat() if g.published_on else g.published_on_raw,
        "processed_at": g.processed_at.isoformat(),
        "summary": g.summary,
        "signals": [
            {
                "title": s.title,
                "summary": s.summary,
                "is_actionable_immediately": s.is_actionable_immediately,
                "missing_documents": [
                    {
                        "institution": d.institution,
                        "decision_number": d.decision_number,
                        "expected_content": d.expected_content,
                        "actions": [
                            {
                                "status": a.action_status,
                                "document_url": a.document_url,
                                "foia_to": a.foia_to,
                                "foia_message_id": a.foia_message_id,
                                "created_at": a.created_at.isoformat(),
                                "errors": a.errors,
                            }
                            for a in d.actions
                        ],
                    }
                    for d in s.missing_documents
                ],
            }
            for s in g.signals
        ],
    }


# --------------------------------------------------------------------------- #
# Dev entry point
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
