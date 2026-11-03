"""
Persistence helpers: map pipeline results onto the ORM and back.

    gazette_exists(session, gazette_id)          -> Optional[Gazette]
    save_pipeline_run(session, analysis, reports) -> Gazette   (one transaction)
    load_gazette(session, gazette_id)             -> Optional[Gazette] with children

Human-in-the-loop FOIA queue (the pipeline NEVER sends e-mail; only approval does):

    create_foia_draft(...)      -> DraftResult     store a drafted e-mail as pending_review (deduplicated)
    list_review_items()         -> [ReviewItem]    drafts a person can still act on, with their source
    begin_send(...)             -> SendTicket      atomic pending_review/failed -> sending, records who approved
    finish_send(...)                               sending -> sent / failed / back to pending_review
    reject_draft(...)           -> RejectResult    pending_review/failed -> rejected (terminal)
    record_review_outcome(...)                     append foia_sent / foia_send_failed / foia_rejected to the
                                                   documents the draft came from and refresh their summaries
    backfill_foia_requests_from_history()          seed the ledger from old foia_sent rows

Retry support (failed scrapes are re-run automatically):

    find_retry_candidates(...)   -> RetryScan      which documents are due for another attempt
    append_action_for_document() -> bool           store the new outcome, refresh gazette.summary

Keeping this out of main.py means the API layer never touches SQLAlchemy
directly, and the same functions can be reused by a CLI or a batch job.

Backend-neutral: everything here runs unchanged on PostgreSQL (asyncpg) and SQLite.
Which one is used is decided in database.py from DATABASE_URL. The only
Postgres-specific concern that lives here is input hygiene (_fields): VARCHAR
lengths and NUL bytes, which SQLite tolerates and PostgreSQL does not.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Literal, Optional, Sequence

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import SessionLocal
from models import FoiaAction, FoiaRequest, Gazette, MissingDocument, PollLog, Signal
from schemas import GazetteAnalysisResult

logger = logging.getLogger("lrcr.persistence")

# Action statuses that mean "nothing was proven, try again". Keep in sync with main.Action.
# Only scrape_failed is retried automatically. FOIA sending is never automatic (a person
# approves it), so a failed send is retried by approving the draft again, not by the worker.
# Not retryable: unknown_institution (needs a code/config change), foia_pending_review
# (waiting for a person), foia_already_requested, skipped_duplicate.
RETRYABLE_STATUSES: frozenset[str] = frozenset({"scrape_failed"})

# A FoiaRequest stuck in "sending" for longer than this is assumed to belong to a
# crashed process (mail API calls time out long before this). It is reported, never re-sent.
CLAIM_STALE_AFTER = timedelta(minutes=10)


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    """
    SQLite drops tzinfo on DateTime(timezone=True): values come back naive but
    were written as UTC. Postgres returns aware values. Normalise both.
    """
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# Input hygiene for PostgreSQL
# --------------------------------------------------------------------------- #


def _clean(value: Any) -> Any:
    """Strip NUL bytes (PostgreSQL text/JSON columns reject them; SQLite does not) recursively."""
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_clean(v) for v in value]
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    return value


def _fields(model: Any, **fields: Any) -> dict[str, Any]:
    """
    Column values that are safe to insert on any backend.

    SQLite ignores VARCHAR(n) lengths and accepts NUL bytes; PostgreSQL enforces
    the first and rejects the second, and a single bad value would roll back the
    whole gazette. So strings are cut to the column's length (with a warning) and
    NULs removed. Unknown column names raise KeyError immediately (a typo, not data).
    """
    columns = model.__table__.c
    out: dict[str, Any] = {}
    for name, value in fields.items():
        value = _clean(value)
        length = getattr(columns[name].type, "length", None)
        if isinstance(value, str) and length and len(value) > length:
            logger.warning("%s.%s: value of %d chars cut to %d", model.__tablename__, name, len(value), length)
            value = value[:length]
        out[name] = value
    return out


# --------------------------------------------------------------------------- #
# Key normalisation (dedupe identity)
# --------------------------------------------------------------------------- #

_DASH_VARIANTS = str.maketrans({
    "‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-", "―": "-", "−": "-",
})
_WS_RE = re.compile(r"\s+")


def normalize_institution(value: str) -> str:
    """Case-folded, whitespace-collapsed institution name (use the canonical scraper name)."""
    return _WS_RE.sub(" ", value).strip().casefold()[:255]


def normalize_decision(value: str) -> str:
    """Decision number with all whitespace removed, dash variants unified, case-folded."""
    return _WS_RE.sub("", value.translate(_DASH_VARIANTS)).casefold()[:64]


# --------------------------------------------------------------------------- #
# Gazette read/write
# --------------------------------------------------------------------------- #


async def gazette_exists(session: AsyncSession, gazette_id: str) -> Optional[Gazette]:
    """Return the stored gazette row for this id, or None."""
    return await session.scalar(select(Gazette).where(Gazette.gazette_id == gazette_id))


async def load_gazette(session: AsyncSession, gazette_id: str) -> Optional[Gazette]:
    """Gazette with signals, missing documents and actions eagerly loaded."""
    stmt = (
        select(Gazette)
        .where(Gazette.gazette_id == gazette_id)
        .options(
            selectinload(Gazette.signals)
            .selectinload(Signal.missing_documents)
            .selectinload(MissingDocument.actions)
        )
    )
    return await session.scalar(stmt)


def action_from_report(r: object) -> FoiaAction:
    """Map a main.DocumentActionReport (duck-typed) onto a FoiaAction row."""
    return FoiaAction(**_fields(
        FoiaAction,
        action_status=str(getattr(getattr(r, "action"), "value", getattr(r, "action"))),
        institution_resolved=getattr(r, "institution", None),
        document_url=getattr(r, "url", None),
        evidence_location=getattr(r, "evidence_location", None),
        foia_to=getattr(r, "foia_to", None),
        foia_message_id=getattr(r, "foia_message_id", None),
        pages_visited=int(getattr(r, "pages_visited", 0)),
        pdfs_checked=int(getattr(r, "pdfs_checked", 0)),
        duration_ms=int(getattr(r, "duration_ms", 0)),
        errors=list(getattr(r, "errors", []) or []) or None,
    ))


async def save_pipeline_run(
    session: AsyncSession,
    analysis: GazetteAnalysisResult,
    reports: Sequence[object],
    *,
    source: str = "api",
    filename: Optional[str] = None,
    extracted_chars: Optional[int] = None,
    summary: Optional[dict[str, int]] = None,
) -> Gazette:
    """
    Persist one full run: gazette -> signals -> missing documents -> actions.

    `reports` are main.DocumentActionReport objects (duck-typed to avoid a
    circular import). Each is matched to its MissingDocument by
    (signal_index, institution as written, decision number). Does NOT commit —
    the caller owns the transaction so the duplicate check and the insert
    share it.
    """
    gazette = Gazette(**_fields(
        Gazette,
        gazette_id=analysis.gazette_id,
        filename=filename,
        source=source,
        published_on=_parse_iso_date(analysis.published_on),
        published_on_raw=analysis.published_on,
        extracted_chars=extracted_chars,
        summary=summary,
    ))
    # A relationship collection is only marked "loaded" on a transient object the moment
    # something in this process actually touches it (e.g. .append()). A gazette with zero
    # signals — a purely administrative issue, perfectly normal — never touches
    # gazette.signals in the loop below, so it stays unloaded even after session.add() +
    # flush() makes the row persistent. The len() call further down would then be the FIRST
    # touch of that attribute, and SQLAlchemy would try to satisfy it with an implicit lazy
    # SELECT — which AsyncSession cannot run outside an explicit `await` and raises
    # sqlalchemy.exc.MissingGreenlet. Setting every collection here, before anything is
    # appended, marks it loaded from the start regardless of how many items follow.
    gazette.signals = []

    # Index reports by the reference they came from.
    by_ref: dict[tuple[int, str, str], list[object]] = {}
    for r in reports:
        key = (
            int(getattr(r, "signal_index")),
            str(getattr(r, "source_institution", getattr(r, "institution"))).casefold(),
            str(getattr(r, "decision")).casefold(),
        )
        by_ref.setdefault(key, []).append(r)

    matched = 0
    for idx, sig in enumerate(analysis.signals):
        signal = Signal(**_fields(
            Signal,
            position=idx,
            title=sig.title,
            summary=sig.summary,
            is_actionable_immediately=sig.is_actionable_immediately,
        ))
        signal.missing_documents = []   # same reasoning as gazette.signals above
        gazette.signals.append(signal)
        for ref in sig.missing_references:
            doc = MissingDocument(**_fields(
                MissingDocument,
                institution=ref.institution,
                decision_number=ref.decision_number,
                expected_content=ref.expected_content,
            ))
            doc.actions = []   # same reasoning again: a matched-nothing reference has none
            signal.missing_documents.append(doc)
            key = (idx, ref.institution.casefold(), ref.decision_number.casefold())
            for r in by_ref.pop(key, []):
                matched += 1
                doc.actions.append(action_from_report(r))

    if by_ref:
        # Should not happen; means a report did not correspond to any reference.
        logger.error("%d action report(s) had no matching reference: %s",
                     sum(len(v) for v in by_ref.values()), list(by_ref))

    session.add(gazette)
    await session.flush()  # assigns ids, raises IntegrityError on a duplicate gazette_id
    logger.info("persisted gazette %s: %d signals, %d actions", analysis.gazette_id,
                len(gazette.signals), matched)
    return gazette


# --------------------------------------------------------------------------- #
# Session-owning helpers (safe to call from the poller or the API)
# --------------------------------------------------------------------------- #


async def is_gazette_processed(gazette_id: str) -> bool:
    """True when a run for this gazette_id is stored. Never raises HTTP errors."""
    async with SessionLocal() as session:
        return await gazette_exists(session, gazette_id) is not None


@dataclass(frozen=True)
class PersistOutcome:
    status: Literal["persisted", "duplicate", "error"]
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.status == "persisted"


async def persist_run_result(
    analysis: GazetteAnalysisResult,
    reports: Sequence[object],
    *,
    source: str,
    filename: Optional[str] = None,
    extracted_chars: Optional[int] = None,
    summary: Optional[dict[str, int]] = None,
) -> PersistOutcome:
    """
    Persist one run in its own session/transaction and report what happened.

    Never raises: a duplicate gazette_id (unique constraint) comes back as
    "duplicate", anything else as "error" with the message. The API layer
    turns these into HTTP codes; the poller just logs them.
    """
    async with SessionLocal() as session:
        try:
            await save_pipeline_run(
                session, analysis, reports,
                source=source, filename=filename, extracted_chars=extracted_chars, summary=summary,
            )
            await session.commit()
            return PersistOutcome("persisted")
        except IntegrityError as exc:
            await session.rollback()
            logger.warning("gazette %s already stored (unique constraint): %s",
                           analysis.gazette_id, str(exc.orig) if exc.orig else exc)
            return PersistOutcome("duplicate", str(exc.orig) if exc.orig else str(exc))
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            logger.exception("persisting gazette %s failed", analysis.gazette_id)
            return PersistOutcome("error", f"{type(exc).__name__}: {exc}")


async def record_poll_attempt(gazette_id: str, source_url: str, status: str, error: Optional[str] = None) -> None:
    """Append a poll_log row. Failures here are logged, never raised."""
    try:
        async with SessionLocal() as session:
            session.add(PollLog(gazette_id=gazette_id, source_url=source_url, status=status,
                                error=(error or None) and error[:2000]))
            await session.commit()
    except Exception:  # noqa: BLE001
        logger.exception("could not write poll_log for %s", gazette_id)


async def poll_failure_count(gazette_id: str) -> int:
    """How many times the poller has already failed on this gazette."""
    async with SessionLocal() as session:
        n = await session.scalar(
            select(func.count()).select_from(PollLog)
            .where(PollLog.gazette_id == gazette_id, PollLog.status == "failed")
        )
        return int(n or 0)


# --------------------------------------------------------------------------- #
# FOIA drafts: review queue + send ledger (nothing is e-mailed without a human)
# --------------------------------------------------------------------------- #

STATUS_PENDING = "pending_review"
STATUS_SENDING = "sending"
STATUS_SENT = "sent"
STATUS_FAILED = "failed"
STATUS_REJECTED = "rejected"
REVIEWABLE_STATUSES: tuple[str, ...] = (STATUS_PENDING, STATUS_FAILED)   # what a person may still approve / reject

# FoiaAction statuses that a review decision supersedes (see record_review_outcome).
_DRAFT_ACTION_STATUSES: tuple[str, ...] = ("foia_pending_review", "foia_send_failed")


@dataclass(frozen=True)
class DraftView:
    """Detached, read-only copy of a foia_requests row (safe to use after the session closed)."""

    id: int
    institution: str
    decision_number: str
    status: str
    to_email: Optional[str]
    subject: Optional[str]
    body: Optional[str]
    message_id: Optional[str]
    attempts: int
    last_error: Optional[str]
    reviewed_by: Optional[str]
    review_note: Optional[str]
    created_at: datetime
    sent_at: Optional[datetime]


def _view(row: FoiaRequest) -> DraftView:
    return DraftView(
        id=row.id, institution=row.institution, decision_number=row.decision_number, status=row.status,
        to_email=row.to_email, subject=row.subject, body=row.body, message_id=row.message_id,
        attempts=row.attempts, last_error=row.last_error, reviewed_by=row.reviewed_by,
        review_note=row.review_note, created_at=row.created_at, sent_at=row.sent_at,
    )


class DraftOutcome(str, Enum):
    CREATED = "created"                # new draft stored, waiting for a person
    ALREADY_PENDING = "already_pending"  # a draft for this pair is already waiting
    ALREADY_SENT = "already_sent"      # a request for this pair was delivered earlier
    REJECTED = "rejected"              # a person rejected this pair; it is not drafted again
    IN_PROGRESS = "in_progress"        # an approval is being sent right now
    INTERRUPTED = "interrupted"        # an earlier send died mid-flight; outcome unknown
    NEEDS_REAPPROVAL = "needs_reapproval"  # an approved send failed; waiting for a person to retry it
    ERROR = "error"                    # database problem: nothing stored


@dataclass(frozen=True)
class DraftResult:
    outcome: DraftOutcome
    draft_id: Optional[int] = None
    to_email: Optional[str] = None
    message_id: Optional[str] = None
    detail: str = ""


async def create_foia_draft(
    *,
    institution: str,
    decision_number: str,
    to_email: str,
    subject: str,
    body: str,
    message_id: str,
) -> DraftResult:
    """
    Store a drafted FOIA e-mail for review. Never sends anything, never raises.

    At most one row exists per (institution, decision): the first INSERT wins (UNIQUE
    constraint), so several gazettes citing the same decision, or the poller racing a
    manual upload, produce ONE draft. If the pair is already known the existing row is
    reported instead (pending, sent, rejected, ...) and nothing is written.
    """
    ikey = normalize_institution(institution)
    dkey = normalize_decision(decision_number)
    try:
        for _ in range(4):  # a few re-reads cover lost races; contention is otherwise nil
            async with SessionLocal() as session:
                row = await session.scalar(
                    select(FoiaRequest).where(FoiaRequest.institution_key == ikey, FoiaRequest.decision_key == dkey)
                )

                if row is None:
                    new = FoiaRequest(**_fields(
                        FoiaRequest,
                        institution_key=ikey, decision_key=dkey,
                        institution=institution, decision_number=decision_number,
                        status=STATUS_PENDING, to_email=to_email, subject=subject, body=body,
                        message_id=message_id, attempts=0,
                    ))
                    session.add(new)
                    try:
                        await session.commit()
                    except IntegrityError:
                        await session.rollback()
                        continue  # someone inserted first: re-read their row
                    return DraftResult(DraftOutcome.CREATED, new.id, to_email, message_id)

                known = dict(draft_id=row.id, to_email=row.to_email, message_id=row.message_id)

                if row.status == STATUS_PENDING:
                    return DraftResult(DraftOutcome.ALREADY_PENDING, **known,
                                       detail=f"dedupe: draft #{row.id} for this decision is already awaiting review")
                if row.status == STATUS_SENT:
                    when = f" on {_as_utc(row.sent_at):%Y-%m-%d}" if row.sent_at else ""
                    return DraftResult(DraftOutcome.ALREADY_SENT, **known,
                                       detail=f"dedupe: FOIA request for this decision was already sent{when} to {row.to_email}")
                if row.status == STATUS_REJECTED:
                    return DraftResult(DraftOutcome.REJECTED, **known,
                                       detail=f"dedupe: draft #{row.id} for this decision was rejected in review; not drafting it again")
                if row.status == STATUS_SENDING:
                    age = _utcnow() - _as_utc(row.updated_at)
                    if age < CLAIM_STALE_AFTER:
                        return DraftResult(DraftOutcome.IN_PROGRESS, **known,
                                           detail=f"dedupe: draft #{row.id} is being sent right now")
                    return DraftResult(DraftOutcome.INTERRUPTED, **known,
                                       detail=f"dedupe: sending draft #{row.id} was interrupted; outcome unknown (check the Sent folder)")
                if row.status == STATUS_FAILED:
                    if row.body:
                        return DraftResult(DraftOutcome.NEEDS_REAPPROVAL, **known,
                                           detail=f"dedupe: draft #{row.id} was approved but the send failed; approve it again in /review")
                    # A failed row with no stored draft (left by an earlier version that sent automatically):
                    # nothing is waiting for a person, so turn it into a real draft.
                    res = await session.execute(
                        update(FoiaRequest)
                        .where(FoiaRequest.id == row.id, FoiaRequest.status == STATUS_FAILED, FoiaRequest.body.is_(None))
                        .values(**_fields(FoiaRequest, status=STATUS_PENDING, to_email=to_email, subject=subject,
                                          body=body, message_id=message_id, last_error=None),
                                updated_at=_utcnow())
                    )
                    await session.commit()
                    if res.rowcount == 1:  # type: ignore[attr-defined]
                        return DraftResult(DraftOutcome.CREATED, row.id, to_email, message_id)
                    continue
                return DraftResult(DraftOutcome.ERROR, detail=f"foia_requests row {row.id} has unknown status {row.status!r}")

        return DraftResult(DraftOutcome.ERROR, detail="could not settle the foia_requests draft after several attempts")
    except Exception as exc:  # noqa: BLE001
        logger.exception("create_foia_draft failed for %s / %s", institution, decision_number)
        return DraftResult(DraftOutcome.ERROR, detail=f"{type(exc).__name__}: {exc}")


@dataclass(frozen=True)
class ReviewItem:
    draft: DraftView
    gazette_id: Optional[str]          # where the reference was found, so the reviewer can check the source
    signal_title: Optional[str]
    expected_content: Optional[str]


async def list_review_items() -> list[ReviewItem]:
    """Drafts a person can still act on (pending_review and failed), oldest first, with their source."""
    async with SessionLocal() as session:
        rows = (await session.scalars(
            select(FoiaRequest).where(FoiaRequest.status.in_(REVIEWABLE_STATUSES)).order_by(FoiaRequest.created_at, FoiaRequest.id)
        )).all()
        ids = [r.message_id for r in rows if r.message_id]
        source: dict[str, FoiaAction] = {}
        if ids:
            actions = (await session.scalars(
                select(FoiaAction)
                .where(FoiaAction.foia_message_id.in_(ids))
                .options(selectinload(FoiaAction.missing_document).selectinload(MissingDocument.signal).selectinload(Signal.gazette))
                .order_by(FoiaAction.created_at, FoiaAction.id)
            )).all()
            for a in actions:
                source.setdefault(a.foia_message_id or "", a)   # earliest action = the run that created the draft
        items: list[ReviewItem] = []
        for r in rows:
            a = source.get(r.message_id or "")
            doc = a.missing_document if a else None
            items.append(ReviewItem(
                draft=_view(r),
                gazette_id=doc.signal.gazette.gazette_id if doc else None,
                signal_title=doc.signal.title if doc else None,
                expected_content=doc.expected_content if doc else None,
            ))
        return items


async def get_draft(draft_id: int) -> Optional[DraftView]:
    async with SessionLocal() as session:
        row = await session.get(FoiaRequest, draft_id)
        return _view(row) if row else None


async def count_pending_drafts() -> int:
    async with SessionLocal() as session:
        n = await session.scalar(select(func.count()).select_from(FoiaRequest).where(FoiaRequest.status == STATUS_PENDING))
        return int(n or 0)


class SendOutcome(str, Enum):
    STARTED = "started"            # this caller now owns the send: go ahead
    NOT_FOUND = "not_found"
    ALREADY_SENT = "already_sent"
    IN_PROGRESS = "in_progress"
    INTERRUPTED = "interrupted"
    REJECTED = "rejected"
    NO_DRAFT = "no_draft"          # row exists but has no stored recipient/subject/body
    ERROR = "error"


@dataclass(frozen=True)
class SendTicket:
    outcome: SendOutcome
    draft: Optional[DraftView] = None
    detail: str = ""


async def begin_send(draft_id: int, *, reviewer: str, note: Optional[str]) -> SendTicket:
    """
    Atomically move a draft from pending_review / failed to sending and record who approved it.

    A single conditional UPDATE decides the winner, so two simultaneous approvals
    (double click, two tabs, two workers) can never both get SendOutcome.STARTED.
    Never raises.
    """
    try:
        async with SessionLocal() as session:
            res = await session.execute(
                update(FoiaRequest)
                .where(
                    FoiaRequest.id == draft_id, FoiaRequest.status.in_(REVIEWABLE_STATUSES),
                    FoiaRequest.to_email.is_not(None), FoiaRequest.subject.is_not(None), FoiaRequest.body.is_not(None),
                )
                .values(status=STATUS_SENDING, attempts=FoiaRequest.attempts + 1, last_error=None,
                        **_fields(FoiaRequest, reviewed_by=reviewer, review_note=note),
                        reviewed_at=_utcnow(), updated_at=_utcnow())
            )
            await session.commit()
            row = await session.get(FoiaRequest, draft_id, populate_existing=True)
            if row is None:
                return SendTicket(SendOutcome.NOT_FOUND, detail=f"draft {draft_id} does not exist")
            if res.rowcount == 1:  # type: ignore[attr-defined]
                return SendTicket(SendOutcome.STARTED, _view(row))
            view = _view(row)
            if row.status == STATUS_SENT:
                return SendTicket(SendOutcome.ALREADY_SENT, view, "this draft was already sent")
            if row.status == STATUS_REJECTED:
                return SendTicket(SendOutcome.REJECTED, view, "this draft was rejected")
            if row.status == STATUS_SENDING:
                if _utcnow() - _as_utc(row.updated_at) < CLAIM_STALE_AFTER:
                    return SendTicket(SendOutcome.IN_PROGRESS, view, "this draft is being sent right now")
                logger.error("foia_requests id=%d has been 'sending' since %s: a send was interrupted. NOT re-sending "
                             "automatically. Check the mailbox's Sent folder, then set its status to 'sent' or "
                             "'failed' by hand.", row.id, row.updated_at)
                return SendTicket(SendOutcome.INTERRUPTED, view,
                                  "an earlier send of this draft was interrupted and its outcome is unknown; "
                                  "check the Sent folder, then fix the row's status by hand")
            if row.status in REVIEWABLE_STATUSES:
                return SendTicket(SendOutcome.NO_DRAFT, view, "this row has no stored recipient/subject/body to send")
            return SendTicket(SendOutcome.ERROR, view, f"unexpected status {row.status!r}")
    except Exception as exc:  # noqa: BLE001
        logger.exception("begin_send failed for draft %s", draft_id)
        return SendTicket(SendOutcome.ERROR, detail=f"{type(exc).__name__}: {exc}")


async def finish_send(draft_id: int, *, sent: bool, error: Optional[str] = None, released: bool = False) -> None:
    """
    Close out a STARTED send. Never raises.

    sent=True     -> "sent" (permanent)
    sent=False    -> "failed" (stays in the review list; a person may approve it again)
    released=True -> nothing was actually sent (the mailer only logged it): back to pending_review
    """
    try:
        async with SessionLocal() as session:
            if released:
                values: dict[str, Any] = dict(status=STATUS_PENDING, attempts=FoiaRequest.attempts - 1, updated_at=_utcnow())
            elif sent:
                values = dict(status=STATUS_SENT, sent_at=_utcnow(), updated_at=_utcnow(), last_error=None)
            else:
                values = dict(status=STATUS_FAILED, updated_at=_utcnow(),
                              last_error=_clean((error or "unknown error"))[:2000])
            await session.execute(
                update(FoiaRequest).where(FoiaRequest.id == draft_id, FoiaRequest.status == STATUS_SENDING).values(**values)
            )
            await session.commit()
    except Exception:  # noqa: BLE001
        if sent:
            logger.critical(
                "foia_requests id=%s: the e-mail WAS sent but the ledger could not be updated. The row stays "
                "'sending' and will not be re-sent automatically (the safe side); fix its status by hand.",
                draft_id, exc_info=True,
            )
        else:
            logger.exception("could not finalize foia_requests id=%s", draft_id)


class RejectOutcome(str, Enum):
    REJECTED = "rejected"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"     # already sent / rejected / being sent
    ERROR = "error"


@dataclass(frozen=True)
class RejectResult:
    outcome: RejectOutcome
    draft: Optional[DraftView] = None
    detail: str = ""


async def reject_draft(draft_id: int, *, reviewer: str, note: Optional[str]) -> RejectResult:
    """Mark a pending (or failed) draft rejected. Terminal: it is never drafted or sent again. Never raises."""
    try:
        async with SessionLocal() as session:
            res = await session.execute(
                update(FoiaRequest)
                .where(FoiaRequest.id == draft_id, FoiaRequest.status.in_(REVIEWABLE_STATUSES))
                .values(status=STATUS_REJECTED, **_fields(FoiaRequest, reviewed_by=reviewer, review_note=note),
                        reviewed_at=_utcnow(), updated_at=_utcnow())
            )
            await session.commit()
            row = await session.get(FoiaRequest, draft_id, populate_existing=True)
            if row is None:
                return RejectResult(RejectOutcome.NOT_FOUND, detail=f"draft {draft_id} does not exist")
            if res.rowcount == 1:  # type: ignore[attr-defined]
                return RejectResult(RejectOutcome.REJECTED, _view(row))
            return RejectResult(RejectOutcome.CONFLICT, _view(row), f"draft is {row.status}, only pending or failed drafts can be rejected")
    except Exception as exc:  # noqa: BLE001
        logger.exception("reject_draft failed for draft %s", draft_id)
        return RejectResult(RejectOutcome.ERROR, detail=f"{type(exc).__name__}: {exc}")


async def record_review_outcome(message_id: str, action_status: str, *, to_email: Optional[str], error: Optional[str] = None) -> int:
    """
    After approve / reject: append the outcome (foia_sent / foia_send_failed / foia_rejected) to
    every document whose latest action is still the draft (matched by the draft's message id),
    and refresh those gazettes' summaries, so /gazettes/{id} shows what really happened.
    Own transaction; never raises; returns how many documents were updated.
    """
    try:
        async with SessionLocal() as session:
            doc_ids = (await session.scalars(
                select(FoiaAction.missing_document_id)
                .where(FoiaAction.foia_message_id == message_id, FoiaAction.action_status.in_(_DRAFT_ACTION_STATUSES))
                .distinct()
            )).all()
            gazettes: set[str] = set()
            written = 0
            for doc_id in doc_ids:
                doc = await session.get(
                    MissingDocument, doc_id,
                    options=[selectinload(MissingDocument.actions), selectinload(MissingDocument.signal).selectinload(Signal.gazette)],
                )
                if doc is None:
                    continue
                latest = _latest_action(doc)
                if latest is None or latest.foia_message_id != message_id or latest.action_status not in _DRAFT_ACTION_STATUSES:
                    continue   # a later attempt already superseded the draft
                doc.actions.append(FoiaAction(**_fields(
                    FoiaAction, action_status=action_status, institution_resolved=latest.institution_resolved,
                    foia_to=to_email or latest.foia_to, foia_message_id=message_id,
                    errors=[error] if error else None,
                )))
                gazettes.add(doc.signal.gazette.gazette_id)
                written += 1
            await session.flush()
            for key in gazettes:
                await _recompute_summary(session, key)
            await session.commit()
            return written
    except Exception:  # noqa: BLE001
        logger.exception("could not record review outcome %s for message %s", action_status, message_id)
        return 0


async def backfill_foia_requests_from_history() -> int:
    """
    Seed the ledger from FoiaAction rows already marked "foia_sent" so requests
    sent before the ledger existed are not drafted or sent again. Idempotent;
    returns the number of rows added. Safe to call on every startup.
    """
    try:
        async with SessionLocal() as session:
            history = (await session.execute(
                select(
                    FoiaAction.institution_resolved, MissingDocument.institution, MissingDocument.decision_number,
                    FoiaAction.foia_to, FoiaAction.foia_message_id, FoiaAction.created_at,
                )
                .join(MissingDocument, MissingDocument.id == FoiaAction.missing_document_id)
                .where(FoiaAction.action_status == "foia_sent")
                .order_by(FoiaAction.created_at)
            )).all()
            if not history:
                return 0

            known = {
                (i, d) for i, d in (await session.execute(
                    select(FoiaRequest.institution_key, FoiaRequest.decision_key)
                )).all()
            }
            added = 0
            for resolved, written, decision, to_email, message_id, created_at in history:
                name = resolved or written
                key = (normalize_institution(name), normalize_decision(decision))
                if key in known:
                    continue
                known.add(key)
                session.add(FoiaRequest(
                    institution_key=key[0], decision_key=key[1], institution=name[:255],
                    decision_number=decision[:64], status=STATUS_SENT, to_email=to_email, message_id=message_id,
                    attempts=1, sent_at=created_at, created_at=created_at, updated_at=created_at,
                ))
                added += 1
            await session.commit()
            if added:
                logger.info("foia_requests: backfilled %d already-sent request(s) from history", added)
            return added
    except IntegrityError:
        logger.info("foia_requests backfill raced with another worker; nothing to do")
        return 0
    except Exception:  # noqa: BLE001
        logger.exception("foia_requests backfill failed (continuing without it)")
        return 0


# --------------------------------------------------------------------------- #
# Retry support
# --------------------------------------------------------------------------- #


def _latest_action(doc: MissingDocument) -> Optional[FoiaAction]:
    if not doc.actions:
        return None
    return max(doc.actions, key=lambda a: (_as_utc(a.created_at), a.id or 0))


@dataclass(frozen=True)
class RetryCandidate:
    missing_document_id: int
    gazette_id: str
    signal_position: int
    institution: str            # as written in the gazette
    decision_number: str
    expected_content: str
    attempts: int               # actions recorded so far (first run + retries)
    last_status: str
    last_attempt_at: datetime


@dataclass
class RetryScan:
    candidates: list[RetryCandidate] = field(default_factory=list)
    waiting: int = 0            # retryable but still inside the backoff window
    exhausted: int = 0          # retryable but out of attempts (needs a human)


async def find_retry_candidates(
    *,
    gazette_id: Optional[str] = None,
    max_attempts: int,
    backoff_base_s: float,
    respect_backoff: bool = True,
    limit: int = 25,
) -> RetryScan:
    """
    Documents whose LATEST action is scrape_failed / foia_send_failed.

    After the n-th attempt the next one is due backoff_base_s * 2**(n-1) seconds
    later (unless respect_backoff is False, used for manual triggers). Once a
    document has max_attempts actions it is reported as exhausted and left alone.
    Oldest-failed first, at most `limit` candidates.
    """
    now = _utcnow()
    retryable_ids = select(FoiaAction.missing_document_id).where(FoiaAction.action_status.in_(RETRYABLE_STATUSES))
    stmt = (
        select(MissingDocument)
        .where(MissingDocument.id.in_(retryable_ids))
        .options(selectinload(MissingDocument.actions), selectinload(MissingDocument.signal).selectinload(Signal.gazette))
    )
    if gazette_id is not None:
        stmt = (
            stmt.join(Signal, Signal.id == MissingDocument.signal_id)
            .join(Gazette, Gazette.id == Signal.gazette_id)
            .where(Gazette.gazette_id == gazette_id)
        )

    scan = RetryScan()
    eligible: list[RetryCandidate] = []
    async with SessionLocal() as session:
        docs = (await session.scalars(stmt)).all()
        for doc in docs:
            latest = _latest_action(doc)
            if latest is None or latest.action_status not in RETRYABLE_STATUSES:
                continue  # a later attempt already resolved it
            attempts = len(doc.actions)
            if attempts >= max_attempts:
                scan.exhausted += 1
                continue
            last_at = _as_utc(latest.created_at)
            if respect_backoff and now - last_at < timedelta(seconds=backoff_base_s * (2 ** (attempts - 1))):
                scan.waiting += 1
                continue
            eligible.append(RetryCandidate(
                missing_document_id=doc.id,
                gazette_id=doc.signal.gazette.gazette_id,
                signal_position=doc.signal.position,
                institution=doc.institution,
                decision_number=doc.decision_number,
                expected_content=doc.expected_content or "",
                attempts=attempts,
                last_status=latest.action_status,
                last_attempt_at=last_at,
            ))
    eligible.sort(key=lambda c: c.last_attempt_at)
    scan.candidates = eligible[:limit]
    return scan


async def _recompute_summary(session: AsyncSession, gazette_key: str) -> None:
    """Set gazette.summary to the count of each document's LATEST action (caller commits)."""
    gazette = await load_gazette(session, gazette_key)
    if gazette is None:
        return
    # Start from the keys already in the stored summary so its shape stays stable.
    counts: dict[str, int] = {k: 0 for k in (gazette.summary or {})}
    for sig in gazette.signals:
        for d in sig.missing_documents:
            latest = _latest_action(d)
            if latest is not None:
                counts[latest.action_status] = counts.get(latest.action_status, 0) + 1
    gazette.summary = counts  # new object so the JSON column registers the change


async def append_action_for_document(missing_document_id: int, report: object) -> bool:
    """
    Store a retry outcome as a new FoiaAction (history is kept) and recompute the
    gazette's summary from each document's latest action. Own transaction; never
    raises. Returns True when the outcome was written.
    """
    async with SessionLocal() as session:
        try:
            doc = await session.get(
                MissingDocument, missing_document_id,
                options=[selectinload(MissingDocument.actions), selectinload(MissingDocument.signal).selectinload(Signal.gazette)],
            )
            if doc is None:
                logger.error("append_action_for_document: missing_document %d not found", missing_document_id)
                return False
            doc.actions.append(action_from_report(report))
            await session.flush()
            await _recompute_summary(session, doc.signal.gazette.gazette_id)
            await session.commit()
            return True
        except Exception:  # noqa: BLE001
            await session.rollback()
            logger.exception("could not store retry outcome for missing_document %d", missing_document_id)
            return False
