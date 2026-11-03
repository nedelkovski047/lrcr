"""
Retry worker for the Local Regulatory Change Radar (LRCR).

A gazette run stores one outcome per referenced document. One of those outcomes
means "nothing was proven, try again":

    scrape_failed        the institution's site was down / blocked / timed out
                         (or the FOIA draft could not be stored)

This module re-runs Tier 2 -> Tier 3 for exactly those documents, straight from
the database (no PDF, no Gemini call), and appends the new outcome as another
FoiaAction row so the history is kept. The latest row is the current state.
It never sends e-mail: Tier 3 only queues a draft (foia_pending_review) for a
person to approve, so a failed send is retried by approving the draft again.

    RetryWorker.run_pass(...)   one pass; used by the background loop, POST
                                /api/v1/retry-now and the re-upload path
    RetryWorker.start()/stop()  background loop (asyncio task, no scheduler dependency)

Schedule per document: after attempt n the next one is due
LRCR_RETRY_BACKOFF_MINUTES * 2**(n-1) minutes later; after LRCR_RETRY_MAX_ATTEMPTS
attempts in total the document is left alone and counted as "exhausted".
Defaults (60 min, 6 attempts) keep retrying for about 31 hours.

Duplicate protection does NOT live here: process_reference stores at most one
draft per (institution, decision) in the foia_requests table, so overlapping passes
or several server processes cannot queue (or send) the same request twice.

Environment:
    LRCR_RETRY_ENABLED           default 1   (0 disables the background loop; manual triggers still work)
    LRCR_RETRY_INTERVAL_MINUTES  default 30  how often the loop looks for due documents
    LRCR_RETRY_STARTUP_DELAY_S   default 90
    LRCR_RETRY_MAX_ATTEMPTS      default 6   total attempts per document, first run included
    LRCR_RETRY_BACKOFF_MINUTES   default 60
    LRCR_RETRY_MAX_PER_PASS      default 25
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from persistence import RetryCandidate, append_action_for_document, find_retry_candidates
from schemas import MissingDocumentRef

logger = logging.getLogger("lrcr.retry")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        logger.warning("%s is not a number, using %s", name, default)
        return float(default)


RETRY_ENABLED: bool = _env_bool("LRCR_RETRY_ENABLED", True)
RETRY_INTERVAL_MINUTES: float = _env_float("LRCR_RETRY_INTERVAL_MINUTES", 30)
RETRY_STARTUP_DELAY_S: float = _env_float("LRCR_RETRY_STARTUP_DELAY_S", 90)
RETRY_MAX_ATTEMPTS: int = int(_env_float("LRCR_RETRY_MAX_ATTEMPTS", 6))
RETRY_BACKOFF_MINUTES: float = _env_float("LRCR_RETRY_BACKOFF_MINUTES", 60)
RETRY_MAX_PER_PASS: int = int(_env_float("LRCR_RETRY_MAX_PER_PASS", 25))

# (signal_index, reference, shared aiohttp session) -> main.DocumentActionReport.
# Injected by main.py; typed loosely so this module never imports main (no cycle).
ProcessReference = Callable[[int, MissingDocumentRef, aiohttp.ClientSession], Awaitable[Any]]


@dataclass
class RetrySummary:
    started_at: datetime
    finished_at: Optional[datetime] = None
    gazette_id: Optional[str] = None       # set when the pass was limited to one gazette
    retried: int = 0                        # documents re-run
    stored: int = 0                         # outcomes written to the database
    waiting: int = 0                        # due later (inside the backoff window)
    exhausted: int = 0                      # out of attempts, need a human
    outcomes: dict[str, int] = field(default_factory=dict)   # action value -> count for this pass
    reports: list[Any] = field(default_factory=list)          # DocumentActionReport objects

    def as_dict(self) -> dict[str, object]:
        return {
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "gazette_id": self.gazette_id,
            "retried": self.retried,
            "stored": self.stored,
            "waiting": self.waiting,
            "exhausted": self.exhausted,
            "outcomes": dict(self.outcomes),
        }


class RetryWorker:
    def __init__(self, process_reference: ProcessReference, http: aiohttp.ClientSession, *, concurrency: int = 3) -> None:
        self._process_reference = process_reference
        self._http = http
        self._concurrency = max(1, concurrency)
        self._lock = asyncio.Lock()          # one pass at a time per process
        self._task: Optional[asyncio.Task[None]] = None
        self.last_summary: Optional[RetrySummary] = None

    # ------------------------------------------------------------------ #

    async def run_pass(
        self,
        *,
        gazette_id: Optional[str] = None,
        respect_backoff: bool = True,
        max_refs: int = RETRY_MAX_PER_PASS,
    ) -> RetrySummary:
        """
        Re-run every due failed document (optionally only one gazette's).

        Never raises for per-document problems; each outcome is stored as soon as
        it is known, so a crash mid-pass loses at most the documents in flight.
        Passes are serialised: a second caller waits for the first to finish.
        """
        async with self._lock:
            summary = RetrySummary(started_at=datetime.now(timezone.utc), gazette_id=gazette_id)
            scan = await find_retry_candidates(
                gazette_id=gazette_id,
                max_attempts=RETRY_MAX_ATTEMPTS,
                backoff_base_s=RETRY_BACKOFF_MINUTES * 60,
                respect_backoff=respect_backoff,
                limit=max_refs,
            )
            summary.waiting, summary.exhausted = scan.waiting, scan.exhausted
            if scan.exhausted:
                logger.warning("retry: %d document(s) used all %d attempts and need manual attention",
                               scan.exhausted, RETRY_MAX_ATTEMPTS)

            sem = asyncio.Semaphore(self._concurrency)

            async def _one(c: RetryCandidate) -> None:
                async with sem:
                    ref = MissingDocumentRef(
                        institution=c.institution, decision_number=c.decision_number,
                        expected_content=c.expected_content,
                    )
                    logger.info("retry: %s / %s %s (attempt %d, last: %s)", c.gazette_id, c.institution,
                                c.decision_number, c.attempts + 1, c.last_status)
                    try:
                        report = await self._process_reference(c.signal_position, ref, self._http)
                    except Exception:  # noqa: BLE001 — process_reference should never raise; don't lose the pass if it does
                        logger.exception("retry: process_reference crashed for %s %s", c.institution, c.decision_number)
                        return
                    summary.retried += 1
                    summary.reports.append(report)
                    value = str(getattr(getattr(report, "action"), "value", getattr(report, "action")))
                    summary.outcomes[value] = summary.outcomes.get(value, 0) + 1
                    if await append_action_for_document(c.missing_document_id, report):
                        summary.stored += 1

            if scan.candidates:
                t0 = time.perf_counter()
                await asyncio.gather(*(_one(c) for c in scan.candidates))
                logger.info("retry pass done in %.1fs: retried=%d stored=%d outcomes=%s waiting=%d exhausted=%d",
                            time.perf_counter() - t0, summary.retried, summary.stored, summary.outcomes,
                            summary.waiting, summary.exhausted)
            summary.finished_at = datetime.now(timezone.utc)
            if gazette_id is None:
                self.last_summary = summary
            return summary

    # ------------------------------------------------------------------ #

    async def _loop(self) -> None:
        await asyncio.sleep(RETRY_STARTUP_DELAY_S)
        while True:
            try:
                await self.run_pass()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — the loop must survive anything
                logger.exception("retry pass crashed; will try again next interval")
            await asyncio.sleep(RETRY_INTERVAL_MINUTES * 60)

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop(), name="lrcr-retry-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()
