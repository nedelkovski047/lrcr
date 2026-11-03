"""
SQLAlchemy ORM models for the Local Regulatory Change Radar (LRCR).

    Gazette 1 ── * Signal 1 ── * MissingDocument 1 ── * FoiaAction

    FoiaRequest  (independent of the tree above)
        The FOIA review queue and send ledger: one row per (institution, decision)
        pair holding the drafted e-mail (to, subject, body) and its review status.
        The UNIQUE constraint guarantees one draft per pair and that the same request
        is never sent twice, across gazettes, restarts and concurrent workers.

Column names mirror the Pydantic schemas in schemas.py and the
DocumentActionReport in main.py so that persistence is a straight mapping.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Gazette(Base):
    __tablename__ = "gazettes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gazette_id: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    filename: Mapped[Optional[str]] = mapped_column(String(255))
    source: Mapped[str] = mapped_column(String(16), default="api")          # "upload" | "api"
    published_on: Mapped[Optional[date]] = mapped_column(Date)
    published_on_raw: Mapped[Optional[str]] = mapped_column(String(32))     # what the LLM said, verbatim
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    extracted_chars: Mapped[Optional[int]] = mapped_column(Integer)
    summary: Mapped[Optional[dict]] = mapped_column(JSON)                   # action counts (latest outcome per document)

    signals: Mapped[list["Signal"]] = relationship(
        back_populates="gazette", cascade="all, delete-orphan", order_by="Signal.position"
    )

    def __repr__(self) -> str:
        # Deliberately does not touch self.signals: a lazy load inside repr()
        # (logging, debugger) raises MissingGreenlet on an async session.
        return f"<Gazette {self.gazette_id}>"


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gazette_id: Mapped[int] = mapped_column(ForeignKey("gazettes.id", ondelete="CASCADE"), nullable=False, index=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False)          # index within the gazette payload
    title: Mapped[Optional[str]] = mapped_column(String(512))
    summary: Mapped[Optional[str]] = mapped_column(Text)
    is_actionable_immediately: Mapped[bool] = mapped_column(Boolean, nullable=False)

    gazette: Mapped["Gazette"] = relationship(back_populates="signals")
    missing_documents: Mapped[list["MissingDocument"]] = relationship(
        back_populates="signal", cascade="all, delete-orphan"
    )


class MissingDocument(Base):
    __tablename__ = "missing_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id", ondelete="CASCADE"), nullable=False, index=True)
    institution: Mapped[str] = mapped_column(String(255), nullable=False)   # as written in the gazette
    decision_number: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_content: Mapped[str] = mapped_column(Text, default="")

    signal: Mapped["Signal"] = relationship(back_populates="missing_documents")
    actions: Mapped[list["FoiaAction"]] = relationship(
        back_populates="missing_document", cascade="all, delete-orphan", order_by="FoiaAction.created_at"
    )


class FoiaAction(Base):
    """
    One pipeline outcome for a missing document (Tier 2 + Tier 3 result).

    A document can have several rows: the first run, then one more per retry.
    The LATEST row is its current state.
    """

    __tablename__ = "foia_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    missing_document_id: Mapped[int] = mapped_column(
        ForeignKey("missing_documents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    action_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # Action enum value
    institution_resolved: Mapped[Optional[str]] = mapped_column(String(255))          # canonical scraper name
    document_url: Mapped[Optional[str]] = mapped_column(Text)
    evidence_location: Mapped[Optional[str]] = mapped_column(String(32))
    foia_to: Mapped[Optional[str]] = mapped_column(String(512))
    foia_message_id: Mapped[Optional[str]] = mapped_column(String(255))
    pages_visited: Mapped[int] = mapped_column(Integer, default=0)
    pdfs_checked: Mapped[int] = mapped_column(Integer, default=0)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[Optional[list]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)

    missing_document: Mapped["MissingDocument"] = relationship(back_populates="actions")


class FoiaRequest(Base):
    """
    FOIA request ledger and review queue: one row per (institution, decision) pair.

    The pipeline never sends e-mail. When Tier 2 cannot find a document it stores
    the drafted message here as "pending_review". A person approves or rejects it
    through the authenticated /review endpoints; only approval sends anything.

        (no row) -> pending_review --approve--> sending --> sent
                         |                         \\--> failed --approve--> sending ...
                         \\--reject--> rejected   (failed can also be rejected)

    * The UNIQUE constraint means there is at most one draft per pair, no matter how
      many gazettes cite the same decision, and a pair that was sent or rejected is
      never drafted again.
    * "sending" is written BEFORE the send call, so two clicks (or two tabs) cannot
      both send. A row stuck in "sending" (process killed mid-send) is never re-sent
      automatically: we cannot know whether the mail left, and "never e-mail twice"
      beats "always eventually e-mail".
    * "failed" means an approved send did not go through (mail provider error); it stays in
      the review list so it can be approved again.

    Keys are normalised (see persistence.normalize_*): case-folded, whitespace
    collapsed, dash variants unified, so "08-123/1" and "08 - 123 / 1" collide.
    """

    __tablename__ = "foia_requests"
    __table_args__ = (
        UniqueConstraint("institution_key", "decision_key", name="uq_foia_requests_institution_decision"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution_key: Mapped[str] = mapped_column(String(255), nullable=False)
    decision_key: Mapped[str] = mapped_column(String(64), nullable=False)
    institution: Mapped[str] = mapped_column(String(255), nullable=False)       # canonical name, display only
    decision_number: Mapped[str] = mapped_column(String(64), nullable=False)    # as written, display only
    # "pending_review" | "sending" | "sent" | "failed" | "rejected"
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    to_email: Mapped[Optional[str]] = mapped_column(String(512))                # recipient address of the draft
    subject: Mapped[Optional[str]] = mapped_column(Text)                        # drafted subject line
    body: Mapped[Optional[str]] = mapped_column(Text)                           # drafted message body
    message_id: Mapped[Optional[str]] = mapped_column(String(255))
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)   # send attempts so far
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    reviewed_by: Mapped[Optional[str]] = mapped_column(String(128))             # authenticated user who approved/rejected
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    review_note: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class PollLog(Base):
    """
    One row per poller attempt on a gazette URL. Lets the poller skip gazettes
    that keep failing (scanned PDFs, LLM errors) instead of retrying every run.
    """

    __tablename__ = "poll_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gazette_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)   # "processed" | "failed" | "skipped"
    error: Mapped[Optional[str]] = mapped_column(Text)
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


# Fast lookup for "which documents did this institution's decision N produce?"
Index("ix_missing_documents_institution_decision", MissingDocument.institution, MissingDocument.decision_number)

__all__ = ["Gazette", "Signal", "MissingDocument", "FoiaAction", "FoiaRequest", "PollLog"]
