"""
Tier 3 — FOIA fallback for the Local Regulatory Change Radar (LRCR).

When the Tier 2 scraper reports `found=False`, this module drafts a
"Барање за пристап до информации од јавен карактер" (request under the
Закон за слободен пристап до информации од јавен карактер, Сл. весник на
РСМ бр. 101/2019) addressed to the institution's official inbox, asking for
a digital copy of the missing decision.

Drafting only — sending is `mailer.dispatch_foia_email`. `queue_foia_email`
builds the RFC 5322 message and hands back a `QueuedFoiaEmail` with the
plain (to, subject, body, message_id) the mailer needs, plus the full
message for anyone who wants to inspect the raw MIME form. Sender identity
(name/address/e-mail/phone) comes from `mailer`'s LRCR_SENDER_* environment
variables, so the two modules never disagree about who the requester is.

Usage:
    body = await generate_foia_request("ЈП Комуналец Битола", "02-164/7",
                                       "Одлука за утврдување на цени за комунални услуги")
    queued = queue_foia_email("ЈП Комуналец Битола", "02-164/7", body)
    result = await mailer.dispatch_foia_email(queued.to_email, queued.subject,
                                              queued.body, queued.message_id)

    # or directly from a Tier 2 result:
    queued = await foia_from_scraper_result(result, expected_content="...")
"""

from __future__ import annotations

import os
import asyncio
import logging
import re
import textwrap
import unicodedata
from dataclasses import dataclass
from datetime import date
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Final, Optional

from pydantic import BaseModel, EmailStr, Field

import mailer

logger = logging.getLogger("lrcr.foia")

# --123------------------------------------------------------------------------- #
# Institution registry
# --------------------------------------------------------------------------- #

INSTITUTION_EMAILS: Final[dict[str, str]] = {
    "ЈППЈЛП Битола": "jppjlp.bt@gmail.com",
    "ЈП Комуналец Битола": "komunalecbt@komunalecbt.com.mk",
    "КЈП Нискоградба Битола": "niskogradba@t.mk",
}

# Full legal names used in the salutation / addressee block.
INSTITUTION_FULL_NAMES: Final[dict[str, str]] = {
    "ЈППЈЛП Битола": "Јавно претпријатие за паркинзи, јавно осветлување и локални патишта „ЈППЈЛП“ – Битола",
    "ЈП Комуналец Битола": "Јавно претпријатие „Комуналец“ – Битола",
    "КЈП Нискоградба Битола": "Комунално јавно претпријатие „Нискоградба“ – Битола",
}

# Alternative spellings a gazette may use, mapped to the canonical key above.
_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "ЈППЈЛП Битола": ("паркинзи", "паркинг", "parking", "jppjlp", "јппјлп"),
    "ЈП Комуналец Битола": ("комуналец", "komunalec"),
    "КЈП Нискоградба Битола": ("нискоградба", "niskogradba"),
}

LEGAL_DEADLINE_DAYS: Final[int] = 20  # чл. 21 од Законот — рок за одговор на имателот


class UnknownInstitutionError(KeyError):
    """Raised when no contact e-mail is registered for the institution."""


class Sender(BaseModel):
    """
    Details of the requester. Defaults come from mailer's LRCR_SENDER_*
    environment variables — bracket placeholders until those are set, so
    `is_placeholder` and mailer.sender_is_placeholder() agree.
    """


    name: str = os.environ.get("LRCR_SENDER_NAME", "Radar")
    address: str = mailer.SENDER_ADDRESS
    email: str = mailer.SENDER_EMAIL
    phone: str = mailer.SENDER_PHONE

    @property
    def is_placeholder(self) -> bool:
        return any(v.startswith("[") and v.endswith("]") for v in (self.name, self.address, self.email))


class FoiaRequest(BaseModel):
    """A drafted request, ready to be turned into an e-mail."""

    institution: str
    institution_full_name: str
    to_email: EmailStr
    decision_number: str
    expected_content: str
    subject: str
    body: str
    created_on: date = Field(default_factory=date.today)


# --------------------------------------------------------------------------- #
# Institution resolution
# --------------------------------------------------------------------------- #


def _norm(name: str) -> str:
    n = unicodedata.normalize("NFKC", name).lower()
    n = re.sub(r"[\"'„“”«»().,\-–—/]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def resolve_institution(institution_name: str) -> str:
    """
    Map a free-text institution name to a canonical key of INSTITUTION_EMAILS.

    Exact key match first, then alias containment. Raises
    UnknownInstitutionError when nothing matches — the caller must decide
    what to do with a document from an institution we have no address for.
    """
    if institution_name in INSTITUTION_EMAILS:
        return institution_name
    needle = _norm(institution_name)
    best: Optional[tuple[int, str]] = None
    for key, aliases in _ALIASES.items():
        for alias in (key, *aliases):
            a = _norm(alias)
            if a and (a in needle or needle in a):
                if best is None or len(a) > best[0]:
                    best = (len(a), key)
    if best is None:
        raise UnknownInstitutionError(f"no FOIA contact registered for institution {institution_name!r}")
    return best[1]


# --------------------------------------------------------------------------- #
# Template
# --------------------------------------------------------------------------- #

_TEMPLATE: Final[str] = """\
До: {institution_full_name}
Адреса за е-пошта: {to_email}

Од: {sender_name}
Адреса: {sender_address}
Е-пошта: {sender_email}
Телефон: {sender_phone}

Датум: {date}

Предмет: БАРАЊЕ ЗА ПРИСТАП ДО ИНФОРМАЦИИ ОД ЈАВЕН КАРАКТЕР
         (Одлука бр. {decision_number})

Почитувани,

Врз основа на член 4 и член 12 од Законот за слободен пристап до информации
од јавен карактер („Службен весник на Република Северна Македонија“
бр. 101/2019), како барател на информации поднесувам барање за пристап до
следната информација од јавен карактер:

{item_1}

{item_2}

Информацијата ја барам во ЕЛЕКТРОНСКА ФОРМА (PDF или друг машински
читлив формат), доставена по електронска пошта на горенаведената адреса.

Наведената одлука е референцирана во Службен гласник на Општина Битола,
но не е достапна на официјалната веб-страница на {institution_short} по
спроведена проверка на ден {date}.

Согласно член 21 од Законот, љубезно очекувам одговор во законскиот рок
од {deadline_days} дена од денот на приемот на ова барање. Доколку не
располагате со бараната информација, ве молам, согласно член 18 од
Законот, барањето да го препратите до имателот што ја поседува и за тоа
да ме известите.

Однапред ви благодарам на соработката.

Со почит,

{sender_name}
{sender_address}
{sender_email} | {sender_phone}
"""


def _macedonian_date(d: date) -> str:
    return f"{d.day:02d}.{d.month:02d}.{d.year} година"


def build_foia_request(
    institution_name: str,
    decision_number: str,
    expected_content: str,
    sender: Optional[Sender] = None,
    on_date: Optional[date] = None,
) -> FoiaRequest:
    """Synchronous core: resolve the institution and render the template."""
    key = resolve_institution(institution_name)
    sender = sender or Sender()
    on_date = on_date or date.today()
    decision_number = decision_number.strip()
    expected_content = " ".join(expected_content.split()) or "содржината не е позната"

    def _item(n: int, text: str) -> str:
        return textwrap.fill(" ".join(text.split()), width=76,
                             initial_indent=f"    {n}. ", subsequent_indent="       ")

    item_1 = _item(1, (
        f"Целосна дигитална копија на Одлука бр. {decision_number} ({expected_content}), "
        f"донесена од страна на {key}, заедно со сите прилози што се составен дел на одлуката."
    ))
    item_2 = _item(2, (
        "Податок за датумот на донесување и датумот на објавување на наведената одлука, "
        "како и податок дали и каде истата е јавно објавена (веб-страница, службено гласило)."
    ))

    body = _TEMPLATE.format(
        item_1=item_1,
        item_2=item_2,
        institution_full_name=INSTITUTION_FULL_NAMES[key],
        institution_short=key,
        to_email=INSTITUTION_EMAILS[key],
        sender_name=sender.name,
        sender_address=sender.address,
        sender_email=sender.email,
        sender_phone=sender.phone,
        date=_macedonian_date(on_date),
        decision_number=decision_number,
        expected_content=expected_content,
        deadline_days=LEGAL_DEADLINE_DAYS,
    )
    subject = f"Барање за пристап до информации од јавен карактер – Одлука бр. {decision_number}"
    return FoiaRequest(
        institution=key,
        institution_full_name=INSTITUTION_FULL_NAMES[key],
        to_email=INSTITUTION_EMAILS[key],
        decision_number=decision_number,
        expected_content=expected_content,
        subject=subject,
        body=body,
        created_on=on_date,
    )


async def generate_foia_request(
    institution_name: str,
    decision_number: str,
    expected_content: str,
    sender: Optional[Sender] = None,
) -> str:
    """
    Async entry point required by the Tier 3 orchestrator.

    Returns the rendered request text. Rendering is CPU-trivial, so this
    simply wraps the sync builder; the async signature keeps the pipeline
    uniform (Tier 2 -> Tier 3 are both awaited).
    """
    logger.info("FOIA draft requested: institution=%r decision=%r", institution_name, decision_number)
    try:
        req = await asyncio.to_thread(build_foia_request, institution_name, decision_number,
                                      expected_content, sender)
    except UnknownInstitutionError:
        logger.error("FOIA draft failed: unknown institution %r", institution_name)
        raise
    logger.info("FOIA draft ready for %s (%d chars)", req.institution, len(req.body))
    return req.body


# --------------------------------------------------------------------------- #
# E-mail payload
# --------------------------------------------------------------------------- #


def build_email(
    institution_name: str,
    decision_number: str,
    request_body: str,
    sender: Optional[Sender] = None,
    subject: Optional[str] = None,
) -> EmailMessage:
    """Wrap a rendered request in an RFC 5322 message (UTF-8, 8-bit body)."""
    key = resolve_institution(institution_name)
    sender = sender or Sender()
    msg = EmailMessage()
    # Address() quotes display names that contain commas/quotes (e.g. the
    # Parking enterprise's full name) so they are not split into two recipients.
    if sender.is_placeholder:
        msg["From"] = sender.email
    else:
        msg["From"] = Address(display_name=sender.name, addr_spec=sender.email)
    msg["To"] = Address(display_name=INSTITUTION_FULL_NAMES[key], addr_spec=INSTITUTION_EMAILS[key])
    msg["Subject"] = subject or (
        f"Барање за пристап до информации од јавен карактер – Одлука бр. {decision_number.strip()}"
    )
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="lrcr.local")
    msg["X-LRCR-Institution"] = key
    msg["X-LRCR-Decision"] = decision_number.strip()
    msg.set_content(request_body, charset="utf-8", cte="8bit")
    return msg


@dataclass(frozen=True)
class QueuedFoiaEmail:
    """
    A drafted FOIA request, ready for mailer.dispatch_foia_email.

    `to_email`/`subject`/`body`/`message_id` are the plain values the mailer
    needs (its signature takes exactly these four); `message` is the full
    RFC 5322 EmailMessage, for anyone who wants to inspect headers or the
    raw wire form (the CLI demo below does).
    """

    institution: str          # canonical scraper/registry key
    to_email: str
    subject: str
    body: str
    message_id: str
    message: EmailMessage


def queue_foia_email(
    institution_name: str,
    decision_number: str,
    request_body: str,
    sender: Optional[Sender] = None,
) -> QueuedFoiaEmail:
    """
    Build the outgoing e-mail and hand back what mailer.dispatch_foia_email needs.

    Drafting only — this function does not print or send anything itself;
    that is mailer's job (dry-run preview or real SMTP), so a draft is never
    mistaken for proof of delivery.
    """
    key = resolve_institution(institution_name)
    msg = build_email(institution_name, decision_number, request_body, sender)
    if (sender or Sender()).is_placeholder:
        logger.warning("FOIA e-mail for %s drafted with placeholder sender details — "
                       "set LRCR_SENDER_NAME/LRCR_SENDER_EMAIL before a live send", key)
    logger.info("FOIA e-mail drafted for %s -> %s", key, INSTITUTION_EMAILS[key])
    return QueuedFoiaEmail(
        institution=key,
        to_email=INSTITUTION_EMAILS[key],
        subject=str(msg["Subject"]),
        body=request_body,
        message_id=str(msg["Message-ID"]),
        message=msg,
    )


# --------------------------------------------------------------------------- #
# Tier 2 -> Tier 3 bridge
# --------------------------------------------------------------------------- #


async def foia_from_scraper_result(
    result: object,
    expected_content: str,
    sender: Optional[Sender] = None,
) -> Optional[QueuedFoiaEmail]:
    """
    Convenience for the orchestrator: takes a Tier 2 `ScraperResult` (duck-typed
    to avoid a hard import), and drafts a FOIA e-mail only when the document
    was NOT found. Returns None when nothing needs to be sent. Does not send
    — the caller passes the result to mailer.dispatch_foia_email.
    """
    found = bool(getattr(result, "found", False))
    institution = str(getattr(result, "institution", ""))
    decision_number = str(getattr(result, "decision_number", ""))
    if found:
        logger.info("Tier 3 skipped: %s %s was found online at %s",
                    institution, decision_number, getattr(result, "document_url", None))
        return None
    body = await generate_foia_request(institution, decision_number, expected_content, sender)
    return queue_foia_email(institution, decision_number, body, sender)


# --------------------------------------------------------------------------- #
# CLI demo
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LRCR Tier 3 FOIA draft generator")
    parser.add_argument("institution", help='e.g. "ЈП Комуналец Битола"')
    parser.add_argument("decision_number", help='e.g. "02-164/7"')
    parser.add_argument("expected_content", nargs="?",
                        default="Одлука за утврдување на цени на услуги",
                        help="Short description of what the decision is about")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    async def _main() -> None:
        body = await generate_foia_request(args.institution, args.decision_number, args.expected_content)
        queued = queue_foia_email(args.institution, args.decision_number, body)
        result = await mailer.dispatch_foia_email(
            queued.to_email, queued.subject, queued.body, queued.message_id
        )
        print(result)

    asyncio.run(_main())
