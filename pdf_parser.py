"""
Tier 1 — Gazette PDF parser for the Local Regulatory Change Radar (LRCR).

    extract_text_from_pdf(path)               pypdf, page by page, never raises
                                              on a bad page (partial text wins)
    analyze_gazette_with_llm(gazette_id, text) gemini-3.6-flash, structured
                                              output forced to GazetteAnalysisResult

Environment:
    GEMINI_API_KEY   picked up automatically by google.genai.Client()

Dependencies:
    pypdf, google-genai, pydantic
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Final, Optional

from pydantic import ValidationError
from pypdf import PdfReader
from pypdf.errors import PdfReadError

from schemas import GazetteAnalysisResult

logger = logging.getLogger("lrcr.tier1")
logging.getLogger("pypdf").setLevel(logging.ERROR)

# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

GEMINI_MODEL: Final[str] = os.environ.get("LRCR_GEMINI_MODEL", "gemini-3.8-flash")
# A single Gemini call must stay well under the account's 2,000,000 Tokens-Per-Minute cap by itself,
# with no help from spacing between calls. Previously 8_000_000 chars (a "gazettes are far smaller"
# ceiling that was never meant to be hit) — a real ~8M-character gazette got through it and alone used
# close to the whole per-minute quota. 1_500_000 keeps one request safely under the cap even for dense
# Macedonian Cyrillic text, which tokenizes worse (more tokens per character) than Latin script. This
# is a truncation ceiling, not a target: ordinary gazettes are far smaller and never reach it — a
# gazette that does hit it is having its tail end silently dropped, so check the "text truncated from"
# warning log if a huge document's later decisions seem to go missing from the analysis.
MAX_TEXT_CHARS: Final[int] = 1_500_000
MIN_TEXT_CHARS: Final[int] = 200            # below this the PDF is almost certainly scanned
LLM_RETRIES: Final[int] = 3
LLM_BACKOFF_S: Final[float] = 2.0
LLM_TIMEOUT_S: Final[float] = 180.0

TARGET_INSTITUTIONS: Final[tuple[str, ...]] = (
    "ЈППЈЛП Битола",
    "ЈП Комуналец Битола",
    "КЈП Нискоградба Битола",
)


class GazetteTextEmpty(ValueError):
    """The PDF yielded no usable text (scanned image, encrypted, or empty)."""


class LLMAnalysisError(RuntimeError):
    """The model did not return a valid GazetteAnalysisResult after retries."""


# --------------------------------------------------------------------------- #
# Text extraction
# --------------------------------------------------------------------------- #


def _clean_page_text(text: str) -> str:
    text = text.replace("\x00", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_text_from_pdf(file_path: str) -> str:
    """
    Extract text from every page of a PDF.

    A page that fails to parse is logged and skipped; the text of all other
    pages is still returned. Raises GazetteTextEmpty when the document
    produced (almost) nothing — typically a scanned gazette that needs OCR —
    and FileNotFoundError / PdfReadError for an unreadable file.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"PDF not found: {file_path}")

    try:
        reader = PdfReader(str(path), strict=False)
    except PdfReadError as exc:
        raise PdfReadError(f"cannot open {path.name}: {exc}") from exc

    if reader.is_encrypted:
        try:
            reader.decrypt("")
        except Exception as exc:  # noqa: BLE001
            raise PdfReadError(f"{path.name} is encrypted and cannot be opened: {exc}") from exc

    chunks: list[str] = []
    failed_pages: list[int] = []
    running_chars = 0
    stopped_early = False
    total = len(reader.pages)
    attempted = 0
    for index in range(total):
        # Once there is already enough text to reach MAX_TEXT_CHARS, everything after this
        # point gets sliced off below anyway. pypdf builds each page's full object tree
        # (fonts, content streams, embedded images) just to extract its text, so for an
        # oversized gazette (up to MAX_PDF_BYTES = 80 MB) this stops real memory and CPU
        # work, not just a wasted loop — the truncated output is identical either way,
        # since pages are appended in order and the tail was always going to be cut.
        if running_chars > MAX_TEXT_CHARS:
            stopped_early = True
            break
        attempted += 1
        page_no = index + 1
        try:
            page_text = reader.pages[index].extract_text() or ""
        except Exception as exc:  # noqa: BLE001 — one bad page must not sink the gazette
            failed_pages.append(page_no)
            logger.warning("%s: page %d extraction failed: %s", path.name, page_no, str(exc).splitlines()[0] if str(exc) else exc)
            continue
        cleaned = _clean_page_text(page_text)
        if cleaned:
            piece = f"--- страница {page_no} ---\n{cleaned}"
            chunks.append(piece)
            running_chars += len(piece) + 2  # +2: the "\n\n" join() adds between pieces

    text = "\n\n".join(chunks)
    logger.info("%s: %d/%d pages extracted%s, %d chars%s", path.name, attempted - len(failed_pages), total,
                " (stopped early: already past the text-size limit)" if stopped_early else "",
                len(text), f", failed pages: {failed_pages}" if failed_pages else "")

    if len(text) < MIN_TEXT_CHARS:
        raise GazetteTextEmpty(
            f"{path.name}: only {len(text)} characters of text in {total} pages — "
            "the PDF is probably scanned (needs OCR) or empty"
        )
    if len(text) > MAX_TEXT_CHARS:
        logger.warning("%s: text truncated from %d to %d chars", path.name, len(text), MAX_TEXT_CHARS)
        text = text[:MAX_TEXT_CHARS]
    return text


# --------------------------------------------------------------------------- #
# LLM analysis
# --------------------------------------------------------------------------- #

SYSTEM_INSTRUCTION: Final[str] = f"""\
Ти си аналитичар за општинска регулатива во Северна Македонија. Го читаш
Службениот гласник на Општина Битола и ги идентификуваш одлуките на Советот
и актите кои имаат влијание врз бизнисите и граѓаните: цени и тарифи на
комунални услуги, паркинг, такси, надоместоци, режими на работа, јавни
повици, урбанистички планови, програми за работа, буџетски измени, статути и
правилници на јавни претпријатија.

За секоја таква одлука определи дали текстот во гласникот е доволен за
дејствување (is_actionable_immediately = true) или гласникот само дава
согласност / упатува на друг документ чија содржина НЕ е репродуцирана
(is_actionable_immediately = false). Типичен пример: „Советот дава согласност
на Одлуката бр. 02-164/7 на Управниот одбор на ЈП Комуналец за утврдување на
цени…“ — самата одлука на Управниот одбор недостасува.

Посебно внимание на документи (одлуки, ценовници, програми, извештаи,
статути, правилници) на овие три јавни претпријатија:
{chr(10).join(f"  - {name}" for name in TARGET_INSTITUTIONS)}
За секој таков документ што недостасува додај MissingDocumentRef со:
  - institution: името на претпријатието онака како е напишано во гласникот
  - decision_number: само бројот на документот (пр. „02-164/7“), без „бр.“
  - expected_content: една реченица што опишува за што е документот

Правила:
  - Не измислувај броеви на одлуки. Ако бројот не е наведен, не додавај референца.
  - Игнорирај чисто административни акти без влијание (именувања, разрешувања,
    заклучоци за усвојување записник) освен ако упатуваат на документ што
    недостасува од целните претпријатија.
  - Одговарај исклучиво во бараната JSON шема. Полињата title, summary и
    expected_content се на македонски јазик.
  - gazette_id мора да биде точно вредноста што ти е дадена.
"""


def _build_prompt(gazette_id: str, text: str) -> str:
    return (
        f"gazette_id: {gazette_id}\n\n"
        "Анализирај го следниот текст од Службен гласник на Општина Битола и "
        "врати GazetteAnalysisResult.\n\n"
        "=== ПОЧЕТОК НА ГЛАСНИКОТ ===\n"
        f"{text}\n"
        "=== КРАЈ НА ГЛАСНИКОТ ==="
    )


def _parse_response(raw_text: Optional[str], parsed: object, gazette_id: str) -> GazetteAnalysisResult:
    """Prefer the SDK's parsed object; fall back to validating the JSON text."""
    result: Optional[GazetteAnalysisResult] = None
    if isinstance(parsed, GazetteAnalysisResult):
        result = parsed
    elif raw_text:
        result = GazetteAnalysisResult.model_validate_json(raw_text)
    if result is None:
        raise LLMAnalysisError("model returned neither a parsed object nor JSON text")
    if result.gazette_id != gazette_id:
        logger.warning("model returned gazette_id %r, overriding with %r", result.gazette_id, gazette_id)
        result.gazette_id = gazette_id
    return result


async def analyze_gazette_with_llm(
    gazette_id: str,
    text: str,
    *,
    model: str = GEMINI_MODEL,
    temperature: float = 0.0,
) -> GazetteAnalysisResult:
    """
    Ask Gemini to turn gazette text into a GazetteAnalysisResult.

    Structured output is enforced through response_schema; the reply is
    additionally validated with Pydantic. Transient API failures and invalid
    JSON are retried LLM_RETRIES times with exponential backoff.
    """
    if not text or len(text.strip()) < MIN_TEXT_CHARS:
        raise GazetteTextEmpty("gazette text is empty — nothing to analyse")

    # Imported lazily so the rest of the pipeline works without the SDK installed.
    from google import genai
    from google.genai import errors as genai_errors
    from google.genai import types

    client = genai.Client()  # reads GEMINI_API_KEY
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_INSTRUCTION,
        response_mime_type="application/json",
        response_schema=GazetteAnalysisResult,
        temperature=temperature,
    )
    prompt = _build_prompt(gazette_id, text)
    logger.info("Tier 1: sending %s (%d chars) to %s", gazette_id, len(text), model)

    last_error: Optional[BaseException] = None
    for attempt in range(1, LLM_RETRIES + 1):
        try:
            response = await asyncio.wait_for(
                client.aio.models.generate_content(model=model, contents=prompt, config=config),
                timeout=LLM_TIMEOUT_S,
            )
            result = _parse_response(response.text, getattr(response, "parsed", None), gazette_id)
            usage = getattr(response, "usage_metadata", None)
            logger.info(
                "Tier 1: %s -> %d signals, %d missing refs%s",
                gazette_id, len(result.signals),
                sum(len(s.missing_references) for s in result.signals),
                f" (tokens in/out: {usage.prompt_token_count}/{usage.candidates_token_count})" if usage else "",
            )
            return result

        except (ValidationError, json.JSONDecodeError, LLMAnalysisError) as exc:
            last_error = exc
            logger.warning("Tier 1 attempt %d/%d: invalid model output: %s", attempt, LLM_RETRIES,
                           str(exc).splitlines()[0])
        except asyncio.TimeoutError as exc:
            last_error = exc
            logger.warning("Tier 1 attempt %d/%d: timed out after %.0fs", attempt, LLM_RETRIES, LLM_TIMEOUT_S)
        except genai_errors.APIError as exc:
            last_error = exc
            code = getattr(exc, "code", None)
            if code in (400, 401, 403, 404):
                # Bad request / bad key / wrong model: retrying will not help.
                logger.error("Tier 1: non-retryable API error %s: %s", code, exc)
                raise LLMAnalysisError(f"Gemini API error {code}: {exc}") from exc
            logger.warning("Tier 1 attempt %d/%d: API error %s: %s", attempt, LLM_RETRIES, code, exc)

        if attempt < LLM_RETRIES:
            await asyncio.sleep(LLM_BACKOFF_S * (2 ** (attempt - 1)))

    raise LLMAnalysisError(f"Gemini analysis failed after {LLM_RETRIES} attempts: {last_error!r}") from last_error


async def analyze_pdf(file_path: str, gazette_id: Optional[str] = None) -> GazetteAnalysisResult:
    """Convenience: extract + analyse in one call. gazette_id defaults to the file stem."""
    gid = gazette_id or Path(file_path).stem
    text = await asyncio.to_thread(extract_text_from_pdf, file_path)
    return await analyze_gazette_with_llm(gid, text)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LRCR Tier 1 gazette parser")
    parser.add_argument("pdf", help="Path to a gazette PDF")
    parser.add_argument("-g", "--gazette-id", help="Defaults to the file name without extension")
    parser.add_argument("--text-only", action="store_true", help="Only extract text, skip the LLM")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    if args.text_only:
        print(extract_text_from_pdf(args.pdf))
    else:
        analysis = asyncio.run(analyze_pdf(args.pdf, args.gazette_id))
        print(analysis.model_dump_json(indent=2))
