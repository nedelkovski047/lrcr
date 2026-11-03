"""
Tier 2 — Targeted Web Scraper for the Local Regulatory Change Radar (LRCR).

Given an institution name and a decision number referenced in a municipal
gazette (Tier 1), this module searches the institution's own website — its
"News" (Новости / Известувања) and "Documents" (Документи / Извештај / Огласи)
sections plus the site search — for any page or PDF that mentions that
decision number.

Targets (Bitola public enterprises):

    ЈППЈЛП Битола (Parking)      https://jpparkinzibitola.mk/
        WordPress + Elementor. Documents listing at /municipality-documents/
        (paginated /page/N/), news on the homepage, WP search via ?s=.

    ЈП Комуналец Битола (Utilities)  http://www.komunalecbt.com.mk/
        WordPress (HTTP only). Attachments are served through the
        "download-attachments" plugin: /wp-content/plugins/download-attachments/
        includes/download.php?id=N — these are file downloads, never pages.

    КЈП Нискоградба (Infrastructure)
        NOTE: https://niskogradbabitola.mk/ is ONLY the customer bill-payment
        portal (ASP.NET, login page, no news/documents). The informational
        site is https://niskogradba.pelanet.mk/ — WordPress with plain
        permalinks: ?cat=28 (Извештај), ?cat=29 (Огласи), ?paged=N, ?s=.

Matching rules (what counts as "found"):
    * A site-search results page NEVER counts on its own — WordPress echoes
      the query ("Баравте: 02-164/7", "Search Results for: 02-164/7"), so
      only the result links on that page are used as candidates.
    * A match must come from: an article/listing body, a link's anchor text,
      a document filename, or the extracted text of a downloaded PDF.

Dependencies:
    required : aiohttp, beautifulsoup4, pydantic (v2)
    optional : pypdf     (PDF text extraction — without it only PDF filenames
                          and anchor text are matched)
               playwright (JS rendering fallback — `pip install playwright &&
                          playwright install chromium`)

Usage:
    result = await dispatch_scraper("ЈП Комуналец Битола", "02-164/7")
    results = await scrape_all("02-164/7")
"""

from __future__ import annotations

import asyncio
import io
import logging
import re
import unicodedata
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import ClassVar, Optional, Sequence
from urllib.parse import quote_plus, urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup, Tag
from pydantic import BaseModel, Field, HttpUrl

try:  # optional
    from pypdf import PdfReader

    _HAS_PYPDF = True
except ImportError:  # pragma: no cover
    PdfReader = None  # type: ignore[assignment]
    _HAS_PYPDF = False

logger = logging.getLogger("lrcr.scrapers")
# pypdf logs every malformed-file complaint at WARNING; we handle those ourselves.
logging.getLogger("pypdf").setLevel(logging.ERROR)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36 LRCR-Radar/1.0"
)
DEFAULT_TIMEOUT_S = 25
MAX_RETRIES = 3
RETRY_BACKOFF_S = 1.5
MAX_LISTING_PAGES = 5          # pagination depth per section
MAX_ARTICLES_TO_VISIT = 40     # candidate detail pages per scraper
MAX_PDFS_TO_DOWNLOAD = 25      # documents fully downloaded and text-extracted
MAX_HTML_BYTES = 8 * 1024 * 1024
MAX_PDF_BYTES = 15 * 1024 * 1024
MAX_PDF_PAGES_TO_READ = 30
CONCURRENCY = 5
RAW_TEXT_SNIPPET_CHARS = 1500  # context kept around the match

_PDF_RE = re.compile(r"\.pdf(?:$|[?#])", re.IGNORECASE)
_DOC_EXT_RE = re.compile(r"\.(?:pdf|docx?|xlsx?|odt)(?:$|[?#])", re.IGNORECASE)
_SKIP_HREF_RE = re.compile(r"^(?:javascript:|mailto:|tel:|#)", re.IGNORECASE)
# CMS download endpoints that stream a file rather than render a page.
_ATTACHMENT_RE = re.compile(
    r"(?:download\.php|/download-attachments/|/download/|[?&]attachment_id=|[?&]download=|[?&]dl=)",
    re.IGNORECASE,
)
# Phrases WordPress themes use for an empty search result.
_NO_RESULTS_RE = re.compile(
    r"(?:не се пронајдени|нема резултати|ништо не е пронајдено|nothing found|no results|"
    r"nothing matched|не е пронајдено|нема записи)",
    re.IGNORECASE,
)

# Keywords that make a link look like a news/document page worth visiting.
_INTERESTING_LINK_WORDS = (
    "одлук", "решени", "новост", "вест", "известув", "соопшт", "документ",
    "извешта", "оглас", "повик", "јавн", "набавк", "ценовн", "правилник",
    "статут", "програм", "план", "одлука", "odluk", "news", "document",
    "izvest", "oglas", "vest", "?p=", "?cat=", "?paged=", "/page/",
)

# CSS selectors for the main content region — used to ignore nav/footer noise.
_CONTENT_SELECTORS = ("main", "#main", "#content", "#primary", ".site-content",
                      ".content-area", ".elementor-location-archive", "article")


# --------------------------------------------------------------------------- #
# Pydantic models
# --------------------------------------------------------------------------- #


class MatchEvidence(BaseModel):
    """Where and how the decision number was found."""

    location: str = Field(
        description="One of: listing_page, article_page, pdf_text, pdf_filename, anchor_text"
    )
    url: str
    snippet: str = Field(default="", description="Text surrounding the match")


class ScraperResult(BaseModel):
    """Outcome of a Tier 2 scrape for one institution + decision number."""

    institution: str
    domain: str
    decision_number: str
    found: bool = False
    document_url: Optional[HttpUrl] = None
    raw_text: str = Field(default="", description="Text of the matched document/page (truncated)")
    evidence: list[MatchEvidence] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)
    pages_visited: int = 0
    pdfs_checked: int = 0
    used_js_rendering: bool = False
    errors: list[str] = Field(default_factory=list)
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# --------------------------------------------------------------------------- #
# Decision-number matching
# --------------------------------------------------------------------------- #


def normalize_text(text: str) -> str:
    """NFKC-normalise, unify dash/slash variants, collapse whitespace."""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[‐-―−]", "-", text)  # ‐ ‑ ‒ – — ― −
    text = text.replace("\\", "/")
    return re.sub(r"\s+", " ", text)


def build_decision_pattern(decision_number: str) -> re.Pattern[str]:
    """
    Build a tolerant regex for a decision number like "02-164/7".

    Matches "02-164/7", "02 - 164 / 7", "02–164/7", "бр.02-164/7", "02-164/07"
    but NOT "102-164/7" or "02-164/71".
    """
    norm = normalize_text(decision_number).strip()
    tokens = re.split(r"([-/.])", norm)
    parts: list[str] = []
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        if tok in "-/.":
            parts.append(r"\s*" + re.escape(tok) + r"\s*")
        elif tok.isdigit():
            parts.append(r"0*" + re.escape(tok.lstrip("0") or "0"))  # optional leading zeros
        else:
            parts.append(re.escape(tok))
    body = "".join(parts)
    return re.compile(rf"(?<![\d/\-]){body}(?![\d/\-])", re.IGNORECASE)


def find_match(text: str, pattern: re.Pattern[str]) -> Optional[str]:
    """Return a snippet around the first match, or None."""
    if not text:
        return None
    norm = normalize_text(text)
    m = pattern.search(norm)
    if not m:
        return None
    half = RAW_TEXT_SNIPPET_CHARS // 2
    start = max(0, m.start() - half)
    end = min(len(norm), m.end() + half)
    return norm[start:end].strip()


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #


class FetchError(Exception):
    """Raised when a URL cannot be fetched after retries."""


def is_document_url(url: str) -> bool:
    """True for anything that streams a file: .pdf/.docx/... or a CMS download endpoint."""
    return bool(_DOC_EXT_RE.search(url) or _ATTACHMENT_RE.search(url))


@dataclass
class ScrapeContext:
    """Mutable per-run state shared between the base-class steps."""

    pattern: re.Pattern[str]
    result: ScraperResult
    visited: set[str] = field(default_factory=set)
    candidate_pages: list[str] = field(default_factory=list)
    candidate_docs: list[tuple[str, str]] = field(default_factory=list)  # (url, anchor text)


class BaseScraper(ABC):
    """
    Generic institution-website scraper.

    Subclasses configure the site via class attributes; the search pipeline
    itself lives here:

        1. site search  (search_url_template, if the CMS has one) — result
           links only, the page body is never matched
        2. listing pages (news + documents sections, with pagination)
        3. candidate article pages collected from the listings
        4. document filenames / anchor text, then full PDF text extraction

    The scrape stops at the first confirmed match.
    """

    institution: ClassVar[str] = "unknown"
    aliases: ClassVar[tuple[str, ...]] = ()
    base_url: ClassVar[str] = ""
    # Listing page URL templates. "{page}" is replaced with 1..MAX_LISTING_PAGES.
    # A template without "{page}" is fetched once.
    news_paths: ClassVar[tuple[str, ...]] = ()
    document_paths: ClassVar[tuple[str, ...]] = ()
    # "{query}" is replaced with the URL-encoded decision number.
    search_url_template: ClassVar[Optional[str]] = None
    requires_js: ClassVar[bool] = False
    # If plain HTTP returns fewer anchors than this, retry with Playwright.
    js_fallback_min_anchors: ClassVar[int] = 3
    verify_ssl: ClassVar[bool] = True

    def __init__(self, session: Optional[aiohttp.ClientSession] = None) -> None:
        self._external_session = session
        self._session: Optional[aiohttp.ClientSession] = session
        self._sem = asyncio.Semaphore(CONCURRENCY)
        self._js_available: Optional[bool] = None

    # ----------------------------------------------------------------- session

    async def __aenter__(self) -> "BaseScraper":
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT_S)
            connector = aiohttp.TCPConnector(ssl=None if self.verify_ssl else False, limit=CONCURRENCY)
            self._session = aiohttp.ClientSession(
                timeout=timeout, connector=connector, headers={"User-Agent": USER_AGENT}
            )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._session is not None and self._external_session is None:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("Scraper must be used as an async context manager")
        return self._session

    @property
    def domain(self) -> str:
        return urlparse(self.base_url).netloc

    # -------------------------------------------------------------- fetching

    async def fetch_bytes(self, url: str, max_bytes: int = MAX_PDF_BYTES) -> tuple[bytes, str]:
        """
        GET a URL with retries. Returns (body, content_type).

        Reads the body in chunks until EOF — `StreamReader.read(n)` returns
        *up to* n bytes and can stop early, which silently truncates PDFs.
        """
        last_err: Optional[BaseException] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self._sem:
                    async with self.session.get(url, allow_redirects=True) as resp:
                        if resp.status >= 400:
                            raise FetchError(f"HTTP {resp.status} for {url}")
                        ctype = resp.headers.get("Content-Type", "")
                        clen = resp.headers.get("Content-Length")
                        if clen and clen.isdigit() and int(clen) > max_bytes:
                            raise FetchError(f"Body too large ({clen} bytes) for {url}")
                        buf = bytearray()
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            buf.extend(chunk)
                            if len(buf) > max_bytes:
                                raise FetchError(f"Body exceeded {max_bytes} bytes for {url}")
                        return bytes(buf), ctype
            except (aiohttp.ClientError, asyncio.TimeoutError, FetchError) as exc:
                last_err = exc
                if isinstance(exc, FetchError) and ("HTTP 4" in str(exc) or "too large" in str(exc)
                                                    or "exceeded" in str(exc)):
                    break  # not worth retrying
                logger.debug("[%s] fetch attempt %d/%d failed for %s: %s",
                             self.institution, attempt, MAX_RETRIES, url, exc)
                await asyncio.sleep(RETRY_BACKOFF_S * attempt)
        raise FetchError(f"{url}: {last_err}")

    async def fetch_html(self, url: str, ctx: ScrapeContext) -> Optional[str]:
        """
        Fetch a page as HTML. Uses Playwright when the site requires JS or when
        the plain response looks empty (no/few anchors). Document/attachment
        URLs are never rendered — they are routed to the document queue.
        """
        if is_document_url(url):
            ctx.candidate_docs.append((url, ""))
            return None

        if self.requires_js:
            html = await self._render_with_playwright(url, ctx)
            if html is not None:
                return html
            logger.warning("[%s] Playwright unavailable/failed for %s — trying plain HTTP",
                           self.institution, url)

        try:
            body, ctype = await self.fetch_bytes(url, max_bytes=MAX_HTML_BYTES)
        except FetchError as exc:
            ctx.result.errors.append(str(exc))
            logger.warning("[%s] fetch failed: %s", self.institution, exc)
            return None

        if body[:5] == b"%PDF-" or "application/pdf" in ctype.lower():
            ctx.candidate_docs.append((url, ""))  # PDF without .pdf in the URL
            return None
        if "text/html" not in ctype.lower() and "xml" not in ctype.lower() and ctype:
            logger.debug("[%s] skipping non-HTML response (%s) at %s", self.institution, ctype, url)
            return None

        html = body.decode("utf-8", errors="replace")
        ctx.result.pages_visited += 1

        if not self.requires_js:
            anchors = BeautifulSoup(html, "html.parser").find_all("a", href=True)
            if len(anchors) < self.js_fallback_min_anchors:
                logger.info("[%s] %s has %d anchors — attempting JS render",
                            self.institution, url, len(anchors))
                rendered = await self._render_with_playwright(url, ctx)
                if rendered:
                    return rendered
        return html

    async def _render_with_playwright(self, url: str, ctx: ScrapeContext) -> Optional[str]:
        """Render a page in headless Chromium. Returns None if Playwright is unavailable."""
        if self._js_available is False:
            return None
        try:
            from playwright.async_api import async_playwright  # type: ignore[import-not-found]
        except ImportError:
            self._js_available = False
            ctx.result.errors.append("playwright not installed; JS rendering skipped")
            logger.warning("[%s] playwright not installed — JS-rendered content will be missed",
                           self.institution)
            return None
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                try:
                    page = await browser.new_page(user_agent=USER_AGENT,
                                                  ignore_https_errors=not self.verify_ssl,
                                                  accept_downloads=False)
                    await page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT_S * 1000)
                    html = await page.content()
                finally:
                    await browser.close()
            self._js_available = True
            ctx.result.used_js_rendering = True
            ctx.result.pages_visited += 1
            return html
        except Exception as exc:  # noqa: BLE001 — playwright raises many types
            msg = str(exc).splitlines()[0]
            if "Download is starting" in msg:
                # The URL is a file, not a page: hand it to the document queue.
                logger.info("[%s] %s is a download — queued as document", self.institution, url)
                ctx.candidate_docs.append((url, ""))
                return None
            ctx.result.errors.append(f"playwright render failed for {url}: {msg}")
            logger.warning("[%s] playwright render failed for %s: %s", self.institution, url, msg)
            return None

    # ------------------------------------------------------------- parsing

    @staticmethod
    def content_scope(soup: BeautifulSoup) -> Tag:
        """The main content region if the theme marks one, else the whole page."""
        for sel in _CONTENT_SELECTORS:
            node = soup.select_one(sel)
            if node is not None and node.find("a", href=True):
                return node
        return soup

    @staticmethod
    def node_text(node: Tag) -> str:
        for tag in node(["script", "style", "noscript", "svg", "nav", "header", "footer"]):
            tag.decompose()
        return node.get_text(" ", strip=True)

    def extract_links(self, node: Tag, page_url: str) -> list[tuple[str, str]]:
        """Deduplicated links inside `node` as (absolute_url, anchor_text)."""
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in node.find_all("a", href=True):
            href = a["href"].strip()
            if not href or _SKIP_HREF_RE.match(href):
                continue
            abs_url = urljoin(page_url, href)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            out.append((abs_url, a.get_text(" ", strip=True)))
        return out

    def _same_site(self, url: str) -> bool:
        host = urlparse(url).netloc.lower().removeprefix("www.")
        return host == self.domain.lower().removeprefix("www.")

    def _looks_interesting(self, url: str, text: str) -> bool:
        probe = (url + " " + text).lower()
        return any(w in probe for w in _INTERESTING_LINK_WORDS)

    def _harvest_links(self, links: list[tuple[str, str]], ctx: ScrapeContext,
                       take_all_pages: bool) -> bool:
        """
        Classify links into document / page candidates. Returns True when a
        link's anchor text or filename itself contains the decision number.
        """
        for link_url, text in links:
            if link_url in ctx.visited:
                continue
            if is_document_url(link_url):
                if find_match(text, ctx.pattern) or find_match(link_url, ctx.pattern):
                    loc = "pdf_filename" if _DOC_EXT_RE.search(link_url) else "anchor_text"
                    return self._record_match(ctx, link_url, loc, text or link_url)
                ctx.candidate_docs.append((link_url, text))
                continue
            if not self._same_site(link_url):
                continue
            if find_match(text, ctx.pattern):
                return self._record_match(ctx, link_url, "anchor_text", text)
            if take_all_pages or self._looks_interesting(link_url, text):
                ctx.candidate_pages.append(link_url)
        return False

    # ------------------------------------------------------------- pipeline

    def _expand_template(self, tpl: str) -> list[str]:
        """Turn one listing template into its ordered page URLs (page 1 first)."""
        if "{page}" not in tpl:
            return [urljoin(self.base_url, tpl)]
        # Page 1 is the bare listing: strip the pagination suffix.
        first = re.sub(r"page/\{page\}/?$", "", tpl)          # pretty permalinks  /x/page/N/
        first = re.sub(r"[?&]paged=\{page\}$", "", first)     # plain permalinks   ?paged=N
        urls = [urljoin(self.base_url, first)]
        urls += [urljoin(self.base_url, tpl.format(page=n)) for n in range(2, MAX_LISTING_PAGES + 1)]
        return urls

    async def _process_search(self, url: str, ctx: ScrapeContext) -> bool:
        """
        Site-search results page. The body is NOT matched (the CMS echoes the
        query); only result links are harvested, and every same-site link in
        the content region becomes a candidate page.
        """
        if url in ctx.visited:
            return False
        ctx.visited.add(url)
        html = await self.fetch_html(url, ctx)
        if html is None:
            return False
        soup = BeautifulSoup(html, "html.parser")
        scope = self.content_scope(soup)
        if _NO_RESULTS_RE.search(self.node_text(scope) if scope is not soup else soup.get_text(" ", strip=True)):
            logger.info("[%s] site search returned no results", self.institution)
            return False
        links = self.extract_links(scope, url)
        # Search-result links that point back at the search page or pagination are noise.
        links = [(u, t) for u, t in links if "?s=" not in u and "&s=" not in u]
        logger.info("[%s] site search returned %d links", self.institution, len(links))
        return self._harvest_links(links, ctx, take_all_pages=True)

    async def _process_listing(self, url: str, ctx: ScrapeContext) -> bool:
        """Fetch one listing page; record candidates; return True if matched."""
        if url in ctx.visited:
            return False
        ctx.visited.add(url)
        html = await self.fetch_html(url, ctx)
        if html is None:
            return False
        soup = BeautifulSoup(html, "html.parser")
        scope = self.content_scope(soup)
        if self._harvest_links(self.extract_links(scope, url), ctx, take_all_pages=False):
            return True
        snippet = find_match(self.node_text(scope), ctx.pattern)
        if snippet:
            return self._record_match(ctx, url, "listing_page", snippet)
        return False

    async def _process_listing_series(self, tpl: str, ctx: ScrapeContext) -> bool:
        """
        Walk one section's pages in order, stopping at the first page that
        cannot be fetched (end of pagination). Returns True on a match.
        """
        for url in self._expand_template(tpl):
            if url in ctx.visited:
                continue
            errors_before = len(ctx.result.errors)
            if await self._process_listing(url, ctx):
                return True
            if len(ctx.result.errors) > errors_before:
                break  # fetch failed (typically 404 past the last page)
        return False

    async def _process_article(self, url: str, ctx: ScrapeContext) -> bool:
        if url in ctx.visited:
            return False
        ctx.visited.add(url)
        html = await self.fetch_html(url, ctx)
        if html is None:
            return False
        soup = BeautifulSoup(html, "html.parser")
        scope = self.content_scope(soup)
        doc_links = [(u, t) for u, t in self.extract_links(scope, url) if is_document_url(u)]
        if self._harvest_links(doc_links, ctx, take_all_pages=False):
            return True
        snippet = find_match(self.node_text(scope), ctx.pattern)
        if snippet:
            title = soup.title.get_text(strip=True) if soup.title else ""
            ctx.result.metadata.setdefault("page_title", title)
            return self._record_match(ctx, url, "article_page", snippet)
        return False

    async def _process_document(self, url: str, anchor_text: str, ctx: ScrapeContext) -> bool:
        """Download a document; if it is a PDF, extract its text and match."""
        if url in ctx.visited:
            return False
        ctx.visited.add(url)
        if not _HAS_PYPDF:
            return False
        try:
            body, ctype = await self.fetch_bytes(url)
        except FetchError as exc:
            ctx.result.errors.append(str(exc))
            logger.warning("[%s] document fetch failed: %s", self.institution, exc)
            return False

        if body[:5] != b"%PDF-":
            # download.php endpoints can serve docx/xlsx/zip or an HTML error page.
            logger.debug("[%s] %s is not a PDF (%s, %d bytes) — skipped",
                         self.institution, url, ctype or "no content-type", len(body))
            ctx.result.metadata.setdefault("non_pdf_documents", 0)
            ctx.result.metadata["non_pdf_documents"] = int(ctx.result.metadata["non_pdf_documents"]) + 1  # type: ignore[arg-type]
            return False

        ctx.result.pdfs_checked += 1
        text = await asyncio.to_thread(self._pdf_text, body)
        if not text.strip():
            logger.debug("[%s] no extractable text in %s (scanned or malformed PDF)", self.institution, url)
            return False
        snippet = find_match(text, ctx.pattern)
        if snippet:
            ctx.result.metadata.setdefault("pdf_anchor_text", anchor_text)
            return self._record_match(ctx, url, "pdf_text", snippet, full_text=text)
        return False

    @staticmethod
    def _pdf_text(body: bytes) -> str:
        """Best-effort text extraction; never raises."""
        try:
            reader = PdfReader(io.BytesIO(body), strict=False)  # type: ignore[misc]
            if reader.is_encrypted:
                try:
                    reader.decrypt("")
                except Exception:  # noqa: BLE001
                    return ""
            chunks: list[str] = []
            for i, page in enumerate(reader.pages):
                if i >= MAX_PDF_PAGES_TO_READ:
                    break
                try:
                    chunks.append(page.extract_text() or "")
                except Exception as exc:  # noqa: BLE001 — one bad page must not sink the file
                    logger.debug("PDF page %d extraction failed: %s", i, exc)
            return "\n".join(chunks)
        except Exception as exc:  # noqa: BLE001
            logger.debug("PDF text extraction failed: %s", str(exc).splitlines()[0] if str(exc) else exc)
            return ""

    def _record_match(
        self, ctx: ScrapeContext, url: str, location: str, snippet: str, full_text: str = ""
    ) -> bool:
        ctx.result.found = True
        try:
            ctx.result.document_url = HttpUrl(url)
        except Exception:  # noqa: BLE001 — keep going even if URL is odd
            ctx.result.metadata["document_url_raw"] = url
        ctx.result.raw_text = (full_text or snippet)[: 20_000]
        ctx.result.evidence.append(MatchEvidence(location=location, url=url,
                                                 snippet=snippet[:RAW_TEXT_SNIPPET_CHARS]))
        logger.info("[%s] MATCH for %s via %s -> %s",
                    self.institution, ctx.result.decision_number, location, url)
        return True

    async def _run_batch(self, coros: Sequence, ctx: ScrapeContext) -> bool:
        """Run coroutines concurrently; stop early once any reports a match."""
        if not coros:
            return False
        tasks = [asyncio.ensure_future(c) for c in coros]
        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    if await fut:
                        return True
                except Exception as exc:  # noqa: BLE001
                    ctx.result.errors.append(repr(exc))
                    logger.exception("[%s] worker crashed: %s", self.institution, exc)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return False

    async def search(self, decision_number: str) -> ScraperResult:
        """Run the full pipeline. Never raises; failures land in result.errors."""
        ctx = ScrapeContext(
            pattern=build_decision_pattern(decision_number),
            result=ScraperResult(institution=self.institution, domain=self.domain,
                                 decision_number=decision_number),
        )
        logger.info("[%s] dispatched: decision %r on %s", self.institution, decision_number, self.base_url)
        try:
            # 1. Site search — links only, never the page body.
            if self.search_url_template:
                url = urljoin(self.base_url, self.search_url_template.format(query=quote_plus(decision_number)))
                if await self._process_search(url, ctx):
                    return ctx.result

            # 2. Listing pages (news + documents); sections run concurrently,
            #    pages within a section sequentially so pagination can stop early.
            sections = list(dict.fromkeys((*self.news_paths, *self.document_paths)))
            if await self._run_batch([self._process_listing_series(t, ctx) for t in sections], ctx):
                return ctx.result

            # 3. Candidate article pages
            pages = [u for u in dict.fromkeys(ctx.candidate_pages) if u not in ctx.visited][:MAX_ARTICLES_TO_VISIT]
            if await self._run_batch([self._process_article(u, ctx) for u in pages], ctx):
                return ctx.result

            # 4. Documents (PDF text)
            docs = list(dict.fromkeys(ctx.candidate_docs))[:MAX_PDFS_TO_DOWNLOAD]
            if docs and not _HAS_PYPDF:
                ctx.result.errors.append("pypdf not installed; PDF contents not searched")
                logger.warning("[%s] pypdf not installed — %d documents skipped", self.institution, len(docs))
            if await self._run_batch([self._process_document(u, t, ctx) for u, t in docs], ctx):
                return ctx.result

            logger.info("[%s] NOT FOUND: decision %r (pages=%d, pdfs=%d, errors=%d)",
                        self.institution, decision_number, ctx.result.pages_visited,
                        ctx.result.pdfs_checked, len(ctx.result.errors))
        except Exception as exc:  # noqa: BLE001 — last line of defence
            ctx.result.errors.append(f"scraper crashed: {exc!r}")
            logger.exception("[%s] scraper failed for %r", self.institution, decision_number)
        finally:
            ctx.result.metadata.update(
                {"candidate_pages": len(set(ctx.candidate_pages)),
                 "candidate_documents": len(set(ctx.candidate_docs)),
                 "pypdf_available": _HAS_PYPDF}
            )
        return ctx.result


# --------------------------------------------------------------------------- #
# Concrete scrapers
# --------------------------------------------------------------------------- #


class ParkingScraper(BaseScraper):
    """ЈППЈЛП Битола — WordPress/Elementor, pretty permalinks."""

    institution = "ЈППЈЛП Битола"
    aliases = ("паркинзи", "паркинг", "parking", "jppjlp", "јппјлп", "jpparkinzibitola")
    base_url = "https://jpparkinzibitola.mk/"
    news_paths = ("/",)  # news + announcements live on the homepage
    document_paths = ("/municipality-documents/", "/municipality-documents/page/{page}/")
    search_url_template = "/?s={query}"


class KomunalecScraper(BaseScraper):
    """
    ЈП Комуналец Битола — WordPress over plain HTTP.

    Documents are served by the download-attachments plugin
    (download.php?id=N), which `is_document_url` routes straight to the
    PDF extractor. Plain HTTP is enough; the Playwright fallback still kicks
    in automatically if a page comes back without links.
    """

    institution = "ЈП Комуналец Битола"
    aliases = ("комуналец", "komunalec", "komunalecbt")
    base_url = "http://www.komunalecbt.com.mk/"
    news_paths = ("/", "/page/{page}/")
    document_paths = ("/dokumenti/", "/documents/", "/odluki/", "/javni-nabavki/")
    search_url_template = "/?s={query}"
    requires_js = False


class NiskogradbaScraper(BaseScraper):
    """
    КЈП Нискоградба Битола — WordPress with *plain* permalinks on
    niskogradba.pelanet.mk. niskogradbabitola.mk is the billing portal and
    has no publications, so it is not scraped.
    """

    institution = "КЈП Нискоградба Битола"
    aliases = ("нискоградба", "niskogradba", "niskogradbabitola", "кјп нискоградба")
    base_url = "https://niskogradba.pelanet.mk/"
    news_paths = ("/", "/?paged={page}", "/?cat=29", "/?cat=29&paged={page}")
    document_paths = ("/?cat=28", "/?cat=28&paged={page}")
    search_url_template = "/?s={query}"


SCRAPERS: tuple[type[BaseScraper], ...] = (ParkingScraper, KomunalecScraper, NiskogradbaScraper)


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #


def _norm_name(name: str) -> str:
    n = unicodedata.normalize("NFKC", name).lower()
    n = re.sub(r"[\"'„“”«»().,\-–—/]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def resolve_scraper(institution_name: str) -> Optional[type[BaseScraper]]:
    """Map a free-text institution name (as written in a gazette) to a scraper class."""
    needle = _norm_name(institution_name)
    if not needle:
        return None
    best: Optional[tuple[int, type[BaseScraper]]] = None
    for cls in SCRAPERS:
        for key in (cls.institution, *cls.aliases):
            k = _norm_name(key)
            if k and (k in needle or needle in k):
                score = len(k)
                if best is None or score > best[0]:
                    best = (score, cls)
    return best[1] if best else None


async def dispatch_scraper(
    institution_name: str,
    decision_number: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> ScraperResult:
    """
    Entry point used by the Tier 2 orchestrator.

    Resolves the institution to a scraper and runs it. If the institution is
    unknown, returns a not-found result with an explanatory error instead of
    raising, so the Tier 3 FOIA step can still proceed.
    """
    cls = resolve_scraper(institution_name)
    if cls is None:
        logger.error("dispatch failed: no scraper registered for institution %r", institution_name)
        return ScraperResult(
            institution=institution_name,
            domain="",
            decision_number=decision_number,
            errors=[f"no scraper registered for institution {institution_name!r}"],
        )
    async with cls(session=session) as scraper:
        return await scraper.search(decision_number)


async def scrape_all(
    decision_number: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> list[ScraperResult]:
    """Run every registered scraper for one decision number, concurrently."""
    async def _one(cls: type[BaseScraper]) -> ScraperResult:
        async with cls(session=session) as scraper:
            return await scraper.search(decision_number)

    return list(await asyncio.gather(*(_one(c) for c in SCRAPERS)))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="LRCR Tier 2 targeted scraper")
    parser.add_argument("decision_number", help='e.g. "02-164/7"')
    parser.add_argument("-i", "--institution", help="Institution name; omit to run all scrapers")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # asyncio.run() already uses the Proactor loop on Windows (required by
    # Playwright), so no event-loop policy override is needed.

    async def _main() -> None:
        if args.institution:
            results = [await dispatch_scraper(args.institution, args.decision_number)]
        else:
            results = await scrape_all(args.decision_number)
        for r in results:
            print(json.dumps(r.model_dump(mode="json"), ensure_ascii=False, indent=2))

    asyncio.run(_main())
