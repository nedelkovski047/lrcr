# LRCR

LRCR monitors official gazettes for regulatory changes, checks whether the documents those changes refer to are publicly available, and, when a document cannot be found online, drafts a Freedom of Information (FOIA) request for a person to review. **Nothing is e-mailed without a human approving it.**

<img width="960" height="1602" alt="review-page" src="https://github.com/user-attachments/assets/107ab129-bdb6-4611-9468-b070f1cbd5e9" />

## How it works

1. **Polling.** A scheduled job (APScheduler) looks for new gazette PDFs.
2. **Extraction.** Each PDF is parsed and the Gemini API converts it into a Pydantic-validated schema of regulatory changes and the documents they reference.
3. **Verification.** Async scrapers (aiohttp, BeautifulSoup, Playwright) search the relevant institutions' websites for each referenced document. The result distinguishes "not found" from "could not check".
4. **Drafting.** If a document cannot be found, a formal FOIA request is generated and queued.
5. **Human review.** A person approves or rejects each draft on a protected web page (`/review`). Only approval sends an e-mail (via Resend).
6. **Retries.** A retry worker re-attempts failed sends with backoff.

LLM output can be wrong, so by design the system can produce a draft but never an unreviewed e-mail.

## Security design

- HTTP Basic authentication on **every** route, including `/docs` and `/healthz`. Credentials are compared in constant time, and the server **fails closed**: if `ADMIN_USER` or `ADMIN_PASSWORD` is missing or weak, every request gets a 503.
- Cross-site request blocking: state-changing requests that a browser marks as cross-site (`Origin` differs from `Host`, `Origin: null`, or `Sec-Fetch-Site: cross-site`) are refused with 403. This matters because browsers resend cached Basic credentials to cross-site requests.
- The review page is served with a strict, nonce-based Content-Security-Policy (`default-src 'none'`, scripts and styles only with a per-response nonce, `frame-ancestors 'none'`) and has no inline event handlers.
- All text that comes from the pipeline (gazette text, LLM output, scraped pages) is HTML-escaped before it is shown.
- Dry-run mode (`LRCR_EMAIL_DRY_RUN`): approving a draft only logs the message instead of sending it.

## Project layout

| File | Purpose |
|---|---|
| `main.py` | FastAPI app and routes |
| `security.py` | Authentication and cross-site request middleware |
| `poller.py` | Scheduled polling for new gazettes |
| `pdf_parser.py`, `schemas.py` | PDF parsing and the validated data schema |
| `scrapers.py` | Async scrapers that check document availability |
| `foia_generator.py` | Builds the FOIA request text |
| `review_page.py` | HTML for the human review queue |
| `mailer.py`, `retry_worker.py` | Sending e-mail and retrying failures |
| `database.py`, `models.py`, `persistence.py` | Database engine, tables, and queries |
| `Dockerfile`, `render.yaml` | Container image and Render deployment |

## Configuration

Set these as environment variables. `.env.example` lists the names with placeholder values. **Never commit real values.**

| Variable | Purpose |
|---|---|
| `ADMIN_USER`, `ADMIN_PASSWORD` | Login for the web interface. The password must be at least 12 characters. Required. |
| `GEMINI_API_KEY` | Gemini API key. Required. |
| `RESEND_API_KEY` | Resend API key, needed to send e-mail. |
| `DATABASE_URL` / `LRCR_DATABASE_URL` | Database connection string. |
| `LRCR_EMAIL_DRY_RUN` | Log messages instead of sending them. Keep it on while testing. |
| `LRCR_MAIL_FROM`, `LRCR_REPLY_TO`, `LRCR_SENDER_NAME`, `LRCR_SENDER_EMAIL`, `LRCR_SENDER_ADDRESS`, `LRCR_SENDER_PHONE` | Sender details used in the drafted requests. |
| `LRCR_POLL_ENABLED`, `LRCR_POLL_INTERVAL_HOURS`, `LRCR_POLL_GAZETTE_DELAY_S`, `LRCR_POLL_MAX_FAILURES`, `LRCR_POLL_MAX_NEW_PER_RUN`, `LRCR_POLL_MIN_YEAR`, `LRCR_POLL_STARTUP_DELAY_S`, `LRCR_GAZETTE_INDEX_URL` | Polling schedule and limits. |
| `LRCR_RETRY_ENABLED`, `LRCR_RETRY_INTERVAL_MINUTES`, `LRCR_RETRY_BACKOFF_MINUTES`, `LRCR_RETRY_MAX_ATTEMPTS`, `LRCR_RETRY_MAX_PER_PASS`, `LRCR_RETRY_STARTUP_DELAY_S` | Retry worker settings. |
| `LRCR_GEMINI_MODEL`, `LRCR_MAX_CONCURRENT_SCRAPES` | Model choice and scraping concurrency. |
| `LRCR_DB_POOL_SIZE`, `LRCR_DB_MAX_OVERFLOW`, `LRCR_DB_POOL_RECYCLE_S`, `LRCR_DB_CONNECT_TIMEOUT_S`, `LRCR_SQL_ECHO` | Database pool tuning and SQL logging. |

Defaults for the optional settings are defined in the code.

## Run locally (Windows PowerShell)

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m playwright install chromium

$env:ADMIN_USER = "admin"
$env:ADMIN_PASSWORD = "use-a-long-random-password"
$env:GEMINI_API_KEY = "your-key"
$env:LRCR_EMAIL_DRY_RUN = "true"
$env:LRCR_POLL_ENABLED = "false"

uvicorn main:app --reload
```

Then open <http://127.0.0.1:8000/review> and sign in with the admin credentials. With polling disabled and dry-run on, the app makes no Gemini calls and sends no e-mail.

## Tests

```powershell
python -m pip install pytest
python -m pytest -q
```

The tests currently cover the authentication and cross-site middleware and the escaping in the review page.

## Deployment

The app is deployed to Render from the `Dockerfile`. The image is based on Microsoft's Playwright image because Playwright's Chromium needs system libraries that Render's native Python runtime does not provide. The container runs as an unprivileged user, and the build fails on purpose if the Playwright version pinned in `requirements.txt` differs from the one in the image.

The hosted instance is currently stopped; use the local setup above.
