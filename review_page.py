"""
HTML for GET /review: the queue of drafted FOIA e-mails waiting for a person.

Everything that comes from the pipeline (gazette text, LLM output, scraped pages)
is untrusted, so every value is passed through html.escape() and the page runs
under a strict Content-Security-Policy with a per-response nonce. Buttons are wired
up with addEventListener (inline handlers are blocked by the CSP) and call
POST /review/{id}/approve or /reject with the browser's existing Basic-auth login.
"""

from __future__ import annotations

from html import escape
from typing import Sequence

from persistence import ReviewItem

_CSS = """
:root { color-scheme: light dark; --bg:#f6f7f9; --card:#fff; --ink:#1b1f24; --mute:#5b6570; --line:#d8dde3;
        --ok:#1a7f37; --bad:#c62828; --warn:#9a6700; }
@media (prefers-color-scheme: dark) { :root { --bg:#14171b; --card:#1d2126; --ink:#e8eaed; --mute:#9aa3ad; --line:#333a42;
        --ok:#3fb950; --bad:#f47067; --warn:#d29922; } }
* { box-sizing: border-box; }
body { margin:0; padding:16px; background:var(--bg); color:var(--ink); font:15px/1.5 system-ui, sans-serif; }
main { max-width: 860px; margin: 0 auto; }
h1 { font-size: 1.3rem; margin: 0 0 4px; }
.sub { color: var(--mute); margin: 0 0 16px; }
.banner { padding: 10px 12px; border-radius: 8px; border:1px solid var(--line); margin-bottom: 16px; background: var(--card); }
.banner.warn { border-color: var(--warn); }
article { background: var(--card); border:1px solid var(--line); border-radius: 10px; padding: 14px 16px; margin-bottom: 16px; }
.meta { color: var(--mute); font-size: .88rem; margin: 2px 0; }
.tag { display:inline-block; padding:1px 8px; border-radius: 99px; border:1px solid var(--line); font-size:.78rem; }
.tag.failed { color: var(--bad); border-color: var(--bad); }
h2 { font-size: 1.05rem; margin: 0 0 6px; }
.field { margin-top: 10px; }
.label { font-size:.78rem; text-transform: uppercase; letter-spacing:.04em; color: var(--mute); }
pre { white-space: pre-wrap; word-break: break-word; margin: 4px 0 0; padding: 10px; border:1px solid var(--line);
      border-radius: 8px; font: 14px/1.5 ui-monospace, Menlo, Consolas, monospace; }
.err { color: var(--bad); }
.actions { display:flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; align-items:center; }
input[type=text] { flex: 1 1 220px; min-width: 0; padding: 8px 10px; border:1px solid var(--line); border-radius:8px;
                   background: transparent; color: inherit; font: inherit; }
button { padding: 8px 16px; border-radius: 8px; border: 0; font: inherit; font-weight: 600; cursor: pointer; color:#fff; }
button.approve { background: var(--ok); } button.reject { background: var(--bad); }
button:disabled { opacity: .5; cursor: default; }
#status { position: sticky; top: 8px; margin-bottom: 12px; padding: 10px 12px; border-radius: 8px; display:none; border:1px solid var(--line); background: var(--card); }
#status.show { display:block; }
.empty { text-align:center; color: var(--mute); padding: 48px 0; }
"""

_JS = """
const statusBox = document.getElementById('status');
function say(text, isError) {
  statusBox.textContent = text;
  statusBox.className = 'show' + (isError ? ' err' : '');
}
async function act(card, action) {
  const id = card.dataset.id;
  if (action === 'approve' && !confirm('Send this FOIA request to ' + card.dataset.to + ' now?')) return;
  if (action === 'reject' && !confirm('Reject this draft? It will not be drafted or sent again.')) return;
  const buttons = card.querySelectorAll('button');
  buttons.forEach(b => b.disabled = true);
  const note = card.querySelector('input[name=note]').value.trim();
  try {
    const res = await fetch('/review/' + encodeURIComponent(id) + '/' + action, {
      method: 'POST', credentials: 'same-origin',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({note: note || null}),
    });
    let data = {};
    try { data = await res.json(); } catch (e) {}
    if (!res.ok) {
      say('Draft #' + id + ': ' + (data.detail || ('HTTP ' + res.status)), true);
      buttons.forEach(b => b.disabled = false);
      return;
    }
    if (data.dry_run) {
      say('Draft #' + id + ': dry-run is ON, the message was only logged. Nothing was sent and the draft stays in the queue.', false);
      buttons.forEach(b => b.disabled = false);
      return;
    }
    say('Draft #' + id + ': ' + (action === 'approve' ? 'sent to ' + data.to : 'rejected') + '.', false);
    card.remove();
    if (!document.querySelector('article')) location.reload();
  } catch (e) {
    say('Draft #' + id + ': network error, nothing changed on screen. Reload to see the real state.', true);
    buttons.forEach(b => b.disabled = false);
  }
}
document.querySelectorAll('article').forEach(card => {
  card.querySelector('button.approve').addEventListener('click', () => act(card, 'approve'));
  card.querySelector('button.reject').addEventListener('click', () => act(card, 'reject'));
});
"""


def _card(item: ReviewItem) -> str:
    d = item.draft
    e = escape
    failed = d.status == "failed"
    source = " · ".join(filter(None, [
        f"gazette {e(item.gazette_id)}" if item.gazette_id else None,
        f"“{e(item.signal_title)}”" if item.signal_title else None,
    ]))
    parts = [
        f'<article data-id="{d.id}" data-to="{e(d.to_email or "", quote=True)}">',
        f'<h2>#{d.id} · {e(d.institution)} · decision {e(d.decision_number)} '
        f'<span class="tag{" failed" if failed else ""}">{"send failed, approve to retry" if failed else "pending review"}</span></h2>',
        f'<div class="meta">Drafted {e(d.created_at.strftime("%Y-%m-%d %H:%M"))} UTC'
        f'{" · " + source if source else ""}</div>',
    ]
    if failed and d.last_error:
        parts.append(f'<div class="meta err">Last send error: {e(d.last_error)}</div>')
    if item.expected_content:
        parts.append(f'<div class="field"><div class="label">What Gemini says the missing document contains (check this against the gazette)</div>'
                     f'<pre>{e(item.expected_content)}</pre></div>')
    parts += [
        f'<div class="field"><div class="label">To</div><pre>{e(d.to_email or "(none)")}</pre></div>',
        f'<div class="field"><div class="label">Subject</div><pre>{e(d.subject or "(none)")}</pre></div>',
        f'<div class="field"><div class="label">Body</div><pre>{e(d.body or "(none)")}</pre></div>',
        '<div class="actions">'
        '<input type="text" name="note" maxlength="500" placeholder="Optional note (kept in the audit trail)">'
        '<button class="approve" type="button">Approve &amp; send</button>'
        '<button class="reject" type="button">Reject</button></div>',
        '</article>',
    ]
    return "\n".join(parts)


def render_review_page(items: Sequence[ReviewItem], *, dry_run: bool, user: str, nonce: str) -> str:
    cards = "\n".join(_card(i) for i in items) if items else '<p class="empty">No drafts are waiting for review.</p>'
    banner = (
        '<div class="banner warn"><strong>Dry-run is ON.</strong> Approving a draft only logs it on the server; '
        'nothing is sent and the draft stays in this list. Set LRCR_EMAIL_DRY_RUN=false to send for real.</div>'
        if dry_run else
        '<div class="banner"><strong>Live mode.</strong> Approving a draft sends the e-mail immediately.</div>'
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>LRCR: FOIA review ({len(items)})</title>
<style nonce="{nonce}">{_CSS}</style></head>
<body><main>
<h1>FOIA drafts awaiting review ({len(items)})</h1>
<p class="sub">Signed in as {escape(user)}. Nothing is sent until you approve it.</p>
{banner}
<div id="status" role="status" aria-live="polite"></div>
{cards}
</main>
<script nonce="{nonce}">{_JS}</script>
</body></html>"""
