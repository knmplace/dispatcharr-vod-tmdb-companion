"""
Standalone companion service for Dispatcharr: fuzzy-resolves missing TMDB
ids for VOD Series/Movies and (eventually) consolidates duplicate rows
created when providers disagree on or omit a tmdb_id.

Successor to the dispatcharr-vod-tmdb-reconciler in-process PLUGIN -- moved
out of Dispatcharr's own gevent-patched uwsgi process so worker_threads maps
to real OS threads instead of cooperative greenlets (see reconciler.py's
module docstring). Deployed as a companion container in the same Docker
Compose stack as Dispatcharr, on the same network, so it reaches Postgres
via Docker's internal DNS -- no ports published, no assumption about
non-Docker Dispatcharr deployments (see project CLAUDE.md).
"""

import logging
import os

from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from django_bootstrap import setup_django

setup_django()

from reconciler import (  # noqa: E402  (must import after django.setup())
    clear_checkpoint,
    get_review_counts,
    get_run_status,
    list_review_items,
    list_review_items_paginated,
    pause_reconcile_pass,
    resolve_review_items,
    run_reconcile_pass,
)
from settings_store import load_settings, save_settings  # noqa: E402

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("vod_tmdb_companion")

app = FastAPI(title="VOD TMDB Reconciler Companion")
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

_BUILD_SHA = os.environ.get("BUILD_SHA", "unknown")[:7]


def _page(body, nav_extra=""):
    return f"""<html>
<head>
<title>VOD TMDB Reconciler Companion</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/jpeg" href="/static/logo.jpg">
<style>
  :root {{
    --bg: #f4f5f7; --card-bg: #fff; --fg: #1a1d23; --muted: #6b7280;
    --border: #e5e7eb; --input-bg: #fff; --accent: #4f46e5; --accent-fg: #fff;
    --badge-dry: #fef3c7; --badge-dry-fg: #92400e;
    --badge-live: #fee2e2; --badge-live-fg: #991b1b;
    --track: #e5e7eb; --bar: #4f46e5; --bar-done: #10b981;
    --ok: #059669; --warn: #d97706; --err: #dc2626;
    --shadow: 0 1px 2px rgba(0,0,0,.04), 0 1px 8px rgba(0,0,0,.04);
  }}
  :root[data-theme="dark"] {{
    --bg: #14161a; --card-bg: #1c1f26; --fg: #e5e7eb; --muted: #9aa1ac;
    --border: #2b2f38; --input-bg: #22252c; --accent: #6366f1; --accent-fg: #fff;
    --badge-dry: #3f2f0d; --badge-dry-fg: #fbbf24;
    --badge-live: #3f1414; --badge-live-fg: #f87171;
    --track: #2b2f38; --bar: #6366f1; --bar-done: #34d399;
    --ok: #34d399; --warn: #fbbf24; --err: #f87171;
    --shadow: 0 1px 2px rgba(0,0,0,.3), 0 4px 16px rgba(0,0,0,.3);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --bg: #14161a; --card-bg: #1c1f26; --fg: #e5e7eb; --muted: #9aa1ac;
      --border: #2b2f38; --input-bg: #22252c; --accent: #6366f1; --accent-fg: #fff;
      --badge-dry: #3f2f0d; --badge-dry-fg: #fbbf24;
      --badge-live: #3f1414; --badge-live-fg: #f87171;
      --track: #2b2f38; --bar: #6366f1; --bar-done: #34d399;
      --ok: #34d399; --warn: #fbbf24; --err: #f87171;
      --shadow: 0 1px 2px rgba(0,0,0,.3), 0 4px 16px rgba(0,0,0,.3);
    }}
  }}
  * {{ box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    max-width: 760px; margin: 0 auto; padding: 2rem 1.25rem 4rem;
    color: var(--fg); background: var(--bg);
  }}
  .topbar {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 1.75rem; }}
  .brand {{ display: flex; align-items: center; gap: 0.75rem; }}
  .brand img {{ width: 40px; height: 40px; border-radius: 10px; box-shadow: var(--shadow); }}
  h1 {{ font-size: 1.3rem; font-weight: 700; margin: 0; letter-spacing: -0.01em; }}
  h1 span.sub {{ display: block; font-size: 0.8rem; font-weight: 400; color: var(--muted); margin-top: 0.15rem; }}
  .card {{
    margin-bottom: 1.25rem; border: 1px solid var(--border); border-radius: 12px;
    background: var(--card-bg); box-shadow: var(--shadow); padding: 1.25rem 1.4rem;
  }}
  .card-title {{ font-size: 0.95rem; font-weight: 600; margin: 0 0 1rem; display: flex; align-items: center; gap: 0.5rem; }}
  label {{ display: block; margin: 0.9rem 0 0.3rem; font-weight: 600; font-size: 0.85rem; color: var(--muted); }}
  label:first-of-type {{ margin-top: 0; }}
  input[type=text], input[type=password], input[type=number], select {{
    width: 100%; padding: 0.55rem 0.65rem; box-sizing: border-box;
    background: var(--input-bg); color: var(--fg); border: 1px solid var(--border);
    border-radius: 8px; font-size: 0.9rem;
  }}
  input:focus, select:focus {{ outline: 2px solid var(--accent); outline-offset: 1px; }}
  .hint {{ color: var(--muted); font-size: 0.78rem; margin-top: 0.3rem; line-height: 1.4; }}
  .row {{ display: flex; gap: 0.6rem; margin-top: 1.1rem; flex-wrap: wrap; }}
  button {{
    padding: 0.55rem 1.1rem; cursor: pointer; font-size: 0.88rem; font-weight: 600;
    background: var(--input-bg); color: var(--fg); border: 1px solid var(--border);
    border-radius: 8px; transition: filter .1s ease;
  }}
  button:hover {{ filter: brightness(0.96); }}
  :root[data-theme="dark"] button:hover, .dark-hover {{ filter: brightness(1.15); }}
  button.primary {{ background: var(--accent); color: var(--accent-fg); border-color: var(--accent); }}
  #theme-toggle {{ font-size: 0.8rem; padding: 0.4rem 0.75rem; border-radius: 999px; }}
  .badge {{ display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.72rem; font-weight: 700; letter-spacing: 0.02em; text-transform: uppercase; }}
  .badge.dry {{ background: var(--badge-dry); color: var(--badge-dry-fg); }}
  .badge.live {{ background: var(--badge-live); color: var(--badge-live-fg); }}
  .badge.idle {{ background: var(--track); color: var(--muted); }}
  .badge.running {{ background: var(--badge-dry); color: var(--badge-dry-fg); }}
  .badge.pausing {{ background: var(--badge-live); color: var(--badge-live-fg); }}
  .badge.dot::before {{ content: "\\25CF"; margin-right: 0.35rem; font-size: 0.6rem; vertical-align: 1px; }}
  .progress-block {{ margin-bottom: 1.1rem; }}
  .progress-block:last-child {{ margin-bottom: 0; }}
  .progress-label {{ display: flex; justify-content: space-between; font-size: 0.82rem; margin-bottom: 0.35rem; }}
  .progress-label .kind {{ font-weight: 600; text-transform: capitalize; }}
  .progress-label .nums {{ color: var(--muted); font-variant-numeric: tabular-nums; }}
  .track {{ height: 8px; border-radius: 999px; background: var(--track); overflow: hidden; }}
  .bar {{ height: 100%; border-radius: 999px; background: var(--bar); transition: width .4s ease; }}
  .bar.done {{ background: var(--bar-done); }}
  .meta-row {{ display: flex; gap: 1.5rem; margin-top: 1rem; flex-wrap: wrap; font-size: 0.8rem; color: var(--muted); }}
  .meta-row b {{ color: var(--fg); font-variant-numeric: tabular-nums; }}
  .empty-status {{ color: var(--muted); font-size: 0.88rem; padding: 0.5rem 0; }}
  .summary-text {{ font-size: 0.86rem; line-height: 1.55; white-space: pre-wrap; }}
  .error-text {{ color: var(--err); font-size: 0.86rem; font-weight: 600; }}
  .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 0.85rem; }}
  .stat-card {{
    border: 1px solid var(--border); border-radius: 10px; padding: 0.9rem 1rem;
    background: var(--bg);
  }}
  .stat-card .stat-num {{ font-size: 1.5rem; font-weight: 700; font-variant-numeric: tabular-nums; }}
  .stat-card .stat-label {{ font-size: 0.82rem; font-weight: 600; margin-top: 0.15rem; }}
  .stat-card .stat-hint {{ font-size: 0.76rem; color: var(--muted); margin-top: 0.35rem; line-height: 1.4; }}
  .stat-card.ok .stat-num {{ color: var(--ok); }}
  .stat-card.warn .stat-num {{ color: var(--warn); }}
  .stat-card.err .stat-num {{ color: var(--err); }}
  .stat-card a.stat-action {{ display: inline-block; margin-top: 0.5rem; font-size: 0.78rem; font-weight: 600; color: var(--accent); text-decoration: none; }}
  .stat-card a.stat-action:hover {{ text-decoration: underline; }}
  .nav-link {{ font-size: 0.85rem; font-weight: 600; color: var(--accent); text-decoration: none; }}
  .nav-link:hover {{ text-decoration: underline; }}
  .poster-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 1rem; }}
  .review-row {{
    border: 1px solid var(--border); border-radius: 10px; padding: 0.9rem;
    background: var(--bg);
  }}
  .review-row-head {{ display: flex; align-items: flex-start; gap: 0.5rem; margin-bottom: 0.7rem; }}
  .review-row-head input[type=checkbox] {{ margin-top: 0.25rem; width: 16px; height: 16px; }}
  .review-row-title {{ font-weight: 700; font-size: 0.92rem; }}
  .review-row-sub {{ font-size: 0.78rem; color: var(--muted); }}
  .candidates {{ display: flex; gap: 0.6rem; flex-wrap: wrap; margin-bottom: 0.6rem; }}
  .candidate {{
    width: 84px; cursor: pointer; border: 2px solid transparent; border-radius: 8px;
    padding: 3px; text-align: center;
  }}
  .candidate.selected {{ border-color: var(--accent); }}
  .candidate img {{
    width: 100%; aspect-ratio: 2/3; object-fit: cover; border-radius: 6px;
    background: var(--track); display: block;
  }}
  .candidate .cand-title {{ font-size: 0.68rem; margin-top: 0.25rem; line-height: 1.25; color: var(--fg); }}
  .candidate .cand-conf {{ font-size: 0.68rem; color: var(--muted); }}
  .manual-id-row {{ display: flex; gap: 0.4rem; align-items: center; }}
  .manual-id-row input[type=text] {{ width: 110px; padding: 0.35rem 0.5rem; font-size: 0.8rem; }}
  .review-toolbar {{
    position: sticky; top: 0; z-index: 5; display: flex; gap: 0.6rem; align-items: center;
    flex-wrap: wrap; background: var(--card-bg); border: 1px solid var(--border);
    border-radius: 10px; padding: 0.75rem 1rem; margin-bottom: 1.25rem; box-shadow: var(--shadow);
  }}
  .review-toolbar input[type=text] {{ width: 130px; }}
  .conflict-note {{ font-size: 0.8rem; color: var(--muted); }}
  .kind-heading {{ font-size: 1rem; font-weight: 700; margin: 1.5rem 0 0.75rem; }}
  .kind-heading:first-child {{ margin-top: 0; }}
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">
    <img src="/static/logo.jpg" alt="VOD TMDB Reconciler logo">
    <h1>VOD TMDB Reconciler <span class="sub">Companion dashboard &middot; build {_BUILD_SHA}</span></h1>
  </div>
  <div class="row" style="margin-top:0;">
    {nav_extra}
    <button id="theme-toggle" type="button" onclick="toggleTheme()">Dark mode</button>
  </div>
</div>
{body}
<button id="back-to-top" class="primary" type="button" onclick="window.scrollTo({{top: 0, behavior: 'smooth'}})"
  style="display:none;position:fixed;bottom:1.5rem;right:1.5rem;width:3rem;height:3rem;border-radius:50%;
  z-index:100;font-size:1.25rem;line-height:1;cursor:pointer;">&uarr;</button>
<script>
(function() {{
  const stored = localStorage.getItem('theme');
  if (stored) document.documentElement.setAttribute('data-theme', stored);
  updateToggleLabel();
}})();
(function() {{
  const btn = document.getElementById('back-to-top');
  window.addEventListener('scroll', function() {{
    btn.style.display = window.scrollY > 400 ? 'block' : 'none';
  }});
}})();
function toggleTheme() {{
  const current = document.documentElement.getAttribute('data-theme')
    || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  const next = current === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  updateToggleLabel();
}}
function updateToggleLabel() {{
  const current = document.documentElement.getAttribute('data-theme')
    || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  document.getElementById('theme-toggle').textContent = current === 'dark' ? 'Light mode' : 'Dark mode';
}}
</script>
</body>
</html>"""


def _fmt_num(n):
    return f"{n:,}"


def _stat_card(count, label, hint, tone, action_href=None, action_label=None):
    action = (
        f'<a class="stat-action" href="{action_href}">{action_label}</a>'
        if action_href and count else ""
    )
    return f"""<div class="stat-card {tone}">
      <div class="stat-num">{_fmt_num(count)}</div>
      <div class="stat-label">{label}</div>
      <div class="stat-hint">{hint}</div>
      {action}
    </div>"""


def _build_stat_cards():
    counts = get_review_counts()
    series, movie = counts["series"], counts["movie"]
    auto_total = 0  # auto-matched rows aren't tracked in review counts (already handled)
    review_total = series["review"] + movie["review"]
    conflict_total = series["conflicts"] + movie["conflicts"]
    no_result_total = series["no_result"] + movie["no_result"]

    cards = [
        _stat_card(
            review_total, "Needs your review",
            "TMDB found a possible match, but the confidence score was too low to trust automatically. "
            "Pick the right one (or type a TMDB ID) on the review page.",
            "warn", "/review#review", "Review now →",
        ),
        _stat_card(
            no_result_total, "No TMDB match found",
            "TMDB had nothing matching this title/year at all -- usually a typo, wrong year, or an obscure/regional title. "
            "You can search TMDB yourself and enter the correct ID.",
            "warn", "/review#no_result", "Enter ID manually →",
        ),
        _stat_card(
            conflict_total, "Duplicate-row conflicts",
            "Your library has two separate entries for the same title (one provider supplied a TMDB ID, another didn't). "
            "Fixing this needs a safe merge tool that isn't built yet -- tracked, not lost.",
            "err",
        ),
    ]
    return '<div class="stat-grid">' + "".join(cards) + "</div>"


@app.get("/", response_class=HTMLResponse)
def dashboard():
    settings = load_settings()
    status = get_run_status() or {"running": False}
    mode_badge = (
        '<span class="badge dry">DRY RUN</span>' if settings["dry_run"]
        else '<span class="badge live">LIVE -- will write to your database</span>'
    )
    stat_cards_html = _build_stat_cards()
    nav_extra = '<a class="nav-link" href="/review">Manual review →</a>'

    body = f"""
<div class="card">
<div class="card-title">Settings</div>
<form method="post" action="/settings">
  <label for="tmdb_api_key">TMDB API key</label>
  <input type="password" id="tmdb_api_key" name="tmdb_api_key" value="{settings['tmdb_api_key']}" autocomplete="off">
  <div class="hint">Free key from themoviedb.org/settings/api</div>

  <label for="dry_run">Mode</label>
  <select id="dry_run" name="dry_run">
    <option value="true" {"selected" if settings["dry_run"] else ""}>Dry run (report only, no writes)</option>
    <option value="false" {"selected" if not settings["dry_run"] else ""}>Live (writes resolved tmdb_id to the database)</option>
  </select>

  <label for="scope">Scope</label>
  <select id="scope" name="scope">
    <option value="both" {"selected" if settings.get("scope", "both") == "both" else ""}>Series + Movies</option>
    <option value="series" {"selected" if settings.get("scope", "both") == "series" else ""}>Series only</option>
    <option value="movies" {"selected" if settings.get("scope", "both") == "movies" else ""}>Movies only</option>
  </select>
  <div class="hint">Limits a run to just one kind -- useful for re-scanning movies without re-touching series that already finished, or vice versa.</div>

  <label for="auto_accept_confidence">Auto-accept confidence threshold (0-100)</label>
  <input type="number" id="auto_accept_confidence" name="auto_accept_confidence" value="{settings['auto_accept_confidence']}" min="0" max="100">

  <label for="worker_threads">Worker threads</label>
  <input type="number" id="worker_threads" name="worker_threads" value="{settings['worker_threads']}" min="1">

  <label for="max_rows">Max rows per pass (0 = no limit)</label>
  <input type="number" id="max_rows" name="max_rows" value="{settings['max_rows']}" min="0">

  <label for="test_write_limit">Test write limit (0 = no limit, live mode only)</label>
  <input type="number" id="test_write_limit" name="test_write_limit" value="{settings['test_write_limit']}" min="0">
  <div class="hint">Caps how many rows a LIVE pass will actually write, so you can verify a small batch before a full run.</div>

  <div class="row"><button type="submit" class="primary">Save settings</button></div>
</form>
</div>

<div class="card">
<div class="card-title">Run {mode_badge}</div>
<div class="row">
  <form method="post" action="/run"><button type="submit" class="primary">Run reconcile pass</button></form>
  <form method="post" action="/pause"><button id="pause-btn" type="submit">Pause</button></form>
  <form method="post" action="/clear-checkpoint"
        onsubmit="return confirm('Clear checkpoint? The next run will do a full fresh scan instead of resuming.');">
    <button type="submit">Clear checkpoint</button>
  </form>
</div>
</div>

<div class="card">
<div class="card-title">
  Status
  <span id="status-badge" class="badge idle">idle</span>
</div>
<div id="status-body"><div class="empty-status">Loading...</div></div>
</div>

<div class="card">
<div class="card-title">What this means</div>
{stat_cards_html}
</div>

<script>
let lastSample = null;

function fmtNum(n) {{ return n.toLocaleString(); }}

function fmtEta(seconds) {{
  if (!isFinite(seconds) || seconds <= 0) return null;
  if (seconds < 60) return Math.ceil(seconds) + 's';
  if (seconds < 3600) return Math.round(seconds / 60) + 'm';
  return (seconds / 3600).toFixed(1) + 'h';
}}

function renderProgress(progress) {{
  const now = Date.now() / 1000;
  let rateNote = '';
  if (lastSample && progress) {{
    const dt = now - lastSample.t;
    let doneDelta = 0;
    for (const kind in progress) {{
      const prevKind = lastSample.progress[kind];
      if (prevKind) doneDelta += (progress[kind].done - prevKind.done);
    }}
    if (dt > 0 && doneDelta > 0) {{
      const rate = doneDelta / dt;
      let remaining = 0;
      for (const kind in progress) {{
        remaining += Math.max(0, progress[kind].total - progress[kind].done);
      }}
      const eta = fmtEta(remaining / rate);
      rateNote = `<div class="meta-row"><span>Rate: <b>${{rate.toFixed(1)}}/s</b></span>` +
        (eta ? `<span>Est. remaining: <b>${{eta}}</b></span>` : '') + `</div>`;
    }}
  }}
  lastSample = {{ t: now, progress: progress || {{}} }};

  let bars = '';
  for (const kind in (progress || {{}})) {{
    const p = progress[kind];
    const pct = p.total > 0 ? Math.min(100, (p.done / p.total) * 100) : 0;
    const isDone = p.total > 0 && p.done >= p.total;
    bars += `
      <div class="progress-block">
        <div class="progress-label">
          <span class="kind">${{kind}}</span>
          <span class="nums">${{fmtNum(p.done)}} / ${{fmtNum(p.total)}} (${{pct.toFixed(0)}}%)</span>
        </div>
        <div class="track"><div class="bar ${{isDone ? 'done' : ''}}" style="width:${{pct}}%"></div></div>
      </div>`;
  }}
  return bars + rateNote;
}}

async function refreshStatus() {{
  const res = await fetch('/status');
  const data = await res.json();
  const badge = document.getElementById('status-badge');
  const body = document.getElementById('status-body');

  const pauseBtn = document.getElementById('pause-btn');
  if (data.running) {{
    if (data.pause_requested) {{
      badge.textContent = 'pausing…';
      badge.className = 'badge pausing dot';
      pauseBtn.disabled = true;
      pauseBtn.textContent = 'Pausing…';
    }} else {{
      badge.textContent = 'running';
      badge.className = 'badge running dot';
      pauseBtn.disabled = false;
      pauseBtn.textContent = 'Pause';
    }}
    body.innerHTML = renderProgress(data.progress);
  }} else if (data.error) {{
    badge.textContent = 'error';
    badge.className = 'badge live';
    pauseBtn.disabled = false;
    pauseBtn.textContent = 'Pause';
    body.innerHTML = `<div class="error-text">${{data.error}}</div>`;
    lastSample = null;
  }} else if (data.summary) {{
    badge.textContent = 'idle';
    badge.className = 'badge idle';
    pauseBtn.disabled = false;
    pauseBtn.textContent = 'Pause';
    body.innerHTML = `<div class="summary-text">${{data.summary}}</div>` + renderProgress(data.progress);
    lastSample = null;
  }} else {{
    badge.textContent = 'idle';
    badge.className = 'badge idle';
    pauseBtn.disabled = false;
    pauseBtn.textContent = 'Pause';
    body.innerHTML = '<div class="empty-status">No run has been started yet.</div>';
    lastSample = null;
  }}
}}
refreshStatus();
setInterval(refreshStatus, 3000);
</script>
"""
    return _page(body, nav_extra=nav_extra)


@app.post("/settings")
def update_settings(
    tmdb_api_key: str = Form(""),
    dry_run: str = Form("true"),
    scope: str = Form("both"),
    auto_accept_confidence: int = Form(80),
    worker_threads: int = Form(8),
    max_rows: int = Form(0),
    test_write_limit: int = Form(0),
):
    save_settings(
        {
            "tmdb_api_key": tmdb_api_key,
            "dry_run": dry_run.lower() == "true",
            "scope": scope if scope in ("both", "series", "movies") else "both",
            "auto_accept_confidence": auto_accept_confidence,
            "worker_threads": worker_threads,
            "max_rows": max_rows,
            "test_write_limit": test_write_limit,
        }
    )
    return RedirectResponse("/", status_code=303)


@app.get("/status")
def status():
    return get_run_status() or {"running": False}


@app.post("/run")
def run():
    run_reconcile_pass(load_settings(), logger)
    return RedirectResponse("/", status_code=303)


@app.post("/pause")
def pause():
    pause_reconcile_pass()
    return RedirectResponse("/", status_code=303)


@app.post("/clear-checkpoint")
def clear_checkpoint_endpoint():
    clear_checkpoint()
    return RedirectResponse("/", status_code=303)


_TMDB_POSTER_BASE = "https://image.tmdb.org/t/p/w154"


def _candidate_html(kind, row_id, candidates):
    if not candidates:
        return '<div class="conflict-note">TMDB had no results for this title -- no candidates to pick from. Enter a TMDB ID manually below if you know it.</div>'
    tiles = []
    for i, cand in enumerate(candidates):
        poster = cand.get("poster_path")
        img_src = f"{_TMDB_POSTER_BASE}{poster}" if poster else ""
        if img_src:
            img_tag = f'<img src="{img_src}" alt="">'
        else:
            img_tag = (
                '<div class="candidate-noimg" style="width:100%;aspect-ratio:2/3;'
                'background:var(--track);border-radius:6px;display:flex;align-items:center;'
                'justify-content:center;font-size:0.6rem;color:var(--muted);text-align:center;'
                'padding:0.25rem;">no poster (scanned before poster support -- rerun to refresh)</div>'
            )
        selected_cls = " selected" if len(candidates) == 1 else ""
        tiles.append(f"""<div class="candidate{selected_cls}" data-tmdb-id="{cand.get('tmdb_id')}"
             onclick="selectCandidate(this, '{kind}', {row_id})">
          {img_tag}
          <div class="cand-title">{cand.get('name') or '?'} ({cand.get('year') or '?'})</div>
          <div class="cand-conf">{cand.get('confidence', 0):.0f}% match &middot; TMDB #{cand.get('tmdb_id')}</div>
        </div>""")
    return '<div class="candidates">' + "".join(tiles) + "</div>"


def _review_row_html(kind, entry, bucket):
    row_id = entry["id"]
    name = entry.get("name") or "(untitled)"
    year = entry.get("year") or "?"
    candidates = entry.get("top_5") or []
    row_key = f"{kind}:{row_id}"
    checkbox = (
        f'<input type="checkbox" class="row-check" data-kind="{kind}" data-id="{row_id}">'
        if bucket != "conflicts" else ""
    )
    conflict_note = ""
    if bucket == "conflicts":
        conflict_note = (
            f'<div class="conflict-note">This title already exists as row id={entry.get("conflicting_row_id")} '
            f'in your library. Merging duplicate rows isn\'t supported yet (tracked separately) -- no action available here.</div>'
        )
    matched_kind = entry.get("matched_kind")
    mismatch_note = ""
    if matched_kind and matched_kind != kind:
        mismatch_note = (
            f'<div class="conflict-note">No {kind} results on TMDB for this title -- these candidates were found '
            f'by searching TMDB as a {matched_kind} instead. Your library likely has this title mis-tagged as {kind}; '
            f'double-check before applying.</div>'
        )
    single_match = bucket != "conflicts" and len(candidates) == 1
    prefill_id = candidates[0].get("tmdb_id") if single_match else ""
    manual_row = "" if bucket == "conflicts" else f"""
      <div class="manual-id-row">
        <label style="margin:0;font-weight:400;color:var(--muted);">or TMDB ID:</label>
        <input type="text" class="manual-id" data-kind="{kind}" data-id="{row_id}" placeholder="e.g. 603" value="{prefill_id}">
      </div>"""

    return f"""<div class="review-row" data-row-key="{row_key}" data-single-match="{'1' if single_match else '0'}">
      <div class="review-row-head">
        {checkbox}
        <div>
          <div class="review-row-title">{name}</div>
          <div class="review-row-sub">({year}) &middot; {kind}</div>
        </div>
      </div>
      {_candidate_html(kind, row_id, candidates) if bucket != "conflicts" else ""}
      {mismatch_note}
      {conflict_note}
      {manual_row}
    </div>"""


_BUCKET_LABELS = {
    "review": ("Needs your review", "Low-confidence matches -- pick the right poster or enter a TMDB ID."),
    "no_result": ("No TMDB match found", "TMDB had nothing matching this title/year. Enter the correct TMDB ID if you know it."),
    "conflicts": ("Duplicate-row conflicts", "Already exist elsewhere in your library under a different row -- needs a merge tool (not built yet), no action here."),
}


@app.get("/review", response_class=HTMLResponse)
def review_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/review/series?outcome=review&page=1", status_code=302)


def _render_review_section(kind, outcome, page, per_page=500):
    """Render a single paginated review section."""
    data = list_review_items_paginated(kind, outcome, page, per_page)
    items = data["items"]
    total = data["total"]
    pages = data["pages"]
    current_page = data["page"]

    rows_html = [_review_row_html(kind, entry, outcome) for entry in items]

    if not rows_html:
        return f'<div class="card"><div class="empty-status">No {outcome.replace("_", " ")} items for {kind}s.</div></div>', 0

    label, hint = _BUCKET_LABELS.get(outcome, (outcome, ""))

    pagination_html = ""
    if pages > 1:
        page_links = []
        for p in range(1, min(pages + 1, 8)):
            if p == current_page:
                page_links.append(f'<span style="font-weight:700;color:var(--fg);">{p}</span>')
            else:
                page_links.append(
                    f'<a href="/review/{kind}?outcome={outcome}&page={p}" style="color:var(--accent);text-decoration:none;">{p}</a>'
                )
        if pages > 7:
            page_links.append(f'<span style="color:var(--muted);">... <a href="/review/{kind}?outcome={outcome}&page={pages}" '
                            f'style="color:var(--accent);text-decoration:none;">last ({pages})</a></span>')
        pagination_html = f'<div style="margin-top:1.5rem;padding-top:1rem;border-top:1px solid var(--border);text-align:center;font-size:0.9rem;">' \
                         f'Page {current_page} of {pages} &middot; ' + ' '.join(page_links) + '</div>'

    return f"""<div id="{outcome}" class="card">
      <div class="card-title">{label} <span class="badge idle">{total}</span></div>
      <div class="hint" style="margin:-0.5rem 0 1rem;">{hint}</div>
      <div class="poster-grid">{''.join(rows_html)}</div>
      {pagination_html}
    </div>""", total


@app.get("/review/{kind}", response_class=HTMLResponse)
def review_page_paginated(kind: str, outcome: str = "review", page: int = 1):
    if kind not in ("series", "movie"):
        kind = "series"
    if outcome not in ("review", "no_result", "conflicts"):
        outcome = "review"

    settings = load_settings()
    section_html, total = _render_review_section(kind, outcome, page, per_page=500)

    # Tab navigation
    tabs = []
    for kind_name in ("series", "movie"):
        for outcome_name in ("review", "no_result"):
            counts = get_review_counts()
            count = counts[kind_name][outcome_name]
            is_active = kind_name == kind and outcome_name == outcome
            active_style = "font-weight:700;color:var(--accent);border-bottom:3px solid var(--accent);" if is_active else ""
            tabs.append(
                f'<a href="/review/{kind_name}?outcome={outcome_name}&page=1" '
                f'style="padding:0.6rem 1rem;text-decoration:none;color:var(--fg);{active_style}">'
                f'{kind_name.title()} - {outcome_name.replace("_", " ").title()} ({count})</a>'
            )

    mode_note = (
        "Selections will be written immediately (dry_run is OFF)."
        if not settings["dry_run"]
        else "dry_run is ON -- resolving here will simulate the write and mark the row done, but nothing is saved to your database yet. Turn off dry_run on the main dashboard first if you want this to actually stick."
    )

    body = f"""
<div style="display:flex;gap:0;border-bottom:1px solid var(--border);margin-bottom:1.5rem;overflow-x:auto;">
  {''.join(tabs)}
</div>

<div class="review-toolbar">
  <span id="selected-count">0 selected</span>
  <button onclick="selectAllSingleMatches()">Select all single matches</button>
  <button onclick="unselectAll()">Unselect all</button>
  <button class="primary" onclick="applyPicked()">Apply chosen TMDB IDs</button>
  <span class="hint" style="margin:0;">Uses whatever TMDB ID you already picked or typed on each checked row.</span>
  <span style="width:1px;align-self:stretch;background:var(--border,#333);"></span>
  <input type="text" id="bulk-tmdb-id" placeholder="or: TMDB ID for ALL selected">
  <button onclick="applyBulk()">Apply same ID to all selected</button>
  <span class="hint" style="margin:0;">{mode_note}</span>
</div>

{section_html}
<div id="resolve-result"></div>

<script>
function selectCandidate(el, kind, rowId) {{
  const row = el.closest('.review-row');
  row.querySelectorAll('.candidate').forEach(c => c.classList.remove('selected'));
  el.classList.add('selected');
  const manualInput = row.querySelector('.manual-id');
  if (manualInput) manualInput.value = el.dataset.tmdbId;
}}

document.querySelectorAll('.row-check').forEach(cb => cb.addEventListener('change', updateSelectedCount));
function updateSelectedCount() {{
  const n = document.querySelectorAll('.row-check:checked').length;
  document.getElementById('selected-count').textContent = n + ' selected';
}}

function selectAllSingleMatches() {{
  document.querySelectorAll('.review-row[data-single-match="1"]').forEach(row => {{
    const cb = row.querySelector('.row-check');
    if (cb) cb.checked = true;
  }});
  updateSelectedCount();
}}

function unselectAll() {{
  document.querySelectorAll('.row-check:checked').forEach(cb => cb.checked = false);
  updateSelectedCount();
}}

function collectSelections(bulkTmdbId) {{
  const selections = [];
  document.querySelectorAll('.review-row').forEach(row => {{
    const cb = row.querySelector('.row-check');
    const manualInput = row.querySelector('.manual-id');
    if (!manualInput) return; // conflicts have no action
    const [kind, id] = row.dataset.rowKey.split(':');
    let tmdbId = null;
    if (bulkTmdbId && cb && cb.checked) {{
      tmdbId = bulkTmdbId;
    }} else if (!bulkTmdbId && manualInput.value.trim()) {{
      tmdbId = manualInput.value.trim();
    }} else {{
      const picked = row.querySelector('.candidate.selected');
      if (!bulkTmdbId && picked) tmdbId = picked.dataset.tmdbId;
    }}
    if (tmdbId) selections.push({{kind, id: parseInt(id, 10), tmdb_id: tmdbId}});
  }});
  return selections;
}}

async function applyBulk() {{
  const bulkId = document.getElementById('bulk-tmdb-id').value.trim();
  if (!bulkId) {{ alert('Enter a TMDB ID to apply to the selected rows.'); return; }}
  const selections = collectSelections(bulkId);
  if (!selections.length) {{ alert('Check at least one row first.'); return; }}
  await submitResolve(selections);
}}

async function applyPicked() {{
  const checked = document.querySelectorAll('.row-check:checked');
  if (!checked.length) {{ alert('Check at least one row first.'); return; }}
  const selections = [];
  const missing = [];
  checked.forEach(cb => {{
    const row = cb.closest('.review-row');
    const manualInput = row.querySelector('.manual-id');
    const [kind, id] = row.dataset.rowKey.split(':');
    const tmdbId = manualInput ? manualInput.value.trim() : '';
    if (tmdbId) {{
      selections.push({{kind, id: parseInt(id, 10), tmdb_id: tmdbId}});
    }} else {{
      missing.push(row.querySelector('.review-row-title').textContent);
    }}
  }});
  if (missing.length) {{
    alert('These checked rows have no poster picked or ID typed yet, so they were skipped:\\n' + missing.join('\\n'));
  }}
  if (!selections.length) return;
  await submitResolve(selections);
}}

document.querySelectorAll('.review-row .manual-id').forEach(input => {{
  input.addEventListener('keydown', async (e) => {{
    if (e.key !== 'Enter') return;
    e.preventDefault();
    const row = input.closest('.review-row');
    const [kind, id] = row.dataset.rowKey.split(':');
    if (!input.value.trim()) return;
    await submitResolve([{{kind, id: parseInt(id, 10), tmdb_id: input.value.trim()}}]);
  }});
}});

async function submitResolve(selections) {{
  const res = await fetch('/review/resolve', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{selections}}),
  }});
  const data = await res.json();
  const el = document.getElementById('resolve-result');
  if (data.skipped && data.skipped.length) {{
    el.innerHTML = `<div class="error-text">Wrote ${{data.written}}. Skipped ${{data.skipped.length}}: ` +
      data.skipped.map(s => `${{s.kind}}:${{s.id}} (${{s.reason}})`).join('; ') + `</div>`;
  }} else {{
    el.innerHTML = `<div class="summary-text">Wrote ${{data.written}} row(s). Reloading…</div>`;
  }}
  setTimeout(() => location.reload(), 1200);
}}
</script>
"""
    return _page(body, nav_extra='<a class="nav-link" href="/">← Dashboard</a>')


@app.post("/review/resolve")
def review_resolve(payload: dict):
    settings = load_settings()
    selections = payload.get("selections", [])
    written, skipped = resolve_review_items(selections, settings["dry_run"], logger)
    return {"written": written, "skipped": skipped}
