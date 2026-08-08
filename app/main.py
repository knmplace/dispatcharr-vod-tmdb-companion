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
    get_run_status,
    pause_reconcile_pass,
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


def _page(body):
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
</style>
</head>
<body>
<div class="topbar">
  <div class="brand">
    <img src="/static/logo.jpg" alt="VOD TMDB Reconciler logo">
    <h1>VOD TMDB Reconciler <span class="sub">Companion dashboard</span></h1>
  </div>
  <button id="theme-toggle" type="button" onclick="toggleTheme()">Dark mode</button>
</div>
{body}
<script>
(function() {{
  const stored = localStorage.getItem('theme');
  if (stored) document.documentElement.setAttribute('data-theme', stored);
  updateToggleLabel();
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


@app.get("/", response_class=HTMLResponse)
def dashboard():
    settings = load_settings()
    status = get_run_status() or {"running": False}
    mode_badge = (
        '<span class="badge dry">DRY RUN</span>' if settings["dry_run"]
        else '<span class="badge live">LIVE -- will write to your database</span>'
    )

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
  <form method="post" action="/pause"><button type="submit">Pause</button></form>
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

  if (data.running) {{
    badge.textContent = 'running';
    badge.className = 'badge running dot';
    body.innerHTML = renderProgress(data.progress);
  }} else if (data.error) {{
    badge.textContent = 'error';
    badge.className = 'badge live';
    body.innerHTML = `<div class="error-text">${{data.error}}</div>`;
    lastSample = null;
  }} else if (data.summary) {{
    badge.textContent = 'idle';
    badge.className = 'badge idle';
    body.innerHTML = `<div class="summary-text">${{data.summary}}</div>` + renderProgress(data.progress);
    lastSample = null;
  }} else {{
    badge.textContent = 'idle';
    badge.className = 'badge idle';
    body.innerHTML = '<div class="empty-status">No run has been started yet.</div>';
    lastSample = null;
  }}
}}
refreshStatus();
setInterval(refreshStatus, 3000);
</script>
"""
    return _page(body)


@app.post("/settings")
def update_settings(
    tmdb_api_key: str = Form(""),
    dry_run: str = Form("true"),
    auto_accept_confidence: int = Form(80),
    worker_threads: int = Form(8),
    max_rows: int = Form(0),
    test_write_limit: int = Form(0),
):
    save_settings(
        {
            "tmdb_api_key": tmdb_api_key,
            "dry_run": dry_run.lower() == "true",
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
