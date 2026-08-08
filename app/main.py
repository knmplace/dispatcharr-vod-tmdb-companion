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


def _page(body):
    return f"""<html>
<head>
<title>VOD TMDB Reconciler Companion</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; color: #222; }}
  h1 {{ font-size: 1.4rem; }}
  fieldset {{ margin-bottom: 1.5rem; border: 1px solid #ccc; border-radius: 6px; }}
  label {{ display: block; margin: 0.6rem 0 0.2rem; font-weight: 600; }}
  input[type=text], input[type=password], input[type=number] {{ width: 100%; padding: 0.4rem; box-sizing: border-box; }}
  .hint {{ color: #666; font-size: 0.85rem; margin-top: 0.15rem; }}
  .row {{ display: flex; gap: 0.5rem; margin-top: 1rem; }}
  button {{ padding: 0.5rem 1rem; cursor: pointer; }}
  .status {{ background: #f5f5f5; padding: 1rem; border-radius: 6px; white-space: pre-wrap; font-family: monospace; font-size: 0.85rem; }}
  .badge {{ display: inline-block; padding: 0.1rem 0.5rem; border-radius: 4px; font-size: 0.8rem; }}
  .badge.dry {{ background: #fff3cd; }}
  .badge.live {{ background: #f8d7da; }}
</style>
</head>
<body>
<h1>VOD TMDB Reconciler Companion</h1>
{body}
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
<fieldset>
<legend>Settings</legend>
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

  <div class="row"><button type="submit">Save settings</button></div>
</form>
</fieldset>

<fieldset>
<legend>Run {mode_badge}</legend>
<div class="row">
  <form method="post" action="/run"><button type="submit">Run reconcile pass</button></form>
  <form method="post" action="/pause"><button type="submit">Pause</button></form>
  <form method="post" action="/clear-checkpoint"
        onsubmit="return confirm('Clear checkpoint? The next run will do a full fresh scan instead of resuming.');">
    <button type="submit">Clear checkpoint</button>
  </form>
</div>
</fieldset>

<fieldset>
<legend>Status</legend>
<div class="status" id="status">{status}</div>
</fieldset>

<script>
async function refreshStatus() {{
  const res = await fetch('/status');
  const data = await res.json();
  document.getElementById('status').textContent = JSON.stringify(data, null, 2);
}}
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
