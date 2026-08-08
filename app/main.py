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

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from django_bootstrap import setup_django

setup_django()

from reconciler import (  # noqa: E402  (must import after django.setup())
    clear_checkpoint,
    get_run_status,
    pause_reconcile_pass,
    run_reconcile_pass,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("vod_tmdb_companion")

app = FastAPI(title="VOD TMDB Reconciler Companion")


def _settings_from_env():
    return {
        "tmdb_api_key": os.environ.get("TMDB_API_KEY", ""),
        "auto_accept_confidence": os.environ.get("AUTO_ACCEPT_CONFIDENCE", "80"),
        "dry_run": os.environ.get("DRY_RUN", "true").lower() == "true",
        "max_rows": os.environ.get("MAX_ROWS", "0"),
        "worker_threads": os.environ.get("WORKER_THREADS", "8"),
        "test_write_limit": os.environ.get("TEST_WRITE_LIMIT", "0"),
    }


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return "<html><body><h1>VOD TMDB Reconciler Companion</h1><p>Dashboard coming soon -- see /status for now.</p></body></html>"


@app.get("/status")
def status():
    return get_run_status() or {"running": False}


@app.post("/run")
def run():
    message = run_reconcile_pass(_settings_from_env(), logger)
    return {"message": message}


@app.post("/pause")
def pause():
    return {"message": pause_reconcile_pass()}


@app.post("/clear-checkpoint")
def clear_checkpoint_endpoint():
    return {"message": clear_checkpoint()}
