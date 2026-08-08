# Dispatcharr VOD TMDB Companion

A standalone Docker companion service for [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr)
that fuzzy-resolves missing TMDB ids for VOD Series and Movies, and
(eventually) consolidates duplicate rows created when IPTV providers
disagree on or omit a `tmdb_id`.

## What it does

Dispatcharr's VOD library is built from provider M3U/Xtream catalogs. Not
every provider supplies a correct (or any) `tmdb_id` for a Series or Movie,
which leaves gaps in metadata and can create duplicate rows for the same
title under different provider listings. This tool:

1. Scans Dispatcharr's Postgres database (via Django's own ORM, using
   Dispatcharr's own models) for Series/Movie rows missing a `tmdb_id`.
2. Fuzzy-matches each one against [TMDB](https://www.themoviedb.org/)'s
   search API using title/year heuristics.
3. Above a configurable confidence threshold, writes the resolved
   `tmdb_id` back to the row (or reports what it *would* write, in
   dry-run mode).
4. Detects and flags rows where multiple provider entries actually refer
   to the same title, as a precursor to future automatic merge/consolidate
   support.

It exposes a small FastAPI HTTP interface (`/`, `/status`, `/run`,
`/pause`, `/clear-checkpoint`) so a run can be triggered, monitored, and
paused without a shell into the container.

## Why standalone, not a Dispatcharr plugin

Dispatcharr's own web process runs under uwsgi with gevent monkey-patching
enabled. Under gevent, `ThreadPoolExecutor` threads become cooperative
greenlets multiplexed onto a small pool of real OS threads — so a
Dispatcharr *plugin* using a thread pool for concurrent TMDB lookups gets
no real concurrency gain no matter how many worker threads it requests.
This was confirmed by inspecting the running process's actual OS thread
count, which stayed fixed regardless of the plugin's configured worker
count.

Running as a separate, plain Python process outside of Dispatcharr's
gevent-patched runtime sidesteps that ceiling entirely — real OS-thread
concurrency for the TMDB matching workload — at the cost of needing its
own small Docker Compose service alongside Dispatcharr, rather than
living inside Dispatcharr's plugin system.

## How it reaches Dispatcharr's data

This container's image is built `FROM` Dispatcharr's own published image,
copying out Dispatcharr's Django app tree (`apps.*`) and settings module.
At startup it bootstraps Django directly (`django.setup()`) against
Dispatcharr's own `dispatcharr.settings`, so it uses the exact same ORM
models Dispatcharr itself uses — no reimplementation of the schema, and
automatic compatibility with however Dispatcharr's own models evolve
(as long as they're vendored back in via a rebuild against a newer base
image).

It connects to Dispatcharr's Postgres database directly, over the same
Docker network Dispatcharr's own stack already uses — never through a
published port. See `docker-compose.example.yml` for the network-join
pattern.

One notable implementation detail: Dispatcharr's settings module chooses
its Postgres backend based on a heuristic keyed off the running process's
identity — a gevent-dependent connection-pooling backend for uwsgi/daphne
processes, a plain blocking backend for Celery workers. This tool
presents itself as a Celery-like process (see `app/django_bootstrap.py`)
specifically to get routed to the plain backend, since it doesn't have
(and doesn't want) a gevent-patched runtime.

## App structure

```
dispatcharr-vod-tmdb-companion/
├── Dockerfile                  — multi-stage build (see below)
├── requirements.txt            — fastapi, uvicorn[standard]
├── docker-compose.example.yml  — example service to add to your own stack
└── app/
    ├── django_bootstrap.py     — standalone django.setup() against
    │                              Dispatcharr's own settings/app tree
    ├── main.py                 — FastAPI app: /, /status, /run, /pause,
    │                              /clear-checkpoint
    └── reconciler.py           — scan / fuzzy-match / conflict-detect /
                                   backfill-write logic
```

## Built with

- **Python 3.13**, [FastAPI](https://fastapi.tiangolo.com/) +
  [uvicorn](https://www.uvicorn.org/) for the HTTP interface.
- **Django ORM**, borrowed directly from Dispatcharr's own codebase —
  this tool ships no models of its own.
- **Docker multi-stage build**: the final image is `python:3.13-slim`,
  with `/app` (Dispatcharr's Django project) and `/dispatcharrpy`
  (Dispatcharr's own Python runtime/site-packages) copied in from
  `ghcr.io/dispatcharr/dispatcharr:latest` in an earlier build stage.
  Dispatcharr's bundled runtime ships no `pip` of its own (a slim
  production image with dependencies baked in at Dispatcharr's build
  time), so this tool's own dependencies (FastAPI/uvicorn) are installed
  into the final stage's own site-packages instead, with `PYTHONPATH`
  merging both locations so a single interpreter sees everything.
- **GitHub Actions**, building and publishing the image to GHCR (GitHub
  Container Registry) on every push to `main` and on tags — see
  `.github/workflows/docker-publish.yml`.

## Deploying

This is not a standalone service you run in isolation — it needs to sit
on the same Docker network as an existing Dispatcharr deployment to reach
its Postgres database. See `docker-compose.example.yml` for a fully
commented example service block to add to your own Dispatcharr Compose
stack, including how to find your Dispatcharr network name and match
Postgres credentials to your existing Dispatcharr environment.

Recommended first run: `DRY_RUN=true` (the default), so you can review
proposed TMDB matches before anything is written back to your database.

## Status

Early stage. TMDB backfill (scan → fuzzy-match → write) is implemented
and has been run successfully against a live Dispatcharr database.
Duplicate-row consolidation (merging Series/Movie rows that turn out to
represent the same title under different provider listings) is designed
but not yet implemented — see code comments in `reconciler.py` for the
current state.

## License

MIT — see `LICENSE`.
