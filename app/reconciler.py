"""
Core reconcile pass: scans Dispatcharr's own Series/Movie tables for rows
missing a tmdb_id/imdb_id, fuzzy-matches them against TMDB, and (eventually)
consolidates duplicate rows created when providers disagree on or omit a
tmdb_id.

Ported from the dispatcharr-vod-tmdb-reconciler PLUGIN (in-process, ran
inside Dispatcharr's own gevent-patched uwsgi workers -- which capped real
concurrency regardless of worker_threads, since ThreadPoolExecutor threads
become cooperative greenlets under gevent's monkey-patch). This standalone
companion runs as its own container/process, so worker_threads here maps to
real OS threads and TMDB throughput actually scales with it.

Confidence scoring (_calculate_tmdb_confidence) and the TMDB /search query
shape (_search_tmdb) were originally ported from dispatcharr-vod-plex-bridge-
plugin's bridge.py per bead dispatcharr-vod-plex-bridge-plugin-47l's
explicit instruction to reuse that pattern rather than reinvent it.
"""

import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger("vod_tmdb_reconciler")

# Shared session so concurrent TMDB /search calls reuse pooled HTTPS
# connections instead of each request paying its own TCP+TLS handshake --
# without this, raising worker_threads barely moves throughput since the
# handshake cost (not TMDB_SEARCH_DELAY_SECS or TMDB's rate limit) dominates
# per-request latency. Pool sized well above any realistic worker_threads
# setting so threads never block waiting on a pooled connection.
_tmdb_session = requests.Session()
_tmdb_session.mount(
    "https://", HTTPAdapter(pool_connections=64, pool_maxsize=64)
)

AUTO_ACCEPT_DEFAULT = 75  # confidence threshold: >= 75% auto-accept, < 75% leave for review
TMDB_SEARCH_DELAY_SECS = 0.25  # be polite to TMDB's rate limit, applied per-worker
DEFAULT_WORKER_THREADS = 2  # user-tunable via 'worker_threads' setting -- each worker
# sleeps TMDB_SEARCH_DELAY_SECS between its own calls, so N workers gives roughly
# N / TMDB_SEARCH_DELAY_SECS requests/sec in aggregate. TMDB's v3 key limit is ~50
# req/s -- that caps worker_threads at ~12 (12 / 0.25 = 48/s) before requests start
# getting 429'd. Left as a setting rather than a fixed value so it can be tuned
# down further if the key is shared with other tools, but going meaningfully
# above ~12 risks throttling, not just faster scans.
TMDB_MAX_SAFE_WORKER_THREADS = 12

# File-based (not in-memory) run-state/pause-signal/checkpoint, same
# rationale as the plugin this was ported from: any process restart or
# multi-process deployment must be able to see current state. Kept
# file-based here too even though this container runs a single process --
# it's what makes a container restart mid-pass resumable, and costs nothing.
_DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(_DATA_DIR, exist_ok=True)
_CHECKPOINT_PATH = os.path.join(_DATA_DIR, "reconcile_checkpoint.json")
_RUN_STATE_PATH = os.path.join(_DATA_DIR, "reconcile_run_state.json")
_PAUSE_FLAG_PATH = os.path.join(_DATA_DIR, "reconcile_pause.flag")
_file_lock = threading.Lock()  # only guards this process's own concurrent writes

# Simple mtime-based cache to avoid re-parsing huge checkpoint files
_checkpoint_cache = {}
_checkpoint_mtime = None


def _atomic_write_json(path, data):
    with _file_lock:
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)


def _load_checkpoint():
    """Load checkpoint with mtime-based caching to avoid re-parsing huge files."""
    global _checkpoint_cache, _checkpoint_mtime

    if not os.path.exists(_CHECKPOINT_PATH):
        _checkpoint_cache = {}
        _checkpoint_mtime = None
        return {}

    try:
        current_mtime = os.path.getmtime(_CHECKPOINT_PATH)
        if _checkpoint_mtime == current_mtime and _checkpoint_cache:
            return _checkpoint_cache

        with open(_CHECKPOINT_PATH, "r") as f:
            _checkpoint_cache = json.load(f)
            _checkpoint_mtime = current_mtime
            return _checkpoint_cache
    except (json.JSONDecodeError, OSError):
        _checkpoint_cache = {}
        _checkpoint_mtime = None
        return {}


def _save_checkpoint(checkpoint):
    _atomic_write_json(_CHECKPOINT_PATH, checkpoint)


def clear_checkpoint():
    """Deletes the checkpoint file, if any -- forces the next run to do a
    full fresh scan instead of resuming. Called by the 'clear_checkpoint'
    action."""
    try:
        os.remove(_CHECKPOINT_PATH)
    except FileNotFoundError:
        pass
    return "Checkpoint cleared -- next run will start a fresh full scan."


def backfill_checkpoint_confidence(threshold=AUTO_ACCEPT_DEFAULT):
    """Reclassify checkpoint entries based on new confidence threshold.
    Entries with confidence >= threshold become 'auto', < threshold become 'review'.
    Only affects entries with an 'entry' record containing a best_match confidence."""
    checkpoint = _load_checkpoint()
    if not checkpoint:
        return "No checkpoint found."

    changed = 0
    for key, record in checkpoint.items():
        entry = record.get("entry")
        if not entry:
            continue

        best_match = entry.get("best_match", {})
        confidence = best_match.get("confidence", 0)

        if confidence >= threshold:
            new_outcome = "auto"
        else:
            new_outcome = "review"

        old_outcome = record.get("outcome")
        if old_outcome != new_outcome and old_outcome not in ("resolved",):
            record["outcome"] = new_outcome
            changed += 1

    _save_checkpoint(checkpoint)
    global _checkpoint_cache, _checkpoint_mtime
    _checkpoint_cache = checkpoint
    _checkpoint_mtime = os.path.getmtime(_CHECKPOINT_PATH) if os.path.exists(_CHECKPOINT_PATH) else None

    return f"Checkpoint backfill complete: {changed} entries reclassified to new {threshold}% threshold."


def fix_conflicts_in_checkpoint():
    """Re-detect conflicts in existing 'auto' entries and mark them as 'conflicts'.

    During the initial scan, entries are marked 'auto' before conflict detection runs.
    This function loads the checkpoint, re-detects conflicts, and updates their outcome.
    Safe to run multiple times (idempotent)."""
    from apps.vod.models import Series, Movie

    checkpoint = _load_checkpoint()
    if not checkpoint:
        return "No checkpoint found."

    # Gather all 'auto' entries by kind
    auto_series = []
    auto_movies = []
    for key, record in checkpoint.items():
        if record.get("outcome") != "auto":
            continue
        entry = record.get("entry")
        if not entry:
            continue
        kind, row_id = key.split(":", 1)
        if kind == "series":
            auto_series.append(entry)
        elif kind == "movie":
            auto_movies.append(entry)

    # Detect conflicts
    _, series_conflicts = detect_conflicts(auto_series, "series")
    _, movies_conflicts = detect_conflicts(auto_movies, "movie")

    # Update checkpoint to mark conflicts
    changed = 0
    for entry in series_conflicts:
        row_id = entry["id"]
        key = f"series:{row_id}"
        if key in checkpoint:
            checkpoint[key]["outcome"] = "conflicts"
            changed += 1
    for entry in movies_conflicts:
        row_id = entry["id"]
        key = f"movie:{row_id}"
        if key in checkpoint:
            checkpoint[key]["outcome"] = "conflicts"
            changed += 1

    _save_checkpoint(checkpoint)
    global _checkpoint_cache, _checkpoint_mtime
    _checkpoint_cache = checkpoint
    _checkpoint_mtime = os.path.getmtime(_CHECKPOINT_PATH) if os.path.exists(_CHECKPOINT_PATH) else None

    return f"Conflict detection complete: {changed} entries reclassified as conflicts."


# If a container restart kills the background scan thread mid-pass, nothing
# ever gets a chance to write running=False -- the state file would say
# "running" forever otherwise, permanently blocking Run Reconcile after every
# restart. Progress updates touch a heartbeat on every tick (every row, well
# under a second apart even at worker_threads=1), so a heartbeat older than
# this is proof the process that owned it is gone, not just between ticks.
STALE_RUN_HEARTBEAT_SECS = 120


def _load_run_state():
    if not os.path.exists(_RUN_STATE_PATH):
        return {"running": False}
    try:
        with open(_RUN_STATE_PATH, "r") as f:
            state = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"running": False}

    if state.get("running") and (time.time() - state.get("heartbeat", 0)) > STALE_RUN_HEARTBEAT_SECS:
        state["running"] = False
        state["error"] = "Previous pass was interrupted (container restart?) -- state reset."
    return state


def _save_run_state(state):
    state["heartbeat"] = time.time()
    _atomic_write_json(_RUN_STATE_PATH, state)


def get_run_status():
    state = _load_run_state()
    if state.get("running"):
        state["pause_requested"] = _pause_requested()
    return state


def get_review_counts():
    """Read-only. Count-only summary without loading all items (fast).
    Scans checkpoint once, tallies by outcome/kind."""
    checkpoint = _load_checkpoint()
    counts = {
        "series": {"review": 0, "conflicts": 0, "no_result": 0},
        "movie": {"review": 0, "conflicts": 0, "no_result": 0},
    }

    series_auto_count = 0
    movies_auto_count = 0

    for key, record in checkpoint.items():
        kind, row_id = key.split(":", 1)
        outcome = record.get("outcome")
        if outcome == "resolved":
            continue

        if outcome in ("review", "no_result", "conflicts"):
            if kind in counts:
                counts[kind][outcome] = counts[kind].get(outcome, 0) + 1
        elif outcome == "auto":
            if kind == "series":
                series_auto_count += 1
            elif kind == "movie":
                movies_auto_count += 1

    return counts


def _pause_requested():
    return os.path.exists(_PAUSE_FLAG_PATH)


def _clear_pause_flag():
    try:
        os.remove(_PAUSE_FLAG_PATH)
    except FileNotFoundError:
        pass


def pause_reconcile_pass():
    """Signals the currently running pass (in whichever worker process owns
    it) to stop after its in-flight rows finish, by creating a flag file any
    worker process can see. Progress up to that point is already in the
    checkpoint file (rows are saved as they complete, not just at the end),
    so this is safe to call at any time."""
    state = _load_run_state()
    if not state.get("running"):
        return "No reconcile pass is currently running."
    with open(_PAUSE_FLAG_PATH, "w") as f:
        f.write("")
    return "Pause requested -- in-flight rows will finish, then the pass will stop. Progress is saved; click Run Reconcile again to resume."


def scan_missing_ids():
    """Read-only. Returns (series_missing, movies_missing) -- lists of dicts
    with the minimal fields needed for a fuzzy TMDB search: id, name, year.
    A row counts as "missing" if it has neither tmdb_id nor imdb_id -- that's
    the exact condition under which Dispatcharr's own name+year import
    fallback (lookup_by_name_year) can silently fork a duplicate row, per the
    root-cause investigation in bead dispatcharr-vod-plex-bridge-plugin-47l."""
    from apps.vod.models import Series, Movie

    series_missing = list(
        Series.objects.filter(tmdb_id__isnull=True, imdb_id__isnull=True)
        .values("id", "name", "year")
    )
    movies_missing = list(
        Movie.objects.filter(tmdb_id__isnull=True, imdb_id__isnull=True)
        .values("id", "name", "year")
    )
    return series_missing, movies_missing


def _calculate_tmdb_confidence(title, year, tmdb_result):
    """Ported verbatim (scoring weights unchanged) from
    dispatcharr-vod-plex-bridge-plugin/bridge.py's
    _calculate_tmdb_confidence. Hybrid score: popularity + vote weighting +
    name/year match bonuses. Returns 0-100."""
    if not isinstance(tmdb_result, dict):
        return 0

    base_score = 50.0

    popularity = tmdb_result.get("popularity", 0)
    base_score += min(20, (popularity / 100) * 20)

    vote_count = tmdb_result.get("vote_count", 0)
    vote_avg = tmdb_result.get("vote_average", 0)
    if vote_count > 50:
        base_score += 10
    if vote_avg >= 7.0:
        base_score += 5

    result_title_key = "name" if "name" in tmdb_result else "title"
    tmdb_title = (tmdb_result.get(result_title_key) or "").lower()
    clean_title = title.lower().strip()
    if tmdb_title == clean_title:
        base_score += 15
    elif tmdb_title.startswith(clean_title) or clean_title.startswith(tmdb_title):
        base_score += 10

    date_key = "first_air_date" if "first_air_date" in tmdb_result else "release_date"
    tmdb_date_str = tmdb_result.get(date_key, "")
    if tmdb_date_str and year:
        try:
            tmdb_year = int(tmdb_date_str.split("-")[0])
            year_int = int(year)
            if tmdb_year == year_int:
                base_score += 10
            elif abs(tmdb_year - year_int) == 1:
                base_score += 5
        except (ValueError, IndexError):
            pass

    return min(100, base_score)


# Some providers (confirmed live: WOBO, WarpTV) bake the release year
# straight into the Series/Movie `name` field instead of populating the
# separate `year` column -- e.g. "Richie Rich - 1994" or "Triple Oh! (2024)"
# with year=None. Searching TMDB with that literal string returns zero
# results even for well-known titles, which is why an early test pass
# showed an 86-88% "no TMDB results" rate on rows from those providers.
# This strips the embedded year out of the title and, if the row's own
# `year` field is empty, recovers it from what was stripped.
_TRAILING_YEAR_RE = re.compile(r"^(.*?)[\s\-:]*[\(\[]?((?:19|20)\d{2})[\)\]]?\s*$")


def _clean_title_and_year(name, year):
    match = _TRAILING_YEAR_RE.match(name.strip())
    if not match:
        return name.strip(), year

    cleaned = match.group(1).strip(" -:([").strip()
    embedded_year = match.group(2)
    if not cleaned:
        return name.strip(), year

    return cleaned, year or embedded_year


TMDB_RATE_LIMIT_MAX_RETRIES = 3


def _search_tmdb(kind, title, year, api_key):
    """Query TMDB /search/tv or /search/movie. Returns results sorted by
    confidence, descending. `kind` is 'series' or 'movie'.

    A 429 (rate limited) is retried with backoff rather than treated as "no
    results" -- without this, a throttled request would silently land the
    row in the no_result bucket, indistinguishable from TMDB genuinely
    having nothing for that title."""
    if not api_key:
        return []

    endpoint = "tv" if kind == "series" else "movie"
    year_param = "first_air_date_year" if kind == "series" else "primary_release_year"

    url = f"https://api.themoviedb.org/3/search/{endpoint}"
    params = {
        "api_key": api_key,
        "query": title,
        year_param: str(year) if year else None,
    }
    params = {k: v for k, v in params.items() if v}

    for attempt in range(TMDB_RATE_LIMIT_MAX_RETRIES + 1):
        try:
            response = _tmdb_session.get(url, params=params, timeout=5)
            if response.status_code == 429:
                if attempt >= TMDB_RATE_LIMIT_MAX_RETRIES:
                    logger.error(f"TMDB rate limit exceeded for '{title}' ({year}, {kind}) after {attempt} retries, giving up")
                    return []
                retry_after = float(response.headers.get("Retry-After", 1.0))
                logger.warning(f"TMDB rate limited on '{title}' ({year}, {kind}), retrying in {retry_after}s")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            scored = []
            for result in results:
                confidence = _calculate_tmdb_confidence(title, year, result)
                result_title = result.get("name") if kind == "series" else result.get("title")
                result_date = result.get("first_air_date") if kind == "series" else result.get("release_date")
                scored.append({
                    "tmdb_id": result.get("id"),
                    "name": result_title,
                    "year": (result_date or "")[:4],
                    "confidence": confidence,
                    "poster_path": result.get("poster_path"),
                })

            scored.sort(key=lambda x: x["confidence"], reverse=True)
            return scored
        except Exception as e:
            logger.error(f"TMDB search error for '{title}' ({year}, {kind}): {e}")
        return []


def _search_tmdb_with_fallback(kind, title, year, api_key):
    """Wraps _search_tmdb with a cross-endpoint retry: some providers
    mis-tag movies as series (or vice versa) in Dispatcharr's VOD library,
    so searching only the declared `kind`'s endpoint returns zero results
    even though TMDB has the title under the other endpoint (confirmed
    live: "A Dangerous Boy" is a movie on TMDB but stored as a Series row
    here). If the primary search comes back empty, try the opposite
    endpoint and tag every result with found_as so the review UI can flag
    the mismatch instead of silently presenting it as a normal match."""
    results = _search_tmdb(kind, title, year, api_key)
    if results:
        return results, kind

    other_kind = "movie" if kind == "series" else "series"
    fallback_results = _search_tmdb(other_kind, title, year, api_key)
    if fallback_results:
        for r in fallback_results:
            r["found_as"] = other_kind
        return fallback_results, other_kind

    return [], kind


def fuzzy_match_tmdb(rows, kind, api_key, auto_accept_threshold=AUTO_ACCEPT_DEFAULT,
                      progress_cb=None, worker_threads=DEFAULT_WORKER_THREADS,
                      row_done_cb=None, pause_check=None):
    """Read-only. For each row (dict with id/name/year), searches TMDB and
    buckets the best result into 'auto' (>= threshold) or 'review' (< threshold
    or no results). Does not touch the database. Returns
    (auto_matches, review_matches) -- lists of dicts combining the source row
    with its best TMDB candidate.

    progress_cb(kind, done, total), if given, is called after every row so a
    long pass can report where it's at (see reconcile_run_state.json in
    run_reconcile_pass) -- without this, a multi-hour run is
    indistinguishable from a hung one.

    row_done_cb(kind, row_id, outcome, entry), if given, is called after
    every row completes (outcome is 'auto', 'review', or 'no_result') --
    used by _do_run_reconcile_pass to persist each result to the checkpoint
    file incrementally, so a paused/killed run doesn't lose already-scanned
    rows.

    pause_check, if given, is a zero-arg callable checked before each row is
    submitted -- once it returns True, no new rows are submitted (in-flight
    ones still finish) and the function returns early with whatever was
    completed so far. (File-based, not a threading.Event -- Dispatcharr runs
    multiple worker processes, so pause state must be visible across
    processes, not just across threads in this one.)

    Rows are farmed out to worker_threads threads via ThreadPoolExecutor --
    each row is one TMDB /search call, so this is I/O-bound (network wait,
    not CPU), which is exactly what benefits from concurrency here. Each
    worker still sleeps TMDB_SEARCH_DELAY_SECS between its own calls to stay
    polite to TMDB's rate limit; see DEFAULT_WORKER_THREADS."""
    total = len(rows)
    progress_lock = threading.Lock()
    done_count = 0

    def _process(row):
        nonlocal done_count
        name = row.get("name")
        if not name:
            return None

        search_title, search_year = _clean_title_and_year(name, row.get("year"))

        time.sleep(TMDB_SEARCH_DELAY_SECS)
        results, matched_kind = _search_tmdb_with_fallback(kind, search_title, search_year, api_key)

        with progress_lock:
            done_count += 1
            i = done_count
        if progress_cb:
            progress_cb(kind, i, total)

        if not results:
            if row_done_cb:
                row_done_cb(kind, row["id"], "no_result", None)
            return None

        best = results[0]
        entry = {
            "id": row["id"],
            "name": name,
            "year": row.get("year"),
            "search_title": search_title,
            "best_match": best,
            "top_5": results[:5],
            "matched_kind": matched_kind,
        }
        outcome = "auto" if best["confidence"] >= auto_accept_threshold else "review"
        if row_done_cb:
            row_done_cb(kind, row["id"], outcome, entry)
        return entry

    auto_matches = []
    review_matches = []
    worker_threads = max(1, int(worker_threads or 1))

    # ThreadPoolExecutor.map() eagerly drains its iterable to submit all
    # futures up front rather than pulling one row at a time as workers free
    # up -- a pause_check() inside a generator passed to map() only stops
    # the *next* map() call, not the one already in flight, so a pause could
    # land after tens of thousands of rows were already queued (confirmed
    # live: progress kept climbing for a full minute-plus after pause was
    # requested).
    #
    # Submitting futures individually does NOT fix this by itself:
    # ThreadPoolExecutor.submit() has no bound on its internal work queue, so
    # a tight `for row in rows: pool.submit(...)` loop still queues all
    # tens-of-thousands of rows in well under a second -- submission is
    # essentially instantaneous compared to the TMDB call each one triggers,
    # so pause_check() (evaluated once per submission) never gets a chance to
    # land before every row is already queued (confirmed live: progress kept
    # climbing for minutes after pause was requested even after switching to
    # per-row submit()). Fixed by capping the number of outstanding
    # (submitted but not yet completed) futures to roughly worker_threads --
    # submission then proceeds at the same pace as completion, so
    # pause_check() is re-evaluated frequently enough to actually stop new
    # submissions promptly.
    max_in_flight = worker_threads * 2
    with ThreadPoolExecutor(max_workers=worker_threads) as pool:
        pending = set()
        row_iter = iter(rows)
        exhausted = False

        while True:
            while not exhausted and len(pending) < max_in_flight:
                if pause_check is not None and pause_check():
                    exhausted = True
                    break
                row = next(row_iter, None)
                if row is None:
                    exhausted = True
                    break
                pending.add(pool.submit(_process, row))

            if not pending:
                break

            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                entry = future.result()
                if entry is None:
                    continue
                if entry["best_match"]["confidence"] >= auto_accept_threshold:
                    auto_matches.append(entry)
                else:
                    review_matches.append(entry)

            if exhausted and not pending:
                break

    return auto_matches, review_matches


def detect_conflicts(auto_matches, kind):
    """Read-only. Splits `auto_matches` (fuzzy_match_tmdb's auto-accept
    bucket) into (clean, conflicts):

    - clean: the proposed tmdb_id is not already used by any OTHER existing
      row -- a plain backfill, safe for apply_backfill_and_merge() to write
      once that's implemented.
    - conflicts: the proposed tmdb_id already belongs to a DIFFERENT
      existing Series/Movie row. Writing tmdb_id here without also merging
      the two rows would hit the DB's unique constraint on tmdb_id -- see
      CLAUDE.md Rule #1. This is also the exact "provider A supplied a
      tmdb_id, provider B didn't" duplicate-row scenario this whole project
      exists to eventually consolidate (bead dispatcharr-vod-tmdb-companion-07r).
      Routed to manual review, never auto-merged -- see CLAUDE.md Rule #2."""
    from apps.vod.models import Series, Movie

    model = Series if kind == "series" else Movie

    # tmdb_id is a Django CharField (stored/returned as str) but TMDB's API
    # returns an int `id` (see fuzzy_match_tmdb's "tmdb_id": result.get("id"))
    # -- every proposed id must be stringified before it's used as a lookup
    # key or filter value, or comparisons against the DB's string ids always
    # miss and everything falls through to "clean" even when a row already
    # owns that id (confirmed live 2026-08-08: 9/10 test writes hit
    # unique-constraint violations that this function should have caught).
    proposed_ids = [
        str(entry["best_match"]["tmdb_id"])
        for entry in auto_matches
        if entry["best_match"].get("tmdb_id")
    ]
    if not proposed_ids:
        return auto_matches, []

    existing_owners = {
        row["tmdb_id"]: row["id"]
        for row in model.objects.filter(tmdb_id__in=proposed_ids).values("id", "tmdb_id")
    }

    clean = []
    conflicts = []
    seen_in_batch = {}  # tmdb_id (str) -> row id already claimed by an earlier clean entry
    for entry in auto_matches:
        tmdb_id = entry["best_match"].get("tmdb_id")
        if not tmdb_id:
            continue
        tmdb_id = str(tmdb_id)
        owner_id = existing_owners.get(tmdb_id)
        batch_owner_id = seen_in_batch.get(tmdb_id)
        if owner_id is not None and owner_id != entry["id"]:
            entry = dict(entry, conflicting_row_id=owner_id)
            conflicts.append(entry)
        elif batch_owner_id is not None and batch_owner_id != entry["id"]:
            # Two different rows in THIS batch proposed the same tmdb_id --
            # neither owns it in the DB yet, so the existing_owners check
            # above can't catch it, but only one write can succeed. Route
            # the second (and any further) claimant to manual review rather
            # than letting it fail at write time.
            entry = dict(entry, conflicting_row_id=batch_owner_id)
            conflicts.append(entry)
        else:
            seen_in_batch[tmdb_id] = entry["id"]
            clean.append(entry)

    return clean, conflicts


def list_review_items():
    """Read-only. Rebuilds the same auto/review/conflict/no_result buckets
    _do_run_reconcile_pass's summary uses, but returns the actual row-level
    entries instead of just counts -- this is what the /review page renders.

    'review' and 'no_result' rows are actionable here (a plain tmdb_id write,
    same path as apply_backfill_and_merge). 'conflict' rows are shown for
    visibility only -- resolving them means merging the losing row's
    Episode/M3U relations into the surviving row before deleting it, which
    isn't implemented yet (see apply_backfill_and_merge's TODO / bead
    dispatcharr-vod-tmdb-companion-07r), so no write action is offered
    for them here."""
    checkpoint = _load_checkpoint()

    series_auto_raw, series_review, series_no_result = [], [], []
    movies_auto_raw, movies_review, movies_no_result = [], [], []
    for key, record in checkpoint.items():
        kind, row_id = key.split(":", 1)
        outcome, entry = record["outcome"], record.get("entry")
        if outcome == "resolved":
            continue
        if kind == "series":
            if outcome == "auto":
                series_auto_raw.append(entry)
            elif outcome == "review":
                series_review.append(entry)
            elif outcome == "no_result":
                series_no_result.append({"id": int(row_id)})
        else:
            if outcome == "auto":
                movies_auto_raw.append(entry)
            elif outcome == "review":
                movies_review.append(entry)
            elif outcome == "no_result":
                movies_no_result.append({"id": int(row_id)})

    _, series_conflicts = detect_conflicts(series_auto_raw, "series")
    _, movies_conflicts = detect_conflicts(movies_auto_raw, "movie")

    # no_result rows have no TMDB candidates to show -- look their name/year
    # back up from the DB so the review page can at least display the title
    # and offer a manual tmdb_id text entry.
    def _hydrate_no_result(rows, kind):
        if not rows:
            return []
        from apps.vod.models import Series, Movie
        model = Series if kind == "series" else Movie
        ids = [r["id"] for r in rows]
        found = {row.id: row for row in model.objects.filter(id__in=ids).only("id", "name", "year")}
        hydrated = []
        for r in rows:
            row = found.get(r["id"])
            if row is None:
                continue
            hydrated.append({"id": row.id, "name": row.name, "year": row.year, "top_5": []})
        return hydrated

    return {
        "series": {
            "review": series_review,
            "conflicts": series_conflicts,
            "no_result": _hydrate_no_result(series_no_result, "series"),
        },
        "movie": {
            "review": movies_review,
            "conflicts": movies_conflicts,
            "no_result": _hydrate_no_result(movies_no_result, "movie"),
        },
    }


def list_review_items_paginated(kind, outcome, page=1, per_page=500):
    """Paginated version: only loads the specific page from checkpoint, not all items.

    This avoids loading 50k+ items into memory just to slice to 500. Direct checkpoint
    iteration stops at the requested page range.

    Args:
        kind: 'series' or 'movie'
        outcome: 'review', 'no_result', or 'conflicts'
        page: 1-indexed page number
        per_page: items per page (default 500)

    Returns: {items: [...], total: int, pages: int, page: int, per_page: int}
    """
    if kind not in ("series", "movie"):
        return {"items": [], "total": 0, "pages": 0, "page": page, "per_page": per_page}

    checkpoint = _load_checkpoint()

    # Single-pass: count + collect only the page we need
    items_on_page = []
    total_count = 0
    current_idx = 0
    target_start = (page - 1) * per_page
    target_end = target_start + per_page

    for key, record in checkpoint.items():
        kind_str, row_id = key.split(":", 1)
        if kind_str != kind or record.get("outcome") != outcome or record.get("outcome") == "resolved":
            continue

        # We found a matching item; decide if we need it
        if total_count >= target_start and total_count < target_end:
            entry = record.get("entry")
            if entry:
                items_on_page.append(entry)
            elif outcome == "no_result":
                # Hydrate no_result row from DB
                from apps.vod.models import Series, Movie
                model = Series if kind == "series" else Movie
                try:
                    row = model.objects.only("id", "name", "year").get(id=int(row_id))
                    items_on_page.append({"id": row.id, "name": row.name, "year": row.year, "top_5": []})
                except:
                    pass  # Row deleted from DB, skip

        total_count += 1

    # Calculate page count
    pages = (total_count + per_page - 1) // per_page if total_count else 1
    page = max(1, min(page, pages))

    return {
        "items": items_on_page,
        "total": total_count,
        "pages": pages,
        "page": page,
        "per_page": per_page,
    }


def resolve_review_items(selections, dry_run, log=None):
    """Writes a user-chosen tmdb_id for one or more review/no_result rows and
    marks them 'resolved' in the checkpoint so they drop off the review page.

    selections: list of {"kind": "series"|"movie", "id": int, "tmdb_id": str}.
    Re-checks for a live tmdb_id conflict at write time (same rule as
    detect_conflicts -- the DB may have changed since the row was scanned)
    rather than trusting the review page's stale snapshot; conflicting
    selections are skipped and reported, not force-written.

    Returns (written_count, skipped) where skipped is a list of
    {"kind", "id", "reason"} for anything not written."""
    from django.db import transaction
    from apps.vod.models import Series, Movie

    checkpoint = _load_checkpoint()
    written = 0
    skipped = []

    for sel in selections:
        kind = sel.get("kind")
        row_id = sel.get("id")
        tmdb_id = str(sel.get("tmdb_id") or "").strip()
        if kind not in ("series", "movie") or not row_id or not tmdb_id:
            skipped.append({"kind": kind, "id": row_id, "reason": "missing kind/id/tmdb_id"})
            continue

        model = Series if kind == "series" else Movie
        owner = model.objects.filter(tmdb_id=tmdb_id).exclude(id=row_id).values_list("id", flat=True).first()
        if owner is not None:
            skipped.append({
                "kind": kind, "id": row_id,
                "reason": f"tmdb_id {tmdb_id} is already used by row id={owner} -- needs a merge, not a plain write",
            })
            continue

        if dry_run:
            written += 1
            if log:
                log.info(f"VOD TMDB Reconciler [{kind}, manual resolve, DRY RUN]: id={row_id} -> tmdb_id={tmdb_id}")
        else:
            try:
                with transaction.atomic():
                    updated = model.objects.filter(id=row_id).update(tmdb_id=tmdb_id)
                if updated:
                    written += 1
                    if log:
                        log.info(f"VOD TMDB Reconciler [{kind}, manual resolve, WRITTEN]: id={row_id} -> tmdb_id={tmdb_id}")
                else:
                    skipped.append({"kind": kind, "id": row_id, "reason": "row no longer exists"})
            except Exception as e:
                skipped.append({"kind": kind, "id": row_id, "reason": str(e)})
                if log:
                    log.error(f"VOD TMDB Reconciler [{kind}, manual resolve, WRITE FAILED]: id={row_id} -> tmdb_id={tmdb_id}: {e}")

        key = f"{kind}:{row_id}"
        checkpoint[key] = {"outcome": "resolved", "entry": {"id": row_id, "resolved_tmdb_id": tmdb_id, "dry_run": dry_run}}

    _save_checkpoint(checkpoint)
    return written, skipped


def apply_backfill_and_merge(clean_matches, kind, limit=None, log=None):
    """Writes tmdb_id for clean auto-matches (see detect_conflicts() -- the
    proposed id is not owned by any other existing row, so this is a plain
    field write with no merge/reassign/delete needed). Conflict-bucket rows
    must never be passed here; that path isn't implemented yet.

    If `limit` is set, writes only the top-`limit` matches by confidence
    (highest first), leaving the rest proposed-only -- used for the initial
    small live test before trusting this at full scale.

    Returns (written_count, error_count)."""
    from django.db import transaction
    from apps.vod.models import Series, Movie

    model = Series if kind == "series" else Movie

    ordered = sorted(
        clean_matches,
        key=lambda entry: entry["best_match"]["confidence"],
        reverse=True,
    )
    if limit is not None:
        ordered = ordered[:limit]

    written = 0
    errors = 0
    for entry in ordered:
        row_id = entry["id"]
        tmdb_id = entry["best_match"].get("tmdb_id")
        title = entry["best_match"].get("title")
        confidence = entry["best_match"]["confidence"]
        if not tmdb_id:
            continue
        try:
            with transaction.atomic():
                updated = model.objects.filter(id=row_id).update(tmdb_id=tmdb_id)
            if updated:
                written += 1
                if log:
                    log.info(
                        f"VOD TMDB Reconciler: [{kind}, WRITTEN] id={row_id} "
                        f"-> tmdb_id={tmdb_id} '{title}' confidence={confidence}%"
                    )
            else:
                errors += 1
                if log:
                    log.warning(
                        f"VOD TMDB Reconciler: [{kind}, write skipped] id={row_id} "
                        f"no longer exists"
                    )
        except Exception as e:
            errors += 1
            if log:
                log.error(
                    f"VOD TMDB Reconciler: [{kind}, WRITE FAILED] id={row_id} "
                    f"-> tmdb_id={tmdb_id}: {e}"
                )

    return written, errors


# Sibling Dispatcharr plugins that may already have a TMDB key configured in
# their own PluginConfig row, checked if TMDB_API_KEY isn't set as an env
# var -- avoids making the user re-enter a key they've already set up
# elsewhere. Plain Django ORM read of another plugin's settings JSON; no
# permission barrier between plugins/apps sharing the same DB.
_FALLBACK_PLUGIN_KEYS = ["vod_plex_bridge", "vod_plex_webdav", "vod_tmdb_reconciler"]


def _resolve_tmdb_api_key(settings, log):
    api_key = settings.get("tmdb_api_key") or os.environ.get("TMDB_API_KEY", "")
    if api_key:
        return api_key

    try:
        from apps.plugins.models import PluginConfig
    except Exception:
        return ""

    for plugin_key in _FALLBACK_PLUGIN_KEYS:
        try:
            cfg = PluginConfig.objects.filter(key=plugin_key).first()
        except Exception:
            continue
        if not cfg:
            continue
        found = (cfg.settings or {}).get("tmdb_api_key") or ""
        if found:
            log.info(f"VOD TMDB Reconciler: using tmdb_api_key from '{plugin_key}' plugin settings")
            return found

    return ""


def _do_run_reconcile_pass(settings, log):
    """The actual work, run on a background thread by run_reconcile_pass()
    below. Never call this directly from an HTTP action handler -- at tens
    of thousands of rows and TMDB_SEARCH_DELAY_SECS between calls, this can
    run for hours, far past Dispatcharr's action-run HTTP timeout (a 504 was
    observed against the live catalog: 5,976 series + 50,114 movies missing
    tmdb_id/imdb_id)."""
    dry_run = bool(settings.get("dry_run", True))
    api_key = _resolve_tmdb_api_key(settings, log)
    threshold = float(settings.get("auto_accept_confidence", AUTO_ACCEPT_DEFAULT))
    max_rows = settings.get("max_rows")
    max_rows = int(max_rows) if max_rows not in (None, "", 0, "0") else None
    worker_threads = settings.get("worker_threads")
    worker_threads = int(worker_threads) if worker_threads not in (None, "", 0, "0") else DEFAULT_WORKER_THREADS
    scope = settings.get("scope") or "both"
    include_series = scope in ("both", "series")
    include_movies = scope in ("both", "movies")

    log.info(
        f"VOD TMDB Reconciler: reconcile pass starting (dry_run={dry_run}, "
        f"scope={scope}, max_rows={max_rows or 'unlimited'}, worker_threads={worker_threads})"
    )

    if not api_key:
        msg = "No TMDB API key configured (checked own settings and sibling plugins) -- nothing to do."
        log.warning(msg)
        _save_run_state({"running": False, "summary": msg, "error": None})
        return

    series_missing, movies_missing = scan_missing_ids()
    total_series_missing, total_movies_missing = len(series_missing), len(movies_missing)
    log.info(
        f"VOD TMDB Reconciler: scan found {total_series_missing} series and "
        f"{total_movies_missing} movies with no tmdb_id/imdb_id"
    )

    checkpoint = _load_checkpoint()
    resuming = bool(checkpoint)
    if resuming:
        log.info(
            f"VOD TMDB Reconciler: found checkpoint with {len(checkpoint)} already-scanned "
            f"rows -- resuming, these will be skipped"
        )

    def _already_done(kind, row_id):
        return f"{kind}:{row_id}" in checkpoint

    series_todo = (
        [r for r in series_missing if not _already_done("series", r["id"])]
        if include_series else []
    )
    movies_todo = (
        [r for r in movies_missing if not _already_done("movie", r["id"])]
        if include_movies else []
    )

    if max_rows:
        series_todo = series_todo[:max_rows]
        movies_todo = movies_todo[:max_rows]
        log.info(
            f"VOD TMDB Reconciler: max_rows={max_rows} set, capping this pass to "
            f"{len(series_todo)} series + {len(movies_todo)} movies (test mode)"
        )

    _clear_pause_flag()

    # save_row/progress fire once per completed row, from any worker thread.
    # Writing the full checkpoint (and run-state) file to disk on every single
    # row means the per-row cost grows with however many entries have
    # accumulated so far -- fine for the first few hundred rows, but by
    # 10-15k+ entries each rewrite gets slow enough to dominate wall-clock
    # time regardless of worker_threads (confirmed live: throughput looked
    # fine early in a pass, then flattened out well below what raw TMDB
    # concurrency supports as the checkpoint grew). Batching the disk flush
    # to roughly every CHECKPOINT_FLUSH_INTERVAL_SECS (or on pause/finish)
    # keeps resume-after-crash safety without paying that cost per row.
    CHECKPOINT_FLUSH_INTERVAL_SECS = 2.0
    _last_flush = [0.0]
    _dirty = [False]

    def _flush_checkpoint(force=False):
        now = time.time()
        if not _dirty[0]:
            return
        if not force and (now - _last_flush[0]) < CHECKPOINT_FLUSH_INTERVAL_SECS:
            return
        _save_checkpoint(checkpoint)
        _last_flush[0] = now
        _dirty[0] = False

    def progress(kind, done, total):
        state = _load_run_state()
        state.setdefault("progress", {})[kind] = {"done": done, "total": total}
        _save_run_state(state)
        if done % 25 == 0 or done == total:
            log.info(f"VOD TMDB Reconciler: {kind} progress {done}/{total}")

    def save_row(kind, row_id, outcome, entry):
        checkpoint[f"{kind}:{row_id}"] = {"outcome": outcome, "entry": entry}
        _dirty[0] = True
        _flush_checkpoint()

    state = _load_run_state()
    state["progress"] = {
        "series": {"done": 0, "total": len(series_todo)},
        "movie": {"done": 0, "total": len(movies_todo)},
    }
    _save_run_state(state)

    fuzzy_match_tmdb(
        series_todo, "series", api_key, threshold, progress_cb=progress,
        worker_threads=worker_threads, row_done_cb=save_row, pause_check=_pause_requested
    )
    _flush_checkpoint(force=True)
    paused_before_movies = _pause_requested()
    if not paused_before_movies:
        fuzzy_match_tmdb(
            movies_todo, "movie", api_key, threshold, progress_cb=progress,
            worker_threads=worker_threads, row_done_cb=save_row, pause_check=_pause_requested
        )
        _flush_checkpoint(force=True)
    paused = _pause_requested()

    # Rebuild the full auto/review buckets from the checkpoint (not just this
    # pass's in-memory results) so a resumed run's summary reflects
    # everything scanned so far, not just the rows processed since resuming.
    series_auto_raw, series_review = [], []
    movies_auto_raw, movies_review = [], []
    series_no_result = movies_no_result = 0
    for key, record in checkpoint.items():
        kind, row_id = key.split(":", 1)
        outcome, entry = record["outcome"], record["entry"]
        if kind == "series":
            if outcome == "auto":
                series_auto_raw.append(entry)
            elif outcome == "review":
                series_review.append(entry)
            else:
                series_no_result += 1
        else:
            if outcome == "auto":
                movies_auto_raw.append(entry)
            elif outcome == "review":
                movies_review.append(entry)
            else:
                movies_no_result += 1

    series_auto, series_conflicts = detect_conflicts(series_auto_raw, "series")
    movies_auto, movies_conflicts = detect_conflicts(movies_auto_raw, "movie")

    # Update checkpoint to mark conflicts (they were stored as "auto" during the scan,
    # but now that we've detected conflicts, they should be reclassified)
    for entry in series_conflicts:
        row_id = entry["id"]
        checkpoint[f"series:{row_id}"]["outcome"] = "conflicts"
    for entry in movies_conflicts:
        row_id = entry["id"]
        checkpoint[f"movie:{row_id}"]["outcome"] = "conflicts"
    _save_checkpoint(checkpoint)

    for entry in series_auto:
        log.info(
            f"VOD TMDB Reconciler [series, proposed, NOT WRITTEN]: "
            f"'{entry['name']}' ({entry['year']}) -> tmdb_id={entry['best_match']['tmdb_id']} "
            f"'{entry['best_match']['name']}' confidence={entry['best_match']['confidence']:.0f}%"
        )
    for entry in movies_auto:
        log.info(
            f"VOD TMDB Reconciler [movie, proposed, NOT WRITTEN]: "
            f"'{entry['name']}' ({entry['year']}) -> tmdb_id={entry['best_match']['tmdb_id']} "
            f"'{entry['best_match']['name']}' confidence={entry['best_match']['confidence']:.0f}%"
        )
    for entry in series_conflicts:
        log.warning(
            f"VOD TMDB Reconciler [series, CONFLICT, needs manual merge]: "
            f"row id={entry['id']} '{entry['name']}' ({entry['year']}) -> tmdb_id="
            f"{entry['best_match']['tmdb_id']} is already used by existing row id="
            f"{entry['conflicting_row_id']} -- NOT written, would violate unique constraint"
        )
    for entry in movies_conflicts:
        log.warning(
            f"VOD TMDB Reconciler [movie, CONFLICT, needs manual merge]: "
            f"row id={entry['id']} '{entry['name']}' ({entry['year']}) -> tmdb_id="
            f"{entry['best_match']['tmdb_id']} is already used by existing row id="
            f"{entry['conflicting_row_id']} -- NOT written, would violate unique constraint"
        )

    scope_note = (
        f" (capped to first {max_rows} of {total_series_missing} series / "
        f"{total_movies_missing} movies -- test mode)"
        if max_rows else ""
    )
    scope_note += "" if scope == "both" else f" (scope: {scope} only)"
    status_note = " -- PAUSED, resume by clicking Run Reconcile again" if paused else ""
    scanned_series = len(series_auto_raw) + len(series_review) + series_no_result
    scanned_movies = len(movies_auto_raw) + len(movies_review) + movies_no_result
    write_mode_note = "Scan-only pass" if dry_run else "Reconcile pass"
    summary = (
        f"{write_mode_note}{status_note}{scope_note}. "
        f"Scanned so far: {scanned_series}/{total_series_missing} series, "
        f"{scanned_movies}/{total_movies_missing} movies. "
        f"Series: {len(series_auto)} would auto-match, {len(series_conflicts)} are "
        f"duplicate-row conflicts (need manual merge), {len(series_review)} would need "
        f"manual review, {series_no_result} had no TMDB results. "
        f"Movies: {len(movies_auto)} would auto-match, {len(movies_conflicts)} are "
        f"duplicate-row conflicts (need manual merge), {len(movies_review)} would need "
        f"manual review, {movies_no_result} had no TMDB results."
    )

    if dry_run:
        summary += " No database writes were performed -- dry_run is enabled."
    else:
        # Series only for now, per the agreed initial rollout -- movies
        # haven't finished a full scan pass yet and are held back until
        # series writes are proven live. Remove this restriction once that
        # trust is established (not yet decided how -- likely just deleting
        # this early-return, see CLAUDE.md).
        test_write_limit = settings.get("test_write_limit")
        test_write_limit = (
            int(test_write_limit) if test_write_limit not in (None, "", 0, "0") else None
        )
        series_written, series_errors = apply_backfill_and_merge(
            series_auto, "series", limit=test_write_limit, log=log
        )
        summary += (
            f" Wrote tmdb_id for {series_written} series"
            f"{f' (capped to {test_write_limit} highest-confidence)' if test_write_limit else ''}"
            f"{f' -- {series_errors} write errors, see log' if series_errors else ''}"
            f". Movies are NOT written yet (series-only for this initial rollout). "
            f"Conflict rows (duplicate-row merge) were NOT written -- that path isn't "
            f"implemented yet, still routed to manual review."
        )

    log.info(f"VOD TMDB Reconciler: {summary}")

    state = _load_run_state()
    state.update(running=False, summary=summary, error=None, paused=paused)
    _save_run_state(state)

    # TODO(bead dispatcharr-vod-tmdb-companion-07r), next milestone:
    #   - Merge path for the conflict bucket (series_conflicts/movies_conflicts
    #     above): reassign M3USeriesRelation/M3UMovieRelation (and, for series,
    #     Episode rows too -- Episode.series is CASCADE, so the losing series'
    #     episodes must be reassigned to the surviving series' Episode rows
    #     BEFORE the losing row is deleted, or they cascade-delete with it)
    #     from the losing row to the surviving row, then delete the losing
    #     row, all in one transaction. Never write the id alone. Still gated
    #     behind dry_run once built. Deliberately NOT implemented yet --
    #     apply_backfill_and_merge() below only handles the zero-conflict
    #     "clean" bucket (tmdb_id not already owned by any other row), which
    #     needs no merge/reassign/delete, just a plain field write.


def run_reconcile_pass(settings, log=None):
    """Launches the actual pass on a background thread and returns
    immediately, so the synchronous HTTP action-run request Dispatcharr
    makes doesn't block until completion (and time out) at scale. Poll
    get_run_status() for progress/result -- the previous version of this
    function returned the summary directly, which worked for the small
    smoke-test case but 504'd against the live catalog's ~56,000 missing
    rows."""
    log = log or logger

    state = _load_run_state()
    if state.get("running"):
        return "A reconcile pass is already running -- check the Logs tab for progress."
    _clear_pause_flag()
    _save_run_state({"running": True, "summary": None, "error": None})

    def _target():
        try:
            _do_run_reconcile_pass(settings, log)
        except Exception as e:
            log.error(f"VOD TMDB Reconciler: reconcile pass failed: {e}")
            state = _load_run_state()
            state.update(running=False, error=str(e))
            _save_run_state(state)

    threading.Thread(target=_target, name="vod-tmdb-reconcile", daemon=True).start()
    return "Reconcile pass started in the background -- check the Logs tab for progress and the final summary."
