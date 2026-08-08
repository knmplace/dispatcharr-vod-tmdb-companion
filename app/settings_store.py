"""
Persisted app settings (TMDB key, DRY_RUN, confidence threshold, etc.) --
same file-based/atomic-write pattern reconciler.py uses for checkpoint/run
state, so settings also survive container restarts without needing a
database of their own.

Env vars still work as the seed for first boot (so an existing .env-based
deploy doesn't silently reset to defaults on upgrade), but settings.json in
DATA_DIR is the source of truth once it exists -- editing via the dashboard
does not require editing .env + recreating the container.
"""

import json
import os
import threading

_DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(_DATA_DIR, exist_ok=True)
_SETTINGS_PATH = os.path.join(_DATA_DIR, "settings.json")
_lock = threading.Lock()

_DEFAULTS = {
    "tmdb_api_key": "",
    "dry_run": True,
    "auto_accept_confidence": 80,
    "max_rows": 0,
    "worker_threads": 8,
    "test_write_limit": 0,
}

# (settings key, env var name, caster) -- used only to seed settings.json
# the first time it doesn't exist yet.
_ENV_SEED = [
    ("tmdb_api_key", "TMDB_API_KEY", str),
    ("dry_run", "DRY_RUN", lambda v: str(v).lower() == "true"),
    ("auto_accept_confidence", "AUTO_ACCEPT_CONFIDENCE", int),
    ("max_rows", "MAX_ROWS", int),
    ("worker_threads", "WORKER_THREADS", int),
    ("test_write_limit", "TEST_WRITE_LIMIT", int),
]


def _seed_from_env():
    settings = dict(_DEFAULTS)
    for key, env_name, cast in _ENV_SEED:
        raw = os.environ.get(env_name)
        if raw is not None and raw != "":
            try:
                settings[key] = cast(raw)
            except (TypeError, ValueError):
                pass
    return settings


def _atomic_write_json(path, data):
    with _lock:
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, path)


def load_settings():
    if not os.path.exists(_SETTINGS_PATH):
        settings = _seed_from_env()
        _atomic_write_json(_SETTINGS_PATH, settings)
        return settings
    try:
        with open(_SETTINGS_PATH, "r") as f:
            settings = json.load(f)
    except (json.JSONDecodeError, OSError):
        settings = _seed_from_env()
    # Backfill any keys added since a settings.json was last written, so an
    # upgrade doesn't crash on a missing key -- existing values are kept.
    merged = dict(_DEFAULTS)
    merged.update(settings)
    return merged


def save_settings(updates):
    settings = load_settings()
    settings.update(updates)
    _atomic_write_json(_SETTINGS_PATH, settings)
    return settings
