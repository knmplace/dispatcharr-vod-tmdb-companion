"""
Bootstraps Django standalone so reconciler.py's `from apps.vod.models import
...` calls work outside of Dispatcharr's own uwsgi process.

This container's image is built FROM (or copies app code out of)
ghcr.io/dispatcharr/dispatcharr -- see Dockerfile -- so `dispatcharr.settings`
and the `apps.*` packages are importable as-is, no reimplementation of
Dispatcharr's own Django config. Must be called once, before any reconciler
function that touches the ORM.

dispatcharr.settings picks its DB backend via
dispatcharr.db.process_label.uses_geventpool_database_backend(), which keys
off sys.argv[0]/role heuristics (uwsgi/daphne/manage.py -> geventpool backend,
celery worker -> plain postgresql backend). This process is none of those, so
it would default to the geventpool backend (django_db_geventpool), which
expects a gevent-patched runtime we deliberately don't have. Faking argv[0]
to look like a celery worker routes us to the plain
django.db.backends.postgresql backend instead -- real blocking psycopg
connections, no gevent dependency, which is the whole point of this being a
standalone process.
"""

import os
import sys


def setup_django():
    sys.path.insert(0, "/app")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
    sys.argv[0] = "celery"

    import django

    django.setup()
