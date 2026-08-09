# Built FROM Dispatcharr's own published image so Django settings, the
# apps.* package tree, and their exact dependency versions (Django,
# psycopg, requests, ...) come along for free -- this container's
# django_bootstrap.py imports dispatcharr.settings directly, so it needs
# Dispatcharr's actual app code, not a reimplementation of it.
FROM ghcr.io/dispatcharr/dispatcharr:latest AS dispatcharr

FROM python:3.13-slim

WORKDIR /app

COPY --from=dispatcharr /app /app
COPY --from=dispatcharr /dispatcharrpy /dispatcharrpy

# dispatcharrpy's venv ships no pip (slim runtime image, packages baked in at
# build time) so we can't `pip install` into it directly. Instead we install
# fastapi/uvicorn into THIS image's own python3.13 site-packages using its
# pip, then point PYTHONPATH at both: our own site-packages (for
# fastapi/uvicorn) and dispatcharrpy's site-packages (for Django/apps.*/etc).
# One interpreter (this image's python3, matching version 3.13) sees both.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY app/ /companion/
WORKDIR /companion

ARG BUILD_SHA=unknown
ENV BUILD_SHA=${BUILD_SHA}
ENV DATA_DIR=/data
ENV PYTHONPATH=/app:/companion:/dispatcharrpy/lib/python3.13/site-packages
VOLUME ["/data"]

EXPOSE 8686

CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8686"]
