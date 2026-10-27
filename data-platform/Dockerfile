# Version tags fix the tool/Python family; base-image digests are not yet pinned.
# Cloud Build will record the resulting application image digest.
FROM python:3.12-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:0.12.12 /uv /usr/local/bin/uv

ENV UV_PYTHON_DOWNLOADS=0 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Explicit copies complement the build-context allowlist.
COPY pyproject.toml uv.lock README.md ./
COPY src/transit_realtime_pipeline/ ./src/transit_realtime_pipeline/
RUN uv sync --locked --no-dev --no-editable

# This image is only for checks. It is not the deployed collector image.
FROM builder AS checks
RUN uv sync --locked --group dev --no-editable
COPY tests/ ./tests/
COPY scripts/check_ci.py ./scripts/check_ci.py
COPY scripts/verify_gcs_repeat.py ./scripts/verify_gcs_repeat.py
ENV PATH="/app/.venv/bin:$PATH" \
    GTFS_RT_STORAGE_BACKEND=local
CMD ["/app/.venv/bin/python", "scripts/check_ci.py"]

# Install non-editably, then copy only runtime packages into the final image.
FROM python:3.12-slim-bookworm AS runtime
ARG SOURCE_COMMIT=unrecorded
LABEL org.opencontainers.image.revision="${SOURCE_COMMIT}"
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
USER 65532:65532
CMD ["/app/.venv/bin/python", "-m", "transit_realtime_pipeline.poller"]
