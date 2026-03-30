FROM python:3.12-slim

# Install system dependencies and uv
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Configure environment early (static metadata)
ENV PATH="/app/.venv/bin:$PATH"
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies (Medium frequency change)
# This layer is only invalidated if pyproject.toml or uv.lock changes.
COPY pyproject.toml uv.lock ./
COPY builders/server/pyproject.toml ./builders/server/pyproject.toml
# - --mount=type=cache: Persist uv's package cache across builds.
# - --frozen: strict sync from uv.lock (no updates).
# - --no-install-project: Do not attempt to install the project package itself (source not yet present).
# - --no-dev: Skip dev dependencies.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy configuration and scripts (Low frequency change)
# Moving these above the app code ensures they stay cached even if source code changes.
# Using --chmod saves an extra layer compared to a separate RUN command.
COPY --chmod=755 infra/builder/entrypoint.sh ./entrypoint.sh
COPY alembic.ini ./

# Copy application source code (High frequency change)
# This changes almost every build, so it sits at the very bottom.
COPY builders/server/ ./

ENTRYPOINT ["/app/entrypoint.sh"]
