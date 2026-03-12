FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# install tern migration tool
ARG TERN_VERSION=2.2.3
RUN apt-get update && apt-get install -y --no-install-recommends curl tar \
    && curl -fsSL https://github.com/jackc/tern/releases/download/v${TERN_VERSION}/tern_${TERN_VERSION}_linux_amd64.tar.gz \
       | tar -xz -C /usr/local/bin/ tern \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock /app/
RUN uv sync --no-dev

COPY builders/server/ /app/
COPY builders/migrations/ /app/migrations/

ENTRYPOINT ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]
