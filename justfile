# build all docker images and start containers in detached mode (production, no exposed internal ports)
docker-up:
    docker compose -f infra/docker-compose.yml up --build -d

# build and start containers with all ports exposed for local development
docker-up-dev:
    docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up --build -d

# stop and remove all docker containers
docker-down:
    docker compose -f infra/docker-compose.yml down

# stop and remove all docker containers (dev overlay)
docker-down-dev:
    docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml down

# run ruff linter with autofix and formatter
fix:
    uv run ruff check --fix && uv run ruff format

# run unit tests, optionally scoped to a specific path
test PATH="":
    uv run pytest --ignore=builders/server/tests/integration --ignore=builders/server/benchmarks {{PATH}}

# run integration tests only
test-integration:
    uv run pytest -m integration builders/server/tests/integration/

# run all pre-commit hooks against all files
precommit:
    uv run pre-commit run --all-files

# generate root pyrightconfig.json with per-builder venv environments
gen-pyright:
    python dev-tools/gen_pyrightconfig.py

# run the builder service locally against the postgres docker container
# postgres is exposed on localhost:5432, so DATABASE_URL uses localhost instead of the docker-internal hostname
# --reload enables hot reload on file changes
# mints a fresh dev api key each run (never persisted or committed) and prints it for use as: Authorization: Bearer <key>
backend-dev:
    #!/usr/bin/env bash
    set -euo pipefail
    docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up postgres -d --wait
    DATABASE_URL=postgresql://datastream:changeme@localhost:5432/datastream uv run alembic upgrade head
    python dev-tools/gen_pyrightconfig.py
    gen_output=$(cd builders/server && uv run python -m core.auth generate dev)
    raw_key=$(echo "$gen_output" | sed -n 's/^key (give to client): //p')
    env_line=$(echo "$gen_output" | sed -n 's/^env line (add to API_KEYS): //p')
    echo "dev api key (Authorization: Bearer $raw_key)"
    SCRIPTS_DIR={{justfile_directory()}}/builders/scripts DATABASE_URL=postgresql://datastream:changeme@localhost:5432/datastream API_KEYS="$env_line" uv run uvicorn main:app --host 0.0.0.0 --port 3000 --app-dir builders/server --reload

# install frontend deps and start the vite dev server on port 5173
frontend-dev:
    cd frontend && bun install && bun run dev

# apply all pending db migrations
migrate:
    uv run alembic upgrade head

# create a new migration revision with the given name
migrate-new NAME:
    uv run alembic revision -m "{{NAME}}"

# revert the last applied migration
migrate-down:
    uv run alembic downgrade -1

# show full migration history
migrate-history:
    uv run alembic history

# run build benchmarks with pytest-benchmark
bench:
    uv run pytest builders/server/benchmarks/ --benchmark-only --benchmark-sort=mean -v

# generate a flame graph of the build pipeline via py-spy
bench-profile DAYS="90":
    cd builders/server && sudo py-spy record --subprocesses --format raw -o ../../bench-flamegraph.raw -- uv run python -m benchmarks.bench_build --days {{DAYS}} && inferno-flamegraph < ../../bench-flamegraph.raw > ../../bench-flamegraph.svg && rm ../../bench-flamegraph.raw
