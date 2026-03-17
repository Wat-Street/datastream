# build all docker images and start containers in detached mode
docker-up:
    docker compose -f infra/docker-compose.yml up --build -d

# stop and remove all docker containers
docker-down:
    docker compose -f infra/docker-compose.yml down

# run ruff linter with autofix and formatter
fix:
    uv run ruff check --fix && uv run ruff format

# run unit tests, optionally scoped to a specific path
test PATH="":
    uv run pytest --ignore=builders/server/tests/integration {{PATH}}

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
backend-dev:
    docker compose -f infra/docker-compose.yml up postgres -d --wait
    DATABASE_URL=postgresql://datastream:changeme@localhost:5432/datastream uv run alembic upgrade head
    python dev-tools/gen_pyrightconfig.py
    SCRIPTS_DIR={{justfile_directory()}}/builders/scripts DATABASE_URL=postgresql://datastream:changeme@localhost:5432/datastream uv run uvicorn main:app --host 0.0.0.0 --port 3000 --app-dir builders/server --reload

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
