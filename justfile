docker-up:
    docker compose -f infra/docker-compose.yml up --build -d

docker-down:
    docker compose -f infra/docker-compose.yml down

fix:
    uv run ruff check --fix && uv run ruff format

test PATH="":
    uv run pytest --ignore=builders/server/tests/integration {{PATH}}

test-integration:
    uv run pytest -m integration builders/server/tests/integration/

precommit:
    uv run pre-commit run --all-files

# run the builder service locally against the postgres docker container
# postgres is exposed on localhost:5432, so DATABASE_URL uses localhost instead of the docker-internal hostname
# --reload enables hot reload on file changes
backend-dev:
    docker compose -f infra/docker-compose.yml up postgres -d --wait
    DATABASE_URL=postgresql://datastream:changeme@localhost:5432/datastream uv run alembic upgrade head
    SCRIPTS_DIR={{justfile_directory()}}/builders/scripts DATABASE_URL=postgresql://datastream:changeme@localhost:5432/datastream uv run uvicorn main:app --host 0.0.0.0 --port 3000 --app-dir builders/server --reload

frontend-dev:
    cd frontend && bun install && bun run dev

migrate:
    uv run alembic upgrade head

migrate-new NAME:
    uv run alembic revision -m "{{NAME}}"

migrate-down:
    uv run alembic downgrade -1

migrate-history:
    uv run alembic history
