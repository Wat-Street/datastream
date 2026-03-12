docker-up:
    docker compose -f infra/docker-compose.yml up --build -d

docker-down:
    docker compose -f infra/docker-compose.yml down

lint:
    uv run ruff check

format:
    uv run ruff format

test:
    uv run pytest

precommit:
    uv run pre-commit run --all-files

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
