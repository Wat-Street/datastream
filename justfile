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
backend-dev:
    docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up postgres -d --wait
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

# create a new git worktree as a sibling directory and copy env files
worktree-create NAME:
    git worktree add "{{justfile_directory()}}/../{{NAME}}" -b "_wt-{{NAME}}" main
    cp "{{justfile_directory()}}/infra/.env" "{{justfile_directory()}}/../{{NAME}}/infra/.env"
    gt track "_wt-{{NAME}}" --parent main --no-interactive
    @echo "worktree created at ../{{NAME}}"
    @echo "cd {{justfile_directory()}}/../{{NAME}} to start working"

# submit graphite stack, auto-cleaning any worktree branch
submit:
    {{justfile_directory()}}/dev-tools/wt-submit.sh

# remove a worktree and clean up its graphite branch
worktree-remove NAME:
    git worktree remove "{{justfile_directory()}}/../{{NAME}}"
    -gt delete "_wt-{{NAME}}" --force --no-interactive

# run build benchmarks with pytest-benchmark
bench:
    uv run pytest builders/server/benchmarks/ --benchmark-only --benchmark-sort=mean -v

# generate a flame graph of the build pipeline via py-spy
bench-profile DAYS="90":
    cd builders/server && sudo py-spy record --subprocesses --format raw -o ../../bench-flamegraph.raw -- uv run python -m benchmarks.bench_build --days {{DAYS}} && inferno-flamegraph < ../../bench-flamegraph.raw > ../../bench-flamegraph.svg && rm ../../bench-flamegraph.raw
#test
# another test
