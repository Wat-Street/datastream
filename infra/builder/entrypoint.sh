#!/bin/sh
set -e

# fix script_location for container layout
# (alembic.ini uses repo-relative paths, but server code is at /app/)
sed -i 's|builders/server/core/db/migrations|core/db/migrations|' /app/alembic.ini
uv run alembic upgrade head
exec uv run uvicorn main:app --host 0.0.0.0 --port 3000
