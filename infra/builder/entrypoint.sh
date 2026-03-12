#!/bin/sh
set -e

alembic upgrade head
exec uv run uvicorn main:app --host 0.0.0.0 --port 3000
