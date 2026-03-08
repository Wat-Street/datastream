lint:
    uv run ruff check

test:
    uv run pytest

precommit:
    uv run pre-commit run --all-files
