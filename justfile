lint:
    uv run ruff check

test:
    uv run pytest

build-rs:
    cd api && cargo build

clippy:
    cd api && cargo clippy -- -D warnings

precommit:
    uv run pre-commit run --all-files
