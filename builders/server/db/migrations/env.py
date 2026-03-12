import os

from alembic import context

# read the same DATABASE_URL the app uses
url = os.environ["DATABASE_URL"]

config = context.config


def run_migrations_online() -> None:
    """Run migrations against a live database."""
    from sqlalchemy import create_engine

    engine = create_engine(url)
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
