import os

from alembic import context
from sqlalchemy.engine import make_url

# parse DATABASE_URL and set the psycopg v3 dialect explicitly;
# plain postgresql:// defaults to psycopg2 in sqlalchemy
url = make_url(os.environ["DATABASE_URL"]).set(drivername="postgresql+psycopg")

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
