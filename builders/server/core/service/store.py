"""Data store abstraction for the build path.

The build path reads and writes dataset data through four operations:
``get_existing_timestamps``, ``get_rows_range``, ``get_rows_timestamps`` and
``insert_rows``. ``Store`` puts those behind an interface with two backends:

- ``PostgresStore``: a thin shell over ``core.db.datasets`` (real builds).
- ``MemoryStore``: an in-process dict (dry runs -- never touches the DB).

The worker holds a ``store`` instead of calling the DB module directly, so a
dry run swaps the backend without changing any build logic. The store also owns
the build lock: real builds serialize on a shared per-dataset lock, while a dry
run's private ``MemoryStore`` needs no lock at all.
"""

import json
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime

import core.db.datasets
from core.service.locks import get_build_lock
from core.utils.semver import SemVer


class Store(ABC):
    """Backend for the four data operations the build path needs."""

    @abstractmethod
    def get_existing_timestamps(
        self,
        name: str,
        version: SemVer,
        start: datetime,
        end: datetime,
    ) -> list[datetime]:
        """Return distinct timestamps in [start, end] that already have rows."""
        ...

    @abstractmethod
    def get_rows_range(
        self,
        name: str,
        version: SemVer,
        start: datetime,
        end: datetime,
    ) -> dict[datetime, list[dict]]:
        """Return rows for [start, end], keyed by timestamp."""
        ...

    @abstractmethod
    def get_rows_timestamps(
        self,
        name: str,
        version: SemVer,
        timestamps: list[datetime],
    ) -> dict[datetime, list[dict]]:
        """Return rows for specific timestamps, keyed by timestamp."""
        ...

    @abstractmethod
    def insert_rows(
        self,
        name: str,
        version: SemVer,
        rows: list[tuple[datetime, list[dict]]],
    ) -> None:
        """Insert (timestamp, list[dict]) rows for a dataset."""
        ...

    @abstractmethod
    def build_lock(self, name: str, version: SemVer) -> AbstractContextManager:
        """Return the critical-section lock for this dataset's build."""
        ...


class PostgresStore(Store):
    """Real-build backend: forwards to ``core.db.datasets``, no behavior change."""

    def get_existing_timestamps(
        self,
        name: str,
        version: SemVer,
        start: datetime,
        end: datetime,
    ) -> list[datetime]:
        return core.db.datasets.get_existing_timestamps(name, version, start, end)

    def get_rows_range(
        self,
        name: str,
        version: SemVer,
        start: datetime,
        end: datetime,
    ) -> dict[datetime, list[dict]]:
        return core.db.datasets.get_rows_range(name, version, start, end)

    def get_rows_timestamps(
        self,
        name: str,
        version: SemVer,
        timestamps: list[datetime],
    ) -> dict[datetime, list[dict]]:
        return core.db.datasets.get_rows_timestamps(name, version, timestamps)

    def insert_rows(
        self,
        name: str,
        version: SemVer,
        rows: list[tuple[datetime, list[dict]]],
    ) -> None:
        core.db.datasets.insert_rows(name, version, rows)

    def build_lock(self, name: str, version: SemVer) -> AbstractContextManager:
        # shared per-dataset lock serializes concurrent real builds
        return get_build_lock(name, str(version))


class MemoryStore(Store):
    """Dry-run backend: holds produced rows in a dict, never opens a DB connection.

    Layout: ``{(name, version_str): {timestamp: [rows]}}``. Each request gets its
    own instance, so dry runs are isolated from each other and from real builds,
    and the whole graph is rebuilt from an empty store (it never reads committed
    data). The store is garbage-collected when the request ends.
    """

    def __init__(self) -> None:
        self._data: dict[tuple[str, str], dict[datetime, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )

    def get_existing_timestamps(
        self,
        name: str,
        version: SemVer,
        start: datetime,
        end: datetime,
    ) -> list[datetime]:
        table = self._data.get((name, str(version)), {})
        return sorted(ts for ts, rows in table.items() if rows and start <= ts <= end)

    def get_rows_range(
        self,
        name: str,
        version: SemVer,
        start: datetime,
        end: datetime,
    ) -> dict[datetime, list[dict]]:
        table = self._data.get((name, str(version)), {})
        return {
            ts: list(table[ts])
            for ts in sorted(table)
            if table[ts] and start <= ts <= end
        }

    def get_rows_timestamps(
        self,
        name: str,
        version: SemVer,
        timestamps: list[datetime],
    ) -> dict[datetime, list[dict]]:
        if not timestamps:
            return {}
        table = self._data.get((name, str(version)), {})
        wanted = set(timestamps)
        return {
            ts: list(table[ts]) for ts in sorted(table) if table[ts] and ts in wanted
        }

    def insert_rows(
        self,
        name: str,
        version: SemVer,
        rows: list[tuple[datetime, list[dict]]],
    ) -> None:
        if not rows:
            return
        table = self._data[(name, str(version))]
        for ts, data_list in rows:
            for data in data_list:
                # round-trip through json to mirror Postgres Jsonb serialization,
                # so non-serializable builder output fails here too
                table[ts].append(json.loads(json.dumps(data)))

    def build_lock(self, name: str, version: SemVer) -> AbstractContextManager:
        # a dry run's store is request-private, so there is no shared state to
        # guard -- and it must not take the real lock, which would block
        # production builds of the same dataset
        return nullcontext()
