"""Data store abstraction for the build path.

``Store`` puts the build path's data operations behind an ABC with two implementations:

- ``PostgresStore``: a thin shell over real build operations.
- ``MemoryStore``: an in-process dict, unrelated to the DB.

Workers hold a ``store`` instead of calling the DB directly
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
    """Interface for the build path's data operations."""

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
    """Real-build implementation that hits the DB"""

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
        return get_build_lock(name, str(version))


class MemoryStore(Store):
    """Dry-run implementation: holds produced rows in a dict, never hits the DB."""

    def __init__(self) -> None:
        # in-memory stand-in for the datasets table, holding rows produced during a dry run.
        # outer key: (dataset_name, stringified version) identifying a dataset
        # inner key: timestamp for that dataset's rows
        # value: list of data dicts at that timestamp (multiple rows can share a timestamp)
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
        table = self._data.get((name, str(version)), {})
        # iterate the wanted timestamps directly (no full-table scan or sort);
        # list(...) hands back a shallow copy so callers can't mutate stored rows
        return {ts: list(table[ts]) for ts in set(timestamps) if table.get(ts)}

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
                # json round-trip to mirror Postgres Jsonb serialization
                table[ts].append(json.loads(json.dumps(data)))

    def build_lock(self, name: str, version: SemVer) -> AbstractContextManager:
        """Stub to prevent interference with real build lock"""
        return nullcontext()
