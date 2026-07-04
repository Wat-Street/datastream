from dataclasses import dataclass
from datetime import datetime

import structlog

import core.db.datasets
from core.runtime import registry
from core.service.locks import get_build_lock
from core.service.orchestrator import run_build
from core.service.store import MemoryStore, PostgresStore
from core.service.timestamps import NoValidTimestampsError, generate_timestamps
from core.utils.semver import SemVer

logger = structlog.get_logger()

# re-export so existing imports (api/routes.py, tests) keep working
__all__ = [
    "NoValidTimestampsError",
    "generate_timestamps",
    "build_dataset",
    "get_data",
    "delete_data",
    "DataResult",
    "DeleteResult",
    "DatasetNotFoundError",
    "NoDataInRangeError",
]


class DatasetNotFoundError(Exception):
    """Raised when a dataset is not present in the config registry."""


class NoDataInRangeError(Exception):
    """Raised when a delete matches no rows in the requested range."""


@dataclass
class DataResult:
    """Result of a data fetch, including completeness metadata."""

    data: dict[datetime, list[dict]]
    total_timestamps: int
    returned_timestamps: int


@dataclass
class DeleteResult:
    """Result of a delete, including the actual range of deleted rows."""

    rows_deleted: int
    start: datetime
    end: datetime


def build_dataset(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
    *,
    dry_run: bool = False,
) -> dict[datetime, list[dict]] | None:
    """
    Public entrypoint for building a dataset and its dependencies.
    With ``dry_run=true``, builders data in-memory and return when completed.
    """
    if not dry_run:
        run_build(dataset_name, dataset_version, start, end, store=PostgresStore())
        return None

    store = MemoryStore()
    run_build(dataset_name, dataset_version, start, end, store=store)
    return store.get_rows_range(dataset_name, dataset_version, start, end)


def delete_data(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> DeleteResult:
    """Delete a dataset's rows in [start, end].

    Holds the dataset's build lock during the delete so it cannot interleave
    with a concurrent build of the same dataset. Dependents are not checked:
    deleting from a dataset is allowed even if datasets that depend on it
    still have derived data in the range.

    Raises DatasetNotFoundError if the dataset is not in the config registry,
    NoDataInRangeError if no rows exist in the range.
    """
    try:
        registry.get_config(dataset_name, dataset_version)
    except ValueError as exc:
        raise DatasetNotFoundError(str(exc)) from exc

    with get_build_lock(dataset_name, str(dataset_version)):
        deleted = core.db.datasets.delete_rows_range(
            dataset_name, dataset_version, start, end
        )

    if not deleted:
        raise NoDataInRangeError(
            f"no data for {dataset_name}/{dataset_version} in "
            f"[{start.isoformat()}, {end.isoformat()}]"
        )

    logger.info(
        "data deleted",
        dataset=dataset_name,
        version=str(dataset_version),
        rows_deleted=len(deleted),
        start=min(deleted).isoformat(),
        end=max(deleted).isoformat(),
    )

    return DeleteResult(
        rows_deleted=len(deleted),
        start=min(deleted),
        end=max(deleted),
    )


def get_data(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
    *,
    build_data: bool,
) -> DataResult:
    """Fetch data for a dataset, optionally building missing data first."""
    cfg = registry.get_config(dataset_name, dataset_version)

    if build_data:
        logger.info(
            "triggering build after get_data call",
            dataset=dataset_name,
            version=str(dataset_version),
            start=start.isoformat(),
            end=end.isoformat(),
        )
        build_dataset(dataset_name, dataset_version, start, end)

    total_num_rows = len(generate_timestamps(start, end, cfg.granularity, cfg.calendar))
    data = core.db.datasets.get_rows_range(dataset_name, dataset_version, start, end)

    logger.info(
        "data fetched",
        dataset=dataset_name,
        version=str(dataset_version),
        build_data=build_data,
        total_timestamps=total_num_rows,
        returned_timestamps=len(data),
    )

    return DataResult(
        data=data,
        total_timestamps=total_num_rows,
        returned_timestamps=len(data),
    )
