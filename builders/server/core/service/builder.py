"""Public build API: build_dataset() and get_data(); selects the store."""

from dataclasses import dataclass
from datetime import datetime

import structlog

import core.db.datasets
from core.runtime import registry
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
    "DataResult",
]


@dataclass
class DataResult:
    """Result of a data fetch, including completeness metadata."""

    data: dict[datetime, list[dict]]
    total_timestamps: int
    returned_timestamps: int


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
