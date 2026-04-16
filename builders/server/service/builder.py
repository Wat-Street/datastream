from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import db.datasets
import structlog
from calendars.interface import Calendar
from runtime import config, registry, runner, validator
from utils.semver import SemVer

from service.locks import get_build_lock

logger = structlog.get_logger()


class NoValidTimestampsError(Exception):
    """raised when generate_timestamps returns no valid calendar dates for the range."""


@dataclass
class DataResult:
    """Result of a data fetch, including completeness metadata."""

    data: dict[datetime, list[dict]]
    total_timestamps: int
    returned_timestamps: int


# TODO (bryan): benchmark this, and optimize if needed
def generate_timestamps(
    start: datetime,
    end: datetime,
    granularity: timedelta,
    calendar: Calendar,
) -> list[datetime]:
    """Generate timestamps in [start, end], filtered by calendar."""
    current = calendar.next_open(start)
    if current is None:
        # no open timestamps
        return []

    timestamps = []
    while current <= end:
        if calendar.is_open(current):
            timestamps.append(current)
        current += granularity
    return timestamps


def build_dataset(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> None:
    """Public entrypoint for building a dataset and its dependencies."""
    _build_recursive(dataset_name, dataset_version, start, end)


def _build_recursive(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> None:
    """Resolve dependencies, build missing timestamps, insert rows."""
    cfg = registry.get_config(dataset_name, dataset_version)

    # enforce start-date
    start_date = cfg.start_date
    if end < start_date:
        raise ValueError(
            f"build request end date {end} is before dataset start-date {start_date}"
        )
    if start < start_date:
        logger.warning(
            "clamping start to dataset start-date",
            dataset=dataset_name,
            version=str(dataset_version),
            original_start=str(start),
            start_date=str(start_date),
        )
        start = start_date

    # recursively build dependencies first, expanding range for lookback
    for dep_name, dep_info in cfg.dependencies.items():
        if dep_info.lookback_subtract is not None:
            dep_start = start - dep_info.lookback_subtract
        else:
            dep_start = start
        _build_recursive(dep_name, dep_info.version, dep_start, end)

    # determine which timestamps are valid for the range (outside lock, deterministic)
    all_timestamps = generate_timestamps(start, end, cfg.granularity, cfg.calendar)

    if not all_timestamps:
        raise NoValidTimestampsError(
            f"{dataset_name}/{dataset_version}: no valid calendar timestamps "
            f"in range [{start}, {end}] for calendar '{cfg.calendar.name}'"
        )

    # acquire per-dataset lock to prevent concurrent builds from racing
    # between the "check missing" read and "insert rows" write
    with get_build_lock(dataset_name, str(dataset_version)):
        existing = set(
            db.datasets.get_existing_timestamps(
                dataset_name, dataset_version, start, end
            )
        )
        missing = [ts for ts in all_timestamps if ts not in existing]

        if not missing:
            logger.info(
                "all timestamps present, skipping",
                dataset=dataset_name,
                version=str(dataset_version),
            )
            return

        logger.info(
            "building missing timestamps",
            dataset=dataset_name,
            version=str(dataset_version),
            count=len(missing),
        )

        # resolve env file if env-vars is enabled
        env_file: Path | None = None
        if cfg.env_vars:
            env_file = config.SCRIPTS_DIR / dataset_name / str(cfg.version) / ".env"
            if not env_file.exists():
                raise FileNotFoundError(
                    f"{dataset_name}/{dataset_version}: env-vars is enabled "
                    f"but {env_file} does not exist"
                )

        script_dir = config.SCRIPTS_DIR / dataset_name / str(cfg.version)

        # TODO (bryan): this spawns builder processes sequentially, one for each
        # timestamp. we then sync wait for that process to be done before moving on.
        # can we spawn them all at the start, then launch them all at the same time?
        # because certain builders may block (i.e. fetch from an external API)

        # build each missing timestamp
        rows = []
        for ts in missing:
            # fetch dependency data for this timestamp
            dep_data: dict[str, dict[datetime, list[dict]]] = {}
            for dep_name, dep_info in cfg.dependencies.items():
                if dep_info.lookback_subtract is not None:
                    dep_rows = db.datasets.get_rows_range(
                        dep_name,
                        dep_info.version,
                        ts - dep_info.lookback_subtract,
                        ts,
                    )
                else:
                    # no lookback, fetch just this timestamp
                    dep_rows = db.datasets.get_rows_timestamps(
                        dep_name, dep_info.version, [ts]
                    )

                if not dep_rows:
                    raise RuntimeError(
                        f"Dependency '{dep_name}/{dep_info.version}' "
                        f"missing data for timestamp {ts}"
                    )
                dep_data[dep_name] = dep_rows

            # run builder in subprocess
            result = runner.run_builder(
                script_dir, cfg.builder, dep_data, ts, env_file=env_file
            )

            # validate output against schema
            validator.validate_rows(result, cfg.schema)

            rows.append((ts, result))

        # bulk insert all rows
        db.datasets.insert_rows(dataset_name, dataset_version, rows)
        logger.info(
            "inserted rows",
            dataset=dataset_name,
            version=str(dataset_version),
            count=len(rows),
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
    data = db.datasets.get_rows_range(dataset_name, dataset_version, start, end)

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
