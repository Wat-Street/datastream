import logging
from datetime import datetime, timedelta

import db.datasets
from calendars.interface import Calendar
from runtime import config, loader, runner, validator
from utils.semver import SemVer

logger = logging.getLogger(__name__)


class NoValidTimestampsError(Exception):
    """raised when generate_timestamps returns no valid calendar dates for the range."""


# TODO (bryan): benchmark this, and optimize if needed
# TODO: if start is not aligned to a calendar open day, stepping by delta
# may skip all open days in [start, end]. consider rounding start up to the
# first open day according to the calendar.
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


def _validate_dependency_graph_start_date(
    dataset_name: str, dataset_version: SemVer
) -> datetime:
    """
    Walk the dependency tree and validate start date constrants.

    A dataset's start date must be no earlier than any of its
    dependency's start dates. Raises ValueError if violated.

    Returns the current dataset's start date.
    """
    cfg = config.load_config(dataset_name, dataset_version)
    parent_start_date = cfg.start_date

    for dep_name, dep_info in cfg.dependencies.items():
        dep_start_date = _validate_dependency_graph_start_date(
            dep_name, dep_info.version
        )

        if parent_start_date < dep_start_date:
            raise ValueError(
                f"{dataset_name}/{dataset_version} has start date "
                f"'{parent_start_date}' which comes before dependency "
                f"{dep_name}/{dep_info.version} with start date {dep_start_date}"
            )

    return parent_start_date


def _validate_dependency_graph_granularity(
    dataset_name: str, dataset_version: SemVer
) -> None:
    """Walk the dependency tree and validate granularity constraints.

    A dataset's granularity must be >= (coarser or equal to) each
    dependency's granularity. Raises ValueError if violated.
    """
    cfg = config.load_config(dataset_name, dataset_version)
    parent_delta = cfg.granularity

    for dep_name, dep_info in cfg.dependencies.items():
        dep_cfg = config.load_config(dep_name, dep_info.version)

        if parent_delta < dep_cfg.granularity:
            raise ValueError(
                f"{dataset_name}/{dataset_version} has granularity "
                f"'{cfg.granularity}' which is finer than dependency "
                f"'{dep_name}/{dep_info.version}' with granularity "
                f"'{dep_cfg.granularity}'"
            )

        # recurse into dependency's own dependencies
        _validate_dependency_graph_granularity(dep_name, dep_info.version)


def validate_dependency_graph(
    dataset_name: str,
    dataset_version: SemVer,
) -> None:
    _validate_dependency_graph_granularity(dataset_name, dataset_version)
    _validate_dependency_graph_start_date(dataset_name, dataset_version)


def build_dataset(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> None:
    """Public entrypoint for building a dataset and its dependencies."""
    validate_dependency_graph(dataset_name, dataset_version)
    _build_recursive(dataset_name, dataset_version, start, end)


def _build_recursive(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> None:
    """Resolve dependencies, build missing timestamps, insert rows."""
    cfg = config.load_config(dataset_name, dataset_version)

    # enforce start-date
    start_date = cfg.start_date
    if end < start_date:
        raise ValueError(
            f"build request end date {end} is before dataset start-date {start_date}"
        )
    if start < start_date:
        logger.warning(
            f"{dataset_name}/{dataset_version}: clamping start from {start} "
            f"to start-date {start_date}"
        )
        start = start_date

    # recursively build dependencies first, expanding range for lookback
    for dep_name, dep_info in cfg.dependencies.items():
        dep_start = (
            start - dep_info.lookback if dep_info.lookback is not None else start
        )
        _build_recursive(dep_name, dep_info.version, dep_start, end)

    # determine which timestamps are missing
    all_timestamps = generate_timestamps(start, end, cfg.granularity, cfg.calendar)

    if not all_timestamps:
        raise NoValidTimestampsError(
            f"{dataset_name}/{dataset_version}: no valid calendar timestamps "
            f"in range [{start}, {end}] for calendar '{cfg.calendar.name}'"
        )

    existing = set(
        db.datasets.get_existing_timestamps(dataset_name, dataset_version, start, end)
    )
    missing = [ts for ts in all_timestamps if ts not in existing]

    if not missing:
        logger.info(
            f"{dataset_name}/{dataset_version}: all timestamps present, skipping"
        )
        return

    logger.info(
        f"{dataset_name}/{dataset_version}: building {len(missing)} missing timestamps"
    )

    # load the builder function
    build_fn = loader.load_builder(dataset_name, dataset_version)

    # TODO (bryan): this spawns builder processes sequentially, one for each timestamp.
    # we then sync wait for that process to be done before moving on.
    # can we spawn them all at the start, then launch them all at the same time?
    # because certain builders may block (i.e. fetch from an external API)

    # build each missing timestamp
    rows = []
    for ts in missing:
        # fetch dependency data for this timestamp
        dep_data: dict[str, dict[datetime, list[dict]]] = {}
        for dep_name, dep_info in cfg.dependencies.items():
            if dep_info.lookback is not None:
                # fetch time window [ts - lookback, ts]
                dep_rows = db.datasets.get_rows_range(
                    dep_name, dep_info.version, ts - dep_info.lookback, ts
                )
            else:
                # no lookback, fetch just this timestamp
                dep_rows = db.datasets.get_rows(dep_name, dep_info.version, [ts])

            if not dep_rows:
                raise RuntimeError(
                    f"Dependency '{dep_name}/{dep_info.version}' "
                    f"missing data for timestamp {ts}"
                )
            dep_data[dep_name] = dep_rows

        # run builder in subprocess
        result = runner.run_builder(build_fn, dep_data, ts)

        # validate output against schema
        validator.validate_rows(result, cfg.schema)

        rows.append((ts, result))

    # bulk insert all rows
    db.datasets.insert_rows(dataset_name, dataset_version, rows)
    logger.info(f"{dataset_name}/{dataset_version}: inserted {len(rows)} rows")
