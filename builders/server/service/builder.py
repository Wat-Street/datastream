import logging
from datetime import datetime, timedelta

import db.datasets
from runtime import config, loader, runner, validator
from runtime.config import GRANULARITY_MAP
from utils.semver import SemVer

logger = logging.getLogger(__name__)


# TODO (bryan): benchmark this, and optimize if needed
def generate_timestamps(
    start: datetime, end: datetime, granularity: str
) -> list[datetime]:
    """Generate all timestamps in [start, end] for the given granularity."""
    delta = GRANULARITY_MAP.get(granularity)
    if delta is None:
        raise ValueError(f"Unsupported granularity: {granularity}")
    timestamps, current = [], start
    while current <= end:
        timestamps.append(current)
        current += delta
    return timestamps


def validate_dependency_graph(
    dataset_name: str,
    dataset_version: SemVer,
) -> None:
    """Walk the dependency tree and validate granularity constraints.

    A dataset's granularity must be >= (coarser or equal to) each
    dependency's granularity. Raises ValueError if violated.
    """
    cfg = config.load_config(dataset_name, dataset_version)
    granularity = cfg.get("granularity", "1d")
    parent_delta = GRANULARITY_MAP[granularity]

    for dep_name, dep_info in cfg.get("dependencies", {}).items():
        dep_version = SemVer.parse(dep_info["version"])
        dep_cfg = config.load_config(dep_name, dep_version)
        dep_granularity = dep_cfg.get("granularity", "1d")
        dep_delta = GRANULARITY_MAP[dep_granularity]

        if parent_delta < dep_delta:
            raise ValueError(
                f"{dataset_name}/{dataset_version} has granularity "
                f"'{granularity}' which is finer than dependency "
                f"'{dep_name}/{dep_version}' with granularity "
                f"'{dep_granularity}'"
            )

        # recurse into dependency's own dependencies
        validate_dependency_graph(dep_name, dep_version)


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
    dependencies = cfg.get("dependencies", {})
    schema = cfg.get("schema", {})
    granularity = cfg.get("granularity", "1d")

    # enforce start-date
    start_date = datetime.strptime(cfg["start-date"], "%Y-%m-%d")
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
    for dep_name, dep_info in dependencies.items():
        dep_version = SemVer.parse(dep_info["version"])
        lookback: timedelta | None = dep_info["lookback"]
        dep_start = start - lookback if lookback is not None else start
        _build_recursive(dep_name, dep_version, dep_start, end)

    # determine which timestamps are missing
    all_timestamps = generate_timestamps(start, end, granularity)
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
        for dep_name, dep_info in dependencies.items():
            dep_version = SemVer.parse(dep_info["version"])
            lookback = dep_info["lookback"]

            if lookback is not None:
                # fetch time window [ts - lookback, ts]
                dep_rows = db.datasets.get_rows_range(
                    dep_name, dep_version, ts - lookback, ts
                )
            else:
                # no lookback, fetch just this timestamp
                dep_rows = db.datasets.get_rows(dep_name, dep_version, [ts])

            if not dep_rows:
                raise RuntimeError(
                    f"Dependency '{dep_name}/{dep_version}' "
                    f"missing data for timestamp {ts}"
                )
            dep_data[dep_name] = dep_rows

        # run builder in subprocess
        result = runner.run_builder(build_fn, dep_data, ts)

        # validate output against schema
        validator.validate_rows(result, schema)

        rows.append((ts, result))

    # bulk insert all rows
    db.datasets.insert_rows(dataset_name, dataset_version, rows)
    logger.info(f"{dataset_name}/{dataset_version}: inserted {len(rows)} rows")
