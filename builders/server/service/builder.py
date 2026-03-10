import logging
from datetime import datetime

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


def build_dataset(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> None:
    """Core build logic: resolve dependencies, build missing timestamps, insert rows."""
    cfg = config.load_config(dataset_name, dataset_version)
    dependencies = cfg.get("dependencies", {})
    schema = cfg.get("schema", {})
    granularity = cfg.get("granularity", "1d")

    # recursively build dependencies first
    for dep_name, dep_version_str in dependencies.items():
        build_dataset(dep_name, SemVer.parse(dep_version_str), start, end)

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
        dep_data = {}
        for dep_name, dep_version_str in dependencies.items():
            dep_version = SemVer.parse(dep_version_str)
            dep_rows = db.datasets.get_rows(dep_name, dep_version, [ts])
            if ts not in dep_rows:
                raise RuntimeError(
                    f"Dependency '{dep_name}/{dep_version}' "
                    f"missing data for timestamp {ts}"
                )
            dep_data[dep_name] = dep_rows[ts]

        # run builder in subprocess
        result = runner.run_builder(build_fn, dep_data, ts)

        # validate output against schema
        validator.validate_rows(result, schema)

        rows.append((ts, result))

    # bulk insert all rows
    db.datasets.insert_rows(dataset_name, dataset_version, rows)
    logger.info(f"{dataset_name}/{dataset_version}: inserted {len(rows)} rows")
