"""Single-job build worker.

Extracts the per-dataset build logic from the former ``_build_recursive()``
into a standalone ``execute_job()`` function. Given a ``JobDescriptor``,
the worker:

1. Generates valid timestamps for the job's range
2. Acquires the per-dataset lock
3. Checks which timestamps already exist in the DB
4. For each missing timestamp: fetches dep data, runs the builder
   subprocess, validates output against the schema
5. Bulk-inserts all rows on success (atomicity: no partial inserts)

The worker does NOT handle dependency graph walking or start-date
clamping -- those are the scheduler's responsibility.
"""

from __future__ import annotations

import threading
from datetime import datetime
from pathlib import Path

import db.datasets
import structlog
from runtime import config, registry, runner, validator

from service.locks import get_build_lock
from service.models import JobDescriptor, JobResult
from service.timestamps import NoValidTimestampsError, generate_timestamps

logger = structlog.get_logger()


def execute_job(
    job: JobDescriptor,
    cancelled: threading.Event,
) -> JobResult:
    """Execute a single build job: build missing timestamps for one dataset.

    This is the per-dataset build logic extracted from the former
    ``_build_recursive()``. The scheduler has already resolved the
    dependency graph and computed time ranges (with lookback expansion
    and start-date clamping), so the worker just builds.

    Atomicity: rows are accumulated in memory and bulk-inserted only
    after all timestamps succeed. If any timestamp fails, no rows are
    inserted for this job.

    Args:
        job: describes which dataset to build and over what time range.
        cancelled: shared event that signals early termination. checked
            between timestamps so a failed sibling job can stop peers.

    Returns:
        JobResult indicating success or failure with error detail.
    """
    try:
        _execute(job, cancelled)
        return JobResult(job=job, success=True)
    except NoValidTimestampsError:
        # let NoValidTimestampsError propagate so routes.py can return 422
        raise
    except Exception as exc:
        return JobResult(job=job, success=False, error=str(exc))


def _execute(
    job: JobDescriptor,
    cancelled: threading.Event,
) -> None:
    """Inner execution logic. Raises on any failure.

    Separated from execute_job() so the outer function can catch all
    exceptions uniformly and wrap them into a JobResult.
    """
    cfg = registry.get_config(job.dataset_name, job.dataset_version)

    # generate valid calendar timestamps for the range
    all_timestamps = generate_timestamps(
        job.start, job.end, cfg.granularity, cfg.calendar
    )

    if not all_timestamps:
        raise NoValidTimestampsError(
            f"{job.dataset_name}/{job.dataset_version}: no valid calendar "
            f"timestamps in range [{job.start}, {job.end}] "
            f"for calendar '{cfg.calendar.name}'"
        )

    # acquire per-dataset lock to prevent concurrent builds from racing
    # between the "check missing" read and "insert rows" write
    with get_build_lock(job.dataset_name, str(job.dataset_version)):
        existing = set(
            db.datasets.get_existing_timestamps(
                job.dataset_name, job.dataset_version, job.start, job.end
            )
        )
        missing = [ts for ts in all_timestamps if ts not in existing]

        if not missing:
            logger.info(
                "all timestamps present, skipping",
                dataset=job.dataset_name,
                version=str(job.dataset_version),
            )
            return

        logger.info(
            "building missing timestamps",
            dataset=job.dataset_name,
            version=str(job.dataset_version),
            count=len(missing),
        )

        # resolve env file if env-vars is enabled
        env_file: Path | None = None
        if cfg.env_vars:
            env_file = config.SCRIPTS_DIR / job.dataset_name / str(cfg.version) / ".env"
            if not env_file.exists():
                raise FileNotFoundError(
                    f"{job.dataset_name}/{job.dataset_version}: env-vars is "
                    f"enabled but {env_file} does not exist"
                )

        script_dir = config.SCRIPTS_DIR / job.dataset_name / str(cfg.version)

        # build each missing timestamp, accumulating rows in memory
        rows: list[tuple[datetime, list[dict]]] = []
        for ts in missing:
            # check cancellation between timestamps so a failed sibling
            # job can stop this worker early
            if cancelled.is_set():
                raise RuntimeError(
                    f"{job.dataset_name}/{job.dataset_version}: "
                    "build cancelled by failed sibling job"
                )

            dep_data = _fetch_dep_data(cfg, ts)

            result = runner.run_builder(
                script_dir, cfg.builder, dep_data, ts, env_file=env_file
            )
            validator.validate_rows(result, cfg.schema)
            rows.append((ts, result))

        # bulk insert -- only reached if all timestamps succeeded
        db.datasets.insert_rows(job.dataset_name, job.dataset_version, rows)
        logger.info(
            "inserted rows",
            dataset=job.dataset_name,
            version=str(job.dataset_version),
            count=len(rows),
        )


def _fetch_dep_data(
    cfg: config.DatasetConfig,
    ts: datetime,
) -> dict[str, dict[datetime, list[dict]]]:
    """Fetch dependency data for a single timestamp.

    For deps with lookback, fetches a range [ts - lookback_subtract, ts].
    For deps without lookback, fetches just the single timestamp.
    Raises RuntimeError if any dependency is missing data.
    """
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
            dep_rows = db.datasets.get_rows_timestamps(dep_name, dep_info.version, [ts])

        if not dep_rows:
            raise RuntimeError(
                f"Dependency '{dep_name}/{dep_info.version}' "
                f"missing data for timestamp {ts}"
            )
        dep_data[dep_name] = dep_rows

    return dep_data
