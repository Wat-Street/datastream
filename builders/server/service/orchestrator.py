"""Build orchestrator: level-by-level execution of a topological build plan.

Given a dataset and time range, the orchestrator:

1. Calls ``schedule_build()`` to produce a ``BuildPlan`` (topological
   levels via Kahn's algorithm)
2. Iterates levels sequentially (barrier model: all level N jobs must
   complete before any level N+1 job starts)
3. Within each level, executes jobs sequentially via ``execute_job()``
   (MVP single worker -- future: parallelize within levels)
4. On any job failure: sets a ``cancelled`` event so in-flight siblings
   can terminate early, then raises ``RuntimeError``

The orchestrator does not touch the DB directly -- that is the worker's
responsibility. It only coordinates execution order.
"""

import threading
from datetime import datetime

import structlog
from utils.semver import SemVer

from service.scheduler import schedule_build
from service.worker import execute_job

logger = structlog.get_logger()


def run_build(
    dataset_name: str,
    dataset_version: SemVer,
    start: datetime,
    end: datetime,
) -> None:
    """Orchestrate a full build: schedule, then execute level by level.

    Produces a topological build plan via ``schedule_build()`` and
    executes it. Levels are barriers -- all jobs in level N complete
    before level N+1 starts. Within a level, jobs are independent
    (currently executed sequentially; safe to parallelize later).

    If any job fails, remaining jobs in the current level and all
    subsequent levels are skipped. A ``cancelled`` event is set so
    any in-flight workers (future parallel execution) can terminate
    early.

    Args:
        dataset_name: root dataset to build
        dataset_version: version of the root dataset
        start: requested build start time
        end: requested build end time

    Raises:
        ValueError: if end < start_date for any dataset (from scheduler)
        RuntimeError: if any job fails during execution
        NoValidTimestampsError: if a job has no valid calendar timestamps
    """
    plan = schedule_build(dataset_name, dataset_version, start, end)
    cancelled = threading.Event()

    logger.info(
        "build plan ready",
        dataset=dataset_name,
        version=str(dataset_version),
        levels=len(plan.levels),
        total_jobs=sum(len(level) for level in plan.levels),
    )

    for level_idx, jobs in enumerate(plan.levels):
        logger.info(
            "starting level",
            level=level_idx,
            jobs=[j.dataset_name for j in jobs],
        )

        for job in jobs:
            result = execute_job(job, cancelled)

            if not result.success:
                cancelled.set()
                raise RuntimeError(
                    f"build failed for {job.dataset_name}/"
                    f"{job.dataset_version}: {result.error}"
                )

        logger.info("level complete", level=level_idx)
