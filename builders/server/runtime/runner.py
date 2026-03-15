import multiprocessing
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

TIMEOUT_SECONDS = 120

STATUS_OK = "ok"
STATUS_ERROR = "error"


def _worker(
    build_fn: Callable,
    dependencies: dict,
    timestamp: datetime,
    queue: multiprocessing.Queue,
    env_file: Path | None,
):
    """Subprocess target that runs the builder and puts the result in the queue."""
    try:
        if env_file is not None:
            env = dotenv_values(env_file)
            os.environ.update(env)  # type: ignore[arg-type]  # dotenv_values returns str values for non-None entries
        result = build_fn(dependencies, timestamp)
        queue.put((STATUS_OK, result))
    except Exception as e:
        queue.put((STATUS_ERROR, str(e)))


def run_builder(
    build_fn: Callable,
    dependencies: dict,
    timestamp: datetime,
    env_file: Path | None,
) -> list[dict]:
    """Run a builder function in an isolated subprocess and return its result."""

    # note: there is a performance overhead of using a multiprocessing queue as data has
    # to be serialized when passed between processes
    queue: multiprocessing.Queue[tuple[str, object]] = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_worker, args=(build_fn, dependencies, timestamp, queue, env_file)
    )
    proc.start()
    proc.join(timeout=TIMEOUT_SECONDS)

    # timeout on join, process is still alive
    if proc.is_alive():
        proc.kill()
        proc.join()
        raise RuntimeError(
            f"Builder timed out after {TIMEOUT_SECONDS}s for timestamp {timestamp}"
        )

    # at this point the subprocess is joined, check if it returned a result
    if queue.empty():
        raise RuntimeError(
            "Builder subprocess crashed without returning a result "
            f"for timestamp {timestamp}"
        )

    status, payload = queue.get()
    if status == STATUS_ERROR:
        raise RuntimeError(f"Builder failed for timestamp {timestamp}: {payload}")

    return payload  # type: ignore[return-value]  # STATUS_ERROR case is handled above, payload is always list[dict] here
