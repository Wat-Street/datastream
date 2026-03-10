import multiprocessing
from collections.abc import Callable
from datetime import datetime

TIMEOUT_SECONDS = 120

STATUS_OK = "ok"
STATUS_ERROR = "error"


def _worker(
    build_fn: Callable,
    dependencies: dict,
    timestamp: datetime,
    queue: multiprocessing.Queue,
):
    """Subprocess target that runs the builder and puts the result in the queue."""
    try:
        result = build_fn(dependencies, timestamp)
        queue.put((STATUS_OK, result))
    except Exception as e:
        queue.put((STATUS_ERROR, str(e)))


def run_builder(
    build_fn: Callable, dependencies: dict, timestamp: datetime
) -> list[dict]:
    """Run a builder function in an isolated subprocess and return its result."""

    # note: there is a performance overhead of using a multiprocessing queue as data has
    # to be serialized when passed between processes
    queue: multiprocessing.Queue[tuple[str, object]] = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_worker, args=(build_fn, dependencies, timestamp, queue)
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
