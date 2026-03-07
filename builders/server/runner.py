import multiprocessing
from typing import Callable

import pandas as pd

TIMEOUT_SECONDS = 120

STATUS_OK = "ok"
STATUS_ERROR = "error"


def _worker(build_fn: Callable, dependencies: dict, timestamp: pd.Timestamp, queue: multiprocessing.Queue):
    """Subprocess target that runs the builder and puts the result in the queue."""
    try:
        result = build_fn(dependencies, timestamp)
        queue.put((STATUS_OK, result))
    except Exception as e:
        queue.put((STATUS_ERROR, str(e)))


def run_builder(build_fn: Callable, dependencies: dict, timestamp: pd.Timestamp) -> dict:
    """Run a builder function in an isolated subprocess and return its result."""

    # note: there is a performance overhead of using a multiprocessing queue as data has
    # to be serialized when passed between processes
    queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=_worker, args=(build_fn, dependencies, timestamp, queue))
    proc.start()
    proc.join(timeout=TIMEOUT_SECONDS)

    if proc.is_alive():
        proc.kill()
        proc.join()
        raise RuntimeError(f"Builder timed out after {TIMEOUT_SECONDS}s for timestamp {timestamp}")

    if queue.empty():
        raise RuntimeError(f"Builder subprocess crashed without returning a result for timestamp {timestamp}")

    status, payload = queue.get()
    if status == STATUS_ERROR:
        raise RuntimeError(f"Builder failed for timestamp {timestamp}: {payload}")

    return payload
