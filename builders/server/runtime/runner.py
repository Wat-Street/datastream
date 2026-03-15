import importlib.util
import multiprocessing
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

TIMEOUT_SECONDS = 120

STATUS_OK = "ok"
STATUS_ERROR = "error"


def _worker(
    script_dir: Path,
    builder_filename: str,
    dependencies: dict,
    timestamp: datetime,
    queue: multiprocessing.Queue,
    env_file: Path | None,
):
    """Load and run the builder, putting the result in the queue."""
    try:
        if env_file is not None:
            env = dotenv_values(env_file)
            os.environ.update(env)  # type: ignore[arg-type]  # dotenv_values returns str values for non-None entries

        # add script dir to sys.path for relative imports
        str_dir = str(script_dir)
        if str_dir not in sys.path:
            sys.path.insert(0, str_dir)

        builder_path = script_dir / builder_filename
        spec = importlib.util.spec_from_file_location("builder", builder_path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]  # loader is checked non-None above

        result = module.build(dependencies, timestamp)
        queue.put((STATUS_OK, result))
    except Exception as e:
        queue.put((STATUS_ERROR, str(e)))


def run_builder(
    script_dir: Path,
    builder_filename: str,
    dependencies: dict,
    timestamp: datetime,
    env_file: Path | None,
) -> list[dict]:
    """Run a builder script in an isolated subprocess and return its result."""

    # note: there is a performance overhead of using a multiprocessing queue as data has
    # to be serialized when passed between processes
    queue: multiprocessing.Queue[tuple[str, object]] = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_worker,
        args=(script_dir, builder_filename, dependencies, timestamp, queue, env_file),
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
