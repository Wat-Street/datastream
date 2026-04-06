import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import structlog
from utils.retry import retry_with_backoff

from runtime.serialization import (
    WorkerError,
    WorkerSuccess,
    deserialize_output,
    serialize_input,
)

logger = structlog.get_logger()

TIMEOUT_SECONDS = 120
RETRY_MAX_RETRIES = 5
RETRY_INITIAL_DELAY = 2.0  # seconds
RETRY_BACKOFF_FACTOR = 2.0

WORKER_PATH = Path(__file__).parent / "isolated_worker.py"


def run_builder(
    script_dir: Path,
    builder_filename: str,
    dependencies: dict,
    timestamp: datetime,
    env_file: Path | None,
) -> list[dict]:
    """Run a builder script in an isolated subprocess with retry on failure."""
    # use per-builder venv python if available, otherwise system python
    venv_python = script_dir / ".venv" / "bin" / "python"
    python = str(venv_python) if venv_python.exists() else sys.executable
    # TODO: use multiprocessing.Process for stdlib-only builders (no venv)
    # since fork is faster than spawning a new interpreter via Popen.
    # only use Popen when a venv is present and we need a different
    # python interpreter.
    builder_path = script_dir / builder_filename

    payload = serialize_input(
        builder_path, script_dir, dependencies, timestamp, env_file
    )

    def _execute_subprocess() -> list[dict]:
        """Run the builder subprocess once, raising on any failure."""
        logger.info(
            "subprocess started",
            python=python,
            builder=str(builder_path),
            timestamp=str(timestamp),
        )

        start_time = time.monotonic()

        proc = subprocess.Popen(
            [python, str(WORKER_PATH)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            stdout, stderr = proc.communicate(input=payload, timeout=TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            proc.kill()
            proc.wait()
            logger.error(
                "subprocess timed out",
                builder=str(builder_path),
                timestamp=str(timestamp),
                timeout_seconds=TIMEOUT_SECONDS,
            )
            raise RuntimeError(
                f"Builder timed out after {TIMEOUT_SECONDS}s for timestamp {timestamp}"
            ) from exc

        duration = time.monotonic() - start_time

        # non-zero exit with no stdout means the process crashed hard
        if proc.returncode != 0 and not stdout:
            logger.error(
                "subprocess crashed",
                builder=str(builder_path),
                timestamp=str(timestamp),
                exit_code=proc.returncode,
                stderr=stderr.decode(errors="replace"),
            )
            raise RuntimeError(
                "Builder subprocess crashed without returning a result "
                f"for timestamp {timestamp}"
            )

        logger.info(
            "subprocess completed",
            builder=str(builder_path),
            exit_code=proc.returncode,
            duration_s=round(duration, 3),
        )

        if stderr and proc.returncode == 0:
            logger.warning(
                "subprocess stderr output",
                builder=str(builder_path),
                stderr=stderr.decode(errors="replace"),
            )

        out = deserialize_output(stdout)
        match out:
            case WorkerSuccess(result=result):
                return result
            case WorkerError(message=msg):
                raise RuntimeError(f"Builder failed for timestamp {timestamp}: {msg}")

        raise RuntimeError(  # pragma: no cover
            f"Unexpected worker output for timestamp {timestamp}"
        )

    return retry_with_backoff(
        _execute_subprocess,
        max_retries=RETRY_MAX_RETRIES,
        initial_delay=RETRY_INITIAL_DELAY,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        description=f"builder subprocess {builder_path} @ {timestamp}",
    )
