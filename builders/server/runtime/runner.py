import subprocess
import sys
from datetime import datetime
from pathlib import Path

from runtime.serialization import (
    WorkerError,
    WorkerSuccess,
    deserialize_output,
    serialize_input,
)

TIMEOUT_SECONDS = 120

WORKER_PATH = Path(__file__).parent / "isolated_worker.py"


def run_builder(
    script_dir: Path,
    builder_filename: str,
    dependencies: dict,
    timestamp: datetime,
    env_file: Path | None,
) -> list[dict]:
    """Run a builder script in an isolated subprocess and return its result."""
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
        raise RuntimeError(
            f"Builder timed out after {TIMEOUT_SECONDS}s for timestamp {timestamp}"
        ) from exc

    # non-zero exit with no stdout means the process crashed hard
    if proc.returncode != 0 and not stdout:
        raise RuntimeError(
            "Builder subprocess crashed without returning a result "
            f"for timestamp {timestamp}"
        )

    out = deserialize_output(stdout)
    match out:
        case WorkerSuccess(result=result):
            return result
        case WorkerError(message=msg):
            raise RuntimeError(f"Builder failed for timestamp {timestamp}: {msg}")
