"""Serialization utilities for subprocess IPC.

Handles converting builder inputs/outputs to/from JSON for communication
between the main server process and isolated builder subprocesses.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkerSuccess:
    """Successful builder subprocess result."""

    result: list[dict[str, Any]]


@dataclass(frozen=True)
class WorkerError:
    """Failed builder subprocess result."""

    message: str


def serialize_input(
    builder_path: Path,
    script_dir: Path,
    dependencies: dict[str, dict[datetime, list[dict]]],
    timestamp: datetime,
    env_file: Path | None,
) -> bytes:
    """Serialize builder inputs to JSON bytes for subprocess stdin.

    Converts datetime keys to ISO strings since JSON only supports string keys.
    """
    # TODO: optimize for large dependency payloads (e.g. msgpack, streaming)

    # convert datetime keys to ISO strings
    serializable_deps: dict[str, dict[str, list[dict]]] = {}
    for dep_name, ts_map in dependencies.items():
        serializable_deps[dep_name] = {
            ts.isoformat(): rows for ts, rows in ts_map.items()
        }

    payload = {
        "builder_path": str(builder_path),
        "script_dir": str(script_dir),
        "dependencies": serializable_deps,
        "timestamp": timestamp.isoformat(),
        "env_file": str(env_file) if env_file is not None else None,
    }
    return json.dumps(payload).encode("utf-8")


def deserialize_output(json_bytes: bytes) -> WorkerSuccess | WorkerError:
    """Parse worker JSON output.

    Expected format: {"status": "ok"|"error", "result"|"message": ...}
    """
    # TODO: optimize deserialization for large result sets
    data = json.loads(json_bytes)
    if data["status"] == "ok":
        return WorkerSuccess(result=data["result"])
    else:
        return WorkerError(message=data["message"])
