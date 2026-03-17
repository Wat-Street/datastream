"""
###########################################################################
#  ISOLATED WORKER -- runs inside builder subprocesses                    #
#                                                                         #
#  This file is executed by a DIFFERENT Python interpreter that may be     #
#  inside a per-builder virtual environment. It MUST NOT import anything   #
#  from the rest of the codebase (no server modules, no runtime/, no      #
#  utils/, nothing outside stdlib).                                       #
#                                                                         #
#  Only stdlib imports are allowed. If you add an import here, verify it   #
#  is in sys.stdlib_module_names first.                                    #
#                                                                         #
#  Reason: the builder venv will not have server packages installed, so    #
#  any non-stdlib import will fail at runtime.                             #
###########################################################################
"""

import importlib.util
import json
import os
import sys
import traceback
from datetime import datetime


def _deserialize_input(raw: bytes) -> dict:
    """Parse JSON stdin into builder arguments.

    Reconstructs datetime objects from ISO strings.
    """
    data = json.loads(raw)

    # reconstruct datetime keys in dependencies
    deps: dict[str, dict[datetime, list[dict]]] = {}
    for dep_name, ts_map in data["dependencies"].items():
        deps[dep_name] = {
            datetime.fromisoformat(ts_str): rows for ts_str, rows in ts_map.items()
        }

    data["dependencies"] = deps
    data["timestamp"] = datetime.fromisoformat(data["timestamp"])
    return data


def _serialize_output(status: str, payload: object) -> bytes:
    """Encode worker result as JSON bytes for stdout."""
    if status == "ok":
        obj = {"status": "ok", "result": payload}
    else:
        obj = {"status": "error", "message": str(payload)}
    return json.dumps(obj).encode("utf-8")


def _load_env_file(env_path: str) -> None:
    """Minimal .env parser -- read lines, split on first '=', skip comments."""
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            if not key:
                continue
            value = value.strip()

            # strip single or double quotes from the value
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ[key.strip()] = value


def main() -> None:
    raw = sys.stdin.buffer.read()
    data = _deserialize_input(raw)

    # add script dir to sys.path for relative imports
    script_dir = data["script_dir"]
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

    # load env vars if requested
    if data.get("env_file") is not None:
        _load_env_file(data["env_file"])

    # import and run the builder
    builder_path = data["builder_path"]
    spec = importlib.util.spec_from_file_location("builder", builder_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    result = module.build(data["dependencies"], data["timestamp"])
    sys.stdout.buffer.write(_serialize_output("ok", result))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        msg = traceback.format_exc()
        sys.stdout.buffer.write(_serialize_output("error", msg))
        sys.exit(1)
