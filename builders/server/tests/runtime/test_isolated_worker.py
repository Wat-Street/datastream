import ast
import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

WORKER_PATH = (
    Path(__file__).resolve().parent.parent.parent / "runtime" / "isolated_worker.py"
)
SERVER_DIR = Path(__file__).resolve().parent.parent.parent


@pytest.fixture(scope="module")
def worker():
    """Import isolated_worker as a module for direct function testing."""
    spec = importlib.util.spec_from_file_location("isolated_worker", WORKER_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def test_isolated_worker_only_imports_stdlib():
    """All imports in isolated_worker.py must be stdlib modules."""
    source = WORKER_PATH.read_text()
    tree = ast.parse(source)
    non_stdlib = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top not in sys.stdlib_module_names:
                    non_stdlib.append(top)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            top = node.module.split(".")[0]
            if top not in sys.stdlib_module_names:
                non_stdlib.append(top)
    assert non_stdlib == [], f"non-stdlib imports found: {non_stdlib}"


def test_no_codebase_imports_isolated_worker():
    """No server code imports from isolated_worker."""
    for py_file in SERVER_DIR.rglob("*.py"):
        # skip tests and the worker itself
        if "tests" in py_file.parts or py_file.name == "isolated_worker.py":
            continue
        source = py_file.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                assert "isolated_worker" not in node.module, (
                    f"{py_file} imports from isolated_worker"
                )
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert "isolated_worker" not in alias.name, (
                        f"{py_file} imports isolated_worker"
                    )


# --- unit tests for helper functions ---


def test_deserialize_input_reconstructs_datetimes(worker):
    """_deserialize_input converts ISO strings back to datetime keys."""
    ts1 = datetime(2024, 1, 1, 9, 30)
    ts2 = datetime(2024, 1, 2, 15, 0)
    raw = json.dumps(
        {
            "builder_path": "/tmp/builder.py",
            "script_dir": "/tmp",
            "dependencies": {
                "prices": {
                    ts1.isoformat(): [{"close": 100}],
                    ts2.isoformat(): [{"close": 200}],
                },
            },
            "timestamp": ts1.isoformat(),
            "env_file": None,
        }
    ).encode()
    data = worker._deserialize_input(raw)
    assert data["timestamp"] == ts1
    assert ts1 in data["dependencies"]["prices"]
    assert ts2 in data["dependencies"]["prices"]


def test_deserialize_input_empty_deps(worker):
    """_deserialize_input handles empty dependencies dict."""
    ts = datetime(2024, 6, 15)
    raw = json.dumps(
        {
            "builder_path": "/tmp/b.py",
            "script_dir": "/tmp",
            "dependencies": {},
            "timestamp": ts.isoformat(),
            "env_file": None,
        }
    ).encode()
    data = worker._deserialize_input(raw)
    assert data["dependencies"] == {}
    assert data["timestamp"] == ts


def test_serialize_output_ok(worker):
    """_serialize_output wraps result in ok envelope."""
    out = json.loads(worker._serialize_output("ok", [{"val": 1}]))
    assert out == {"status": "ok", "result": [{"val": 1}]}


def test_serialize_output_error(worker):
    """_serialize_output wraps message in error envelope."""
    out = json.loads(worker._serialize_output("error", "boom"))
    assert out == {"status": "error", "message": "boom"}


def test_load_env_file_basic(worker, tmp_path: Path):
    """_load_env_file sets env vars from a simple .env file."""
    env = tmp_path / ".env"
    env.write_text("UNIT_KEY=unit_val\n")
    try:
        worker._load_env_file(str(env))
        assert os.environ.get("UNIT_KEY") == "unit_val"
    finally:
        os.environ.pop("UNIT_KEY", None)


def test_load_env_file_skips_comments_and_blanks(worker, tmp_path: Path):
    """_load_env_file ignores # comments and blank lines."""
    env = tmp_path / ".env"
    env.write_text("# comment\n\nA=1\n\n# another\nB=2\n")
    try:
        worker._load_env_file(str(env))
        assert os.environ.get("A") == "1"
        assert os.environ.get("B") == "2"
    finally:
        os.environ.pop("A", None)
        os.environ.pop("B", None)


def test_load_env_file_value_with_equals(worker, tmp_path: Path):
    """_load_env_file handles values that contain '='."""
    env = tmp_path / ".env"
    env.write_text("URL=http://example.com?a=1&b=2\n")
    try:
        worker._load_env_file(str(env))
        assert os.environ.get("URL") == "http://example.com?a=1&b=2"
    finally:
        os.environ.pop("URL", None)


# --- subprocess integration tests ---


def _run_worker(
    builder_code: str,
    deps: dict,
    timestamp: datetime,
    tmp_path: Path,
    env_file: str | None = None,
) -> dict:
    """Helper to run the worker as a subprocess with a temporary builder."""
    script_dir = tmp_path / "builder_dir"
    script_dir.mkdir(exist_ok=True)
    builder_path = script_dir / "builder.py"
    builder_path.write_text(builder_code)

    # build input payload (same format as serialization.serialize_input)
    serializable_deps: dict[str, dict[str, list[dict]]] = {}
    for dep_name, ts_map in deps.items():
        serializable_deps[dep_name] = {
            ts.isoformat(): rows for ts, rows in ts_map.items()
        }

    payload = json.dumps(
        {
            "builder_path": str(builder_path),
            "script_dir": str(script_dir),
            "dependencies": serializable_deps,
            "timestamp": timestamp.isoformat(),
            "env_file": env_file,
        }
    ).encode()

    result = subprocess.run(
        [sys.executable, str(WORKER_PATH)],
        input=payload,
        capture_output=True,
        timeout=10,
    )
    return json.loads(result.stdout)


def test_worker_successful_build(tmp_path: Path):
    """Worker runs a simple builder and returns result."""
    code = """
def build(dependencies, timestamp):
    return [{"value": 42}]
"""
    out = _run_worker(code, {}, datetime(2024, 1, 1), tmp_path)
    assert out["status"] == "ok"
    assert out["result"] == [{"value": 42}]


def test_worker_exception_handling(tmp_path: Path):
    """Worker catches builder exceptions and returns error status."""
    code = """
def build(dependencies, timestamp):
    raise ValueError("test error")
"""
    out = _run_worker(code, {}, datetime(2024, 1, 1), tmp_path)
    assert out["status"] == "error"
    assert "test error" in out["message"]


def test_worker_env_var_loading(tmp_path: Path):
    """Worker loads .env file and builder can read env vars."""
    code = """
import os
def build(dependencies, timestamp):
    return [{"key": os.environ.get("MY_KEY", "")}]
"""
    env_file = tmp_path / ".env"
    env_file.write_text("MY_KEY=hello123\n")
    out = _run_worker(
        code,
        {},
        datetime(2024, 1, 1),
        tmp_path,
        env_file=str(env_file),
    )
    assert out["status"] == "ok"
    assert out["result"] == [{"key": "hello123"}]


def test_worker_dependency_passthrough(tmp_path: Path):
    """Worker passes dependency data to builder correctly."""
    code = """
def build(dependencies, timestamp):
    rows = dependencies["prices"]
    # rows is {datetime: [dict]}
    all_rows = []
    for ts, data in rows.items():
        all_rows.extend(data)
    return [{"count": len(all_rows)}]
"""
    ts = datetime(2024, 1, 1)
    deps = {"prices": {ts: [{"close": 100}, {"close": 200}]}}
    out = _run_worker(code, deps, ts, tmp_path)
    assert out["status"] == "ok"
    assert out["result"] == [{"count": 2}]


def test_worker_env_comments_and_blanks(tmp_path: Path):
    """Worker .env parser skips comments and blank lines."""
    code = """
import os
def build(dependencies, timestamp):
    return [{
        "a": os.environ.get("A", ""),
        "b": os.environ.get("B", ""),
    }]
"""
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\n\nA=val_a\n# another\nB=val_b\n")
    out = _run_worker(
        code,
        {},
        datetime(2024, 1, 1),
        tmp_path,
        env_file=str(env_file),
    )
    assert out["status"] == "ok"
    assert out["result"] == [{"a": "val_a", "b": "val_b"}]
