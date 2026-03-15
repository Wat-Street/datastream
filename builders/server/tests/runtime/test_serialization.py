import json
from datetime import datetime
from pathlib import Path

import pytest
from runtime.serialization import (
    WorkerError,
    WorkerSuccess,
    deserialize_output,
    serialize_input,
)


def test_round_trip_datetime_keys():
    """Datetime keys survive serialization and can be reconstructed."""
    ts1 = datetime(2024, 1, 1, 9, 30)
    ts2 = datetime(2024, 1, 2, 9, 30)
    deps = {"prices": {ts1: [{"close": 100}], ts2: [{"close": 200}]}}

    payload = serialize_input(
        Path("/tmp/builder.py"), Path("/tmp"), deps, ts1, env_file=None
    )
    data = json.loads(payload)

    # datetime keys converted to ISO strings
    assert ts1.isoformat() in data["dependencies"]["prices"]
    assert ts2.isoformat() in data["dependencies"]["prices"]


def test_nested_dependency_structure():
    """Multiple deps, multiple timestamps, multiple rows all serialize correctly."""
    ts = datetime(2024, 6, 15)
    deps = {
        "ohlc": {
            ts: [
                {"ticker": "AAPL", "close": 150},
                {"ticker": "MSFT", "close": 300},
            ]
        },
        "volume": {ts: [{"ticker": "AAPL", "vol": 1000000}]},
    }

    payload = serialize_input(
        Path("/tmp/builder.py"), Path("/tmp/scripts"), deps, ts, env_file=None
    )
    data = json.loads(payload)

    assert len(data["dependencies"]["ohlc"][ts.isoformat()]) == 2
    assert len(data["dependencies"]["volume"][ts.isoformat()]) == 1
    assert data["dependencies"]["ohlc"][ts.isoformat()][0]["ticker"] == "AAPL"


def test_empty_deps():
    """Empty dependencies dict serializes correctly."""
    ts = datetime(2024, 1, 1)
    payload = serialize_input(Path("/tmp/b.py"), Path("/tmp"), {}, ts, env_file=None)
    data = json.loads(payload)
    assert data["dependencies"] == {}


def test_none_env_file():
    """None env_file serializes as null."""
    ts = datetime(2024, 1, 1)
    payload = serialize_input(Path("/tmp/b.py"), Path("/tmp"), {}, ts, env_file=None)
    data = json.loads(payload)
    assert data["env_file"] is None


def test_env_file_path_serialized():
    """Non-None env_file is serialized as string."""
    ts = datetime(2024, 1, 1)
    env = Path("/tmp/.env")
    payload = serialize_input(Path("/tmp/b.py"), Path("/tmp"), {}, ts, env_file=env)
    data = json.loads(payload)
    assert data["env_file"] == "/tmp/.env"


def test_single_row_output():
    """Single-row builder output deserializes correctly."""
    raw = json.dumps({"status": "ok", "result": [{"value": 42}]}).encode()
    out = deserialize_output(raw)
    match out:
        case WorkerSuccess(result=result):
            assert result == [{"value": 42}]
        case _:
            pytest.fail("expected WorkerSuccess")


def test_multi_row_output():
    """Multi-row builder output deserializes correctly."""
    rows = [{"ticker": "AAPL", "close": 150}, {"ticker": "MSFT", "close": 300}]
    raw = json.dumps({"status": "ok", "result": rows}).encode()
    out = deserialize_output(raw)
    match out:
        case WorkerSuccess(result=result):
            assert result == rows
        case _:
            pytest.fail("expected WorkerSuccess")


def test_error_output():
    """Error status returns WorkerError with message."""
    raw = json.dumps({"status": "error", "message": "boom"}).encode()
    out = deserialize_output(raw)
    match out:
        case WorkerError(message=msg):
            assert msg == "boom"
        case _:
            pytest.fail("expected WorkerError")


def test_paths_serialized_as_strings():
    """Path arguments become strings in JSON."""
    ts = datetime(2024, 1, 1)
    payload = serialize_input(
        Path("/a/builder.py"), Path("/a/scripts"), {}, ts, env_file=None
    )
    data = json.loads(payload)
    assert data["builder_path"] == "/a/builder.py"
    assert data["script_dir"] == "/a/scripts"
