import pytest

pytestmark = pytest.mark.integration


def _row_count(db_conn, dataset_name=None):
    with db_conn.cursor() as cur:
        if dataset_name:
            cur.execute(
                "SELECT count(*) FROM datasets WHERE dataset_name = %s",
                (dataset_name,),
            )
        else:
            cur.execute("SELECT count(*) FROM datasets")
        return cur.fetchone()[0]


def test_missing_config(client, db_conn):
    """nonexistent dataset -> 500, 0 rows."""
    resp = client.post(
        "/api/v1/build/nonexistent-dataset/0.1.0",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 500
    assert _row_count(db_conn) == 0


def test_invalid_version(client):
    """bad semver -> 400."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/not-a-version",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 400


def test_invalid_timestamp(client):
    """unparseable start/end -> 400."""
    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "not-a-date", "end": "2024-01-02"},
    )
    assert resp.status_code == 400

    resp = client.post(
        "/api/v1/build/mock-ohlc/0.1.0",
        params={"start": "2024-01-02", "end": "garbage"},
    )
    assert resp.status_code == 400


def test_builder_crash(client, db_conn, write_temp_builder):
    """builder raises exception -> 500, 0 rows in DB."""
    name, version = write_temp_builder(
        "crash-builder",
        "0.1.0",
        """\
name = "crash-builder"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    raise RuntimeError("intentional crash")
""",
    )
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 500
    assert _row_count(db_conn, name) == 0


def test_schema_validation_failure(client, db_conn, write_temp_builder):
    """builder returns wrong keys -> 500, 0 rows."""
    name, version = write_temp_builder(
        "bad-schema-keys",
        "0.1.0",
        """\
name = "bad-schema-keys"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
expected_key = "str"
""",
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    return [{"wrong_key": "value"}]
""",
    )
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 500
    assert _row_count(db_conn, name) == 0


def test_schema_type_mismatch(client, db_conn, write_temp_builder):
    """builder returns wrong types -> 500, 0 rows."""
    name, version = write_temp_builder(
        "bad-schema-types",
        "0.1.0",
        """\
name = "bad-schema-types"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    return [{"value": "not_an_int"}]
""",
    )
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 500
    assert _row_count(db_conn, name) == 0
