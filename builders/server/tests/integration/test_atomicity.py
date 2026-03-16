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


def test_builder_failure_midrange_no_partial_insert(
    client, db_conn, write_temp_builder
):
    """builder that fails on 3rd of 5 timestamps -> 500, 0 rows."""
    name, version = write_temp_builder(
        "midrange-crash",
        "0.1.0",
        """\
name = "midrange-crash"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        # each timestamp runs in its own subprocess, so use
        # the timestamp itself to decide when to crash
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    if timestamp.day >= 3:
        raise RuntimeError("crash on day 3+")
    return [{"value": timestamp.day}]
""",
    )
    # request 5 days, builder crashes on the 3rd
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-01", "end": "2024-01-05"},
    )
    assert resp.status_code == 500

    # rows are accumulated in memory and bulk-inserted at the end,
    # so a mid-range crash means nothing gets inserted
    assert _row_count(db_conn, name) == 0


def test_dep_failure_blocks_parent(client, db_conn, write_temp_builder):
    """root dep crashes -> parent not built, 0 rows for both."""
    # create a root dep that crashes
    write_temp_builder(
        "crashing-root",
        "0.1.0",
        """\
name = "crashing-root"
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
    raise RuntimeError("root crash")
""",
    )
    # create a parent that depends on the crashing root
    name, version = write_temp_builder(
        "parent-of-crash",
        "0.1.0",
        """\
name = "parent-of-crash"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"

[dependencies]
crashing-root = "0.1.0"
""",
        """\
from datetime import datetime

def build(dependencies, timestamp: datetime) -> list[dict]:
    return [{"value": 1}]
""",
    )
    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 500
    assert _row_count(db_conn, "crashing-root") == 0
    assert _row_count(db_conn, name) == 0
