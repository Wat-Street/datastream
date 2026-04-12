from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def _row_count(db_conn, dataset_name):
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM datasets WHERE dataset_name = %s",
            (dataset_name,),
        )
        return cur.fetchone()[0]


def _read_attempt_count(counter_file: Path) -> int:
    return int(counter_file.read_text())


def test_retry_recovers_after_transient_crash(
    client, db_conn, write_temp_builder, tmp_path
):
    """builder crash on first attempt is retried and eventually succeeds."""
    counter_file = tmp_path / "attempt_count.txt"
    counter_file.write_text("0")

    name, version = write_temp_builder(
        "retry-transient",
        "0.1.0",
        """\
name = "retry-transient"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        f"""\
import os
from datetime import datetime

COUNTER_FILE = r"{counter_file}"

def build(dependencies, timestamp: datetime) -> list[dict]:
    count = int(open(COUNTER_FILE).read()) + 1
    open(COUNTER_FILE, "w").write(str(count))
    if count == 1:
        os._exit(1)
    return [{{"value": 1}}]
""",
    )

    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 200
    assert _read_attempt_count(counter_file) == 2
    assert _row_count(db_conn, name) == 1


def test_retry_exhausts_after_three_retries(
    client, db_conn, write_temp_builder, tmp_path
):
    """builder that always crashes fails after initial attempt plus 3 retries."""
    counter_file = tmp_path / "attempt_count.txt"
    counter_file.write_text("0")

    name, version = write_temp_builder(
        "retry-exhaust",
        "0.1.0",
        """\
name = "retry-exhaust"
version = "0.1.0"
builder = "builder.py"
calendar = "everyday"
granularity = "1d"
start-date = "2020-01-01"

[schema]
value = "int"
""",
        f"""\
import os
from datetime import datetime

COUNTER_FILE = r"{counter_file}"

def build(dependencies, timestamp: datetime) -> list[dict]:
    count = int(open(COUNTER_FILE).read()) + 1
    open(COUNTER_FILE, "w").write(str(count))
    os._exit(1)
""",
    )

    resp = client.post(
        f"/api/v1/build/{name}/{version}",
        params={"start": "2024-01-02", "end": "2024-01-02"},
    )
    assert resp.status_code == 500
    assert _read_attempt_count(counter_file) == 4
    assert _row_count(db_conn, name) == 0
