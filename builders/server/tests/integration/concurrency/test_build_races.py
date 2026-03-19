"""Concurrency race detection tests.

These tests use a real connection pool and advisory locks to verify
that concurrent build requests do not produce duplicate rows.
"""

import textwrap
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

# slow builder script: sleeps to widen the race window
SLOW_BUILDER_PY = textwrap.dedent("""\
    import time
    from datetime import datetime
    from typing import Any

    def build(
        dependencies: dict[str, dict[datetime, list[dict]]],
        timestamp: datetime,
    ) -> list[dict[str, Any]]:
        time.sleep(0.5)
        return [{"ticker": "TEST", "price": 100}]
""")

SLOW_BUILDER_CONFIG = textwrap.dedent("""\
    name = "slow-builder"
    version = "0.1.0"
    builder = "builder.py"
    calendar = "everyday"
    granularity = "1d"
    start-date = "2024-01-01"

    [schema]
    ticker = "str"
    price = "int"
""")

# slow root for dependency chain tests
SLOW_ROOT_CONFIG = textwrap.dedent("""\
    name = "slow-root"
    version = "0.1.0"
    builder = "builder.py"
    calendar = "everyday"
    granularity = "1d"
    start-date = "2024-01-01"

    [schema]
    ticker = "str"
    price = "int"
""")

CHILD_BUILDER_PY = textwrap.dedent("""\
    from datetime import datetime
    from typing import Any

    def build(
        dependencies: dict[str, dict[datetime, list[dict]]],
        timestamp: datetime,
    ) -> list[dict[str, Any]]:
        root_data = dependencies["slow-root"]
        rows = root_data[timestamp]
        return [{
            "ticker": rows[0]["ticker"],
            "derived": rows[0]["price"] * 2,
        }]
""")

CHILD_CONFIG = textwrap.dedent("""\
    name = "child-of-slow"
    version = "0.1.0"
    builder = "builder.py"
    calendar = "everyday"
    granularity = "1d"
    start-date = "2024-01-01"

    [schema]
    ticker = "str"
    derived = "int"

    [dependencies]
    slow-root = "0.1.0"
""")


def _count_duplicates(
    conninfo: str,
    dataset_name: str,
    version: str,
    expected_per_ts: int,
) -> list:
    """Return timestamps with more rows than expected."""
    conn = psycopg.connect(conninfo, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp, count(*) FROM datasets
            WHERE dataset_name = %s
              AND dataset_version = %s
            GROUP BY timestamp HAVING count(*) > %s
            """,
            (dataset_name, version, expected_per_ts),
        )
        dupes = cur.fetchall()
    conn.close()
    return dupes


def _count_rows(conninfo: str, dataset_name: str, version: str) -> int:
    """Return total row count for a dataset."""
    conn = psycopg.connect(conninfo, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM datasets "
            "WHERE dataset_name = %s "
            "AND dataset_version = %s",
            (dataset_name, version),
        )
        count = cur.fetchone()[0]
    conn.close()
    return count


@pytest.mark.integration
def test_concurrent_same_range_no_duplicates(client, write_temp_builder, conninfo):
    """3 threads build same (dataset, range). No duplicates."""
    write_temp_builder(
        "slow-builder",
        "0.1.0",
        SLOW_BUILDER_CONFIG,
        SLOW_BUILDER_PY,
    )

    def do_build(_):
        return client.post(
            "/api/v1/build/slow-builder/0.1.0",
            params={
                "start": "2024-01-01",
                "end": "2024-01-03",
            },
        )

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(do_build, range(3)))

    # all requests should succeed
    for r in results:
        assert r.status_code == 200, r.text

    # exactly 3 rows (1 per day, 3 days), no duplicates
    dupes = _count_duplicates(conninfo, "slow-builder", "0.1.0", 1)
    assert dupes == [], f"duplicate timestamps: {dupes}"

    total = _count_rows(conninfo, "slow-builder", "0.1.0")
    assert total == 3


@pytest.mark.integration
def test_concurrent_overlapping_ranges_no_duplicates(
    client, write_temp_builder, conninfo
):
    """2 threads build overlapping ranges. No duplicates."""
    write_temp_builder(
        "slow-builder",
        "0.1.0",
        SLOW_BUILDER_CONFIG,
        SLOW_BUILDER_PY,
    )

    def build_range_a(_):
        return client.post(
            "/api/v1/build/slow-builder/0.1.0",
            params={
                "start": "2024-01-01",
                "end": "2024-01-03",
            },
        )

    def build_range_b(_):
        return client.post(
            "/api/v1/build/slow-builder/0.1.0",
            params={
                "start": "2024-01-02",
                "end": "2024-01-04",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_a = pool.submit(build_range_a, None)
        fut_b = pool.submit(build_range_b, None)
        resp_a = fut_a.result()
        resp_b = fut_b.result()

    assert resp_a.status_code == 200, resp_a.text
    assert resp_b.status_code == 200, resp_b.text

    dupes = _count_duplicates(conninfo, "slow-builder", "0.1.0", 1)
    assert dupes == [], f"duplicate timestamps: {dupes}"

    # should have exactly 4 rows (Jan 1-4)
    total = _count_rows(conninfo, "slow-builder", "0.1.0")
    assert total == 4


@pytest.mark.integration
def test_concurrent_dependency_chain_no_duplicates(
    client, write_temp_builder, conninfo
):
    """2 threads build child depending on slow root. No dupes."""
    write_temp_builder(
        "slow-root",
        "0.1.0",
        SLOW_ROOT_CONFIG,
        SLOW_BUILDER_PY,
    )
    write_temp_builder(
        "child-of-slow",
        "0.1.0",
        CHILD_CONFIG,
        CHILD_BUILDER_PY,
    )

    def do_build(_):
        return client.post(
            "/api/v1/build/child-of-slow/0.1.0",
            params={
                "start": "2024-01-01",
                "end": "2024-01-02",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(do_build, range(2)))

    for r in results:
        assert r.status_code == 200, r.text

    # no duplicates on root
    dupes = _count_duplicates(conninfo, "slow-root", "0.1.0", 1)
    assert dupes == [], f"duplicate root timestamps: {dupes}"

    # no duplicates on child
    dupes = _count_duplicates(conninfo, "child-of-slow", "0.1.0", 1)
    assert dupes == [], f"duplicate child timestamps: {dupes}"


@pytest.mark.integration
def test_concurrent_builds_all_return_200(client, write_temp_builder):
    """All concurrent requests succeed (no 500s)."""
    write_temp_builder(
        "slow-builder",
        "0.1.0",
        SLOW_BUILDER_CONFIG,
        SLOW_BUILDER_PY,
    )

    def do_build(_):
        return client.post(
            "/api/v1/build/slow-builder/0.1.0",
            params={
                "start": "2024-01-01",
                "end": "2024-01-02",
            },
        )

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(do_build, range(5)))

    statuses = [r.status_code for r in results]
    assert all(s == 200 for s in statuses), f"unexpected status codes: {statuses}"
