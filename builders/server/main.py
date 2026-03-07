import logging

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

import config
import db
import loader
import runner
import validator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

GRANULARITY_MAP = {
    "1s": "s",
    "1m": "min",
    "1h": "h",
    "1d": "D",
}


def generate_timestamps(start: pd.Timestamp, end: pd.Timestamp, granularity: str) -> list[pd.Timestamp]:
    """Generate all timestamps in [start, end] for the given granularity."""
    freq = GRANULARITY_MAP.get(granularity)
    if freq is None:
        raise ValueError(f"Unsupported granularity: {granularity}")
    return list(pd.date_range(start=start, end=end, freq=freq))


@app.post("/build/{dataset_name}/{dataset_version}")
def build(
    dataset_name: str,
    dataset_version: str,
    start: str = Query(...),
    end: str = Query(...),
):
    """Build missing data for a dataset in the given time range."""
    try:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid start/end timestamp")

    try:
        _build_dataset(dataset_name, dataset_version, start_ts, end_ts)
    except Exception as e:
        logger.exception(f"Build failed for {dataset_name}/{dataset_version}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "ok"}


def _build_dataset(
    dataset_name: str,
    dataset_version: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> None:
    """Core build logic: resolve dependencies, build missing timestamps, insert rows."""
    cfg = config.load_config(dataset_name, dataset_version)
    dependencies = cfg.get("dependencies", {})
    schema = cfg.get("schema", {})
    granularity = cfg.get("granularity", "1d")

    # Recursively build dependencies first
    for dep_name, dep_version in dependencies.items():
        _build_dataset(dep_name, dep_version, start, end)

    # Determine which timestamps are missing
    all_timestamps = generate_timestamps(start, end, granularity)
    existing = set(db.get_existing_timestamps(dataset_name, dataset_version, start, end))
    missing = [ts for ts in all_timestamps if ts not in existing]

    if not missing:
        logger.info(f"{dataset_name}/{dataset_version}: all timestamps present, skipping")
        return

    logger.info(f"{dataset_name}/{dataset_version}: building {len(missing)} missing timestamps")

    # Load the builder function
    build_fn = loader.load_builder(dataset_name, dataset_version)

    # Build each missing timestamp
    rows = []
    for ts in missing:
        # Fetch dependency data for this timestamp
        dep_data = {}
        for dep_name, dep_version in dependencies.items():
            dep_rows = db.get_rows(dep_name, dep_version, [ts])
            if ts not in dep_rows:
                raise RuntimeError(
                    f"Dependency '{dep_name}/{dep_version}' missing data for timestamp {ts}"
                )
            dep_data[dep_name] = dep_rows[ts]

        # Run builder in subprocess
        result = runner.run_builder(build_fn, dep_data, ts)

        # Validate output against schema
        validator.validate(result, schema)

        rows.append((ts, result))

    # Bulk insert all rows
    db.insert_rows(dataset_name, dataset_version, rows)
    logger.info(f"{dataset_name}/{dataset_version}: inserted {len(rows)} rows")
