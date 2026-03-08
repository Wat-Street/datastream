import logging

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from service.builder import build_dataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


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
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="Invalid start/end timestamp"
        ) from exc

    try:
        build_dataset(dataset_name, dataset_version, start_ts, end_ts)
    except Exception as e:
        logger.exception(f"Build failed for {dataset_name}/{dataset_version}")
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {"status": "ok"}
