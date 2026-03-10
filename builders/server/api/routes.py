import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from service.builder import build_dataset
from utils.semver import SemVer

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/build/{dataset_name}/{dataset_version}")
def build(
    dataset_name: str,
    dataset_version: str,
    start: str = Query(...),
    end: str = Query(...),
):
    """Build missing data for a dataset in the given time range."""
    try:
        version = SemVer.parse(dataset_version)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"Invalid version: {dataset_version}"
        ) from exc

    try:
        start_ts = datetime.fromisoformat(start)
        end_ts = datetime.fromisoformat(end)
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="Invalid start/end timestamp"
        ) from exc

    try:
        build_dataset(dataset_name, version, start_ts, end_ts)
    except Exception as e:
        logger.exception(f"Build failed for {dataset_name}/{version}")
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {"status": "ok"}
