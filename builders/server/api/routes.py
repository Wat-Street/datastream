from datetime import datetime

import structlog
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from service.builder import NoValidTimestampsError, build_dataset, get_data
from utils.semver import SemVer

logger = structlog.get_logger()

router = APIRouter()


@router.get("/ping")
def ping():
    """Health check endpoint."""
    return {"status": "ok"}


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

    structlog.contextvars.bind_contextvars(
        dataset_name=dataset_name, version=str(version)
    )

    try:
        build_dataset(dataset_name, version, start_ts, end_ts)
    except NoValidTimestampsError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        logger.exception("build failed")
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {"status": "ok"}


@router.get("/data/{dataset_name}/{dataset_version}")
def data(
    dataset_name: str,
    dataset_version: str,
    start: str = Query(...),
    end: str = Query(...),
    build_data: bool = Query(True, alias="build-data"),
):
    """Fetch data for a dataset, optionally building missing data first."""
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

    structlog.contextvars.bind_contextvars(
        dataset_name=dataset_name, version=str(version)
    )

    try:
        result = get_data(
            dataset_name, version, start_ts, end_ts, build_data=build_data
        )
    except NoValidTimestampsError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        logger.exception("data fetch failed")
        raise HTTPException(status_code=500, detail=str(e)) from e

    rows = [
        {
            "timestamp": ts.isoformat(),
            "data": data_list,
        }
        for ts, data_list in sorted(result.data.items())
    ]

    body = {
        "dataset_name": dataset_name,
        "dataset_version": str(version),
        "total_timestamps": result.total_timestamps,
        "returned_timestamps": result.returned_timestamps,
        "rows": rows,
    }

    # return 206 when caller opted out of building and data is incomplete
    if not build_data and result.returned_timestamps < result.total_timestamps:
        return JSONResponse(content=body, status_code=206)

    return body
