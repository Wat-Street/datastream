from datetime import datetime

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from core.auth import verify_api_key
from core.github import GitHubError
from core.service.builder import (
    DatasetNotFoundError,
    NoDataInRangeError,
    NoValidTimestampsError,
    build_dataset,
    delete_data,
    get_data,
)
from core.service.catalog import list_datasets
from core.service.proposals import (
    DatasetProposal,
    InvalidProposalError,
    ProposalConflictError,
    ProposedDependency,
    propose_dataset,
)
from core.utils.semver import SemVer

logger = structlog.get_logger()

# public_router carries unauthenticated endpoints (health check); router carries
# the endpoints that main.py mounts behind the api-key dependency
public_router = APIRouter()
router = APIRouter()


@public_router.get("/status")
def status():
    """Health check endpoint (unauthenticated, used by the docker healthcheck)."""
    return {"status": "ok"}


@router.get("/datasets")
def datasets_list() -> dict:
    """List all discovered datasets with their data presence status."""
    try:
        items = list_datasets()
    except Exception as e:
        logger.exception("datasets list failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
    return {
        "datasets": [
            {"name": item.name, "version": item.version, "has_data": item.has_data}
            for item in items
        ]
    }


class DependencyIn(BaseModel):
    name: str
    version: str
    lookback: str | None = None


class DatasetProposalIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    version: str
    calendar: str
    granularity: str
    start_date: str
    builder_script: str
    author_name: str
    team: str
    discord_user: str
    description: str
    # 'schema' shadows a deprecated BaseModel attr, so alias it
    data_schema: dict[str, str] = Field(alias="schema")
    dependencies: list[DependencyIn] = Field(default_factory=list)
    env_vars: bool = False
    requirements_txt: str | None = None
    env_template: str | None = None


@router.post("/datasets")
def datasets_propose(
    payload: DatasetProposalIn, team: str = Depends(verify_api_key)
) -> dict:
    """Propose a new dataset: validate the submission and open a GitHub PR.

    Nothing is written to the server; the dataset goes live only after the
    PR is reviewed, merged, and the server restarts.
    """
    structlog.contextvars.bind_contextvars(
        dataset_name=payload.name, version=payload.version
    )
    proposal = DatasetProposal(
        name=payload.name,
        version=payload.version,
        calendar=payload.calendar,
        granularity=payload.granularity,
        start_date=payload.start_date,
        schema=payload.data_schema,
        builder_script=payload.builder_script,
        author_name=payload.author_name,
        team=payload.team,
        discord_user=payload.discord_user,
        description=payload.description,
        dependencies=[
            ProposedDependency(name=d.name, version=d.version, lookback=d.lookback)
            for d in payload.dependencies
        ],
        env_vars=payload.env_vars,
        requirements_txt=payload.requirements_txt,
        env_template=payload.env_template,
    )

    try:
        result = propose_dataset(proposal, requested_by=team)
    except InvalidProposalError as e:
        logger.warning("proposal rejected", error=str(e))
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ProposalConflictError as e:
        logger.warning("proposal conflict", error=str(e))
        raise HTTPException(status_code=409, detail=str(e)) from e
    except GitHubError as e:
        logger.exception("proposal github call failed")
        raise HTTPException(status_code=502, detail=f"github error: {e.message}") from e
    except Exception as e:
        logger.exception("proposal failed")
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {
        "dataset_name": payload.name,
        "dataset_version": payload.version,
        "pr_url": result.pr_url,
        "branch": result.branch,
    }


@router.post("/build/{dataset_name}/{dataset_version}")
def build(
    dataset_name: str,
    dataset_version: str,
    start: str = Query(...),
    end: str = Query(...),
    dry_run: bool = Query(False, alias="dry-run"),
):
    """Build missing data for a dataset in the given time range.

    With ``dry-run=true``, builders write to an in-memory store, not the database,
    and return the data.
    """
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
        dataset_name=dataset_name, version=str(version), dry_run=dry_run
    )

    try:
        produced = build_dataset(
            dataset_name, version, start_ts, end_ts, dry_run=dry_run
        )
    except NoValidTimestampsError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        logger.exception("build failed")
        raise HTTPException(status_code=500, detail=str(e)) from e

    if not dry_run:
        return {"status": "ok"}

    rows = [
        {"timestamp": ts.isoformat(), "data": data_list}
        for ts, data_list in sorted((produced or {}).items())
    ]
    return {
        "dataset_name": dataset_name,
        "dataset_version": str(version),
        "dry_run": True,
        "rows": rows,
    }


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


@router.delete("/data/{dataset_name}/{dataset_version}")
def delete(
    dataset_name: str,
    dataset_version: str,
    start: str = Query(...),
    end: str = Query(...),
):
    """Delete a dataset's rows in [start, end].

    No calendar check: whatever rows exist in the range are deleted. Returns
    the deleted row count and the actual range of deleted timestamps.
    """
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
        result = delete_data(dataset_name, version, start_ts, end_ts)
    except DatasetNotFoundError as e:
        logger.warning("delete failed: dataset not found", error=str(e))
        raise HTTPException(status_code=404, detail=str(e)) from e
    except NoDataInRangeError as e:
        logger.warning("delete failed: no data in range", error=str(e))
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("delete failed")
        raise HTTPException(status_code=500, detail=str(e)) from e

    return {
        "dataset_name": dataset_name,
        "dataset_version": str(version),
        "rows_deleted": result.rows_deleted,
        "start": result.start.isoformat(),
        "end": result.end.isoformat(),
    }
