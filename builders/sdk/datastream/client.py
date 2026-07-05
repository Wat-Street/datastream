from __future__ import annotations

from datetime import datetime

import httpx

from datastream.config import get_api_key, get_base_url
from datastream.exceptions import DatastreamAPIError
from datastream.types import (
    DatasetName,
    DatasetResponse,
    DatasetRow,
    DatasetVersion,
)


class DatastreamClient:
    """HTTP client for the datastream API."""

    def __init__(
        self,
        base_url: str | None = None,
        transport: httpx.BaseTransport | None = None,
        api_key: str | None = None,
    ) -> None:
        self._base_url = base_url or get_base_url()
        self._transport = transport
        # resolve the api key from the argument, then config/env; sent as a bearer token
        key = api_key or get_api_key()
        self._headers = {"Authorization": f"Bearer {key}"} if key else {}

    def get_data(
        self,
        name: str | DatasetName,
        version: str | DatasetVersion,
        start: datetime,
        end: datetime,
        *,
        build_data: bool = True,
    ) -> DatasetResponse:
        """Fetch dataset data for a time range."""
        if isinstance(version, str):
            version = DatasetVersion.parse(version)

        url = f"{self._base_url}/data/{name}/{version}"
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "build-data": str(build_data).lower(),
        }

        with httpx.Client(transport=self._transport, headers=self._headers) as http:
            resp = http.get(url, params=params)

        if resp.status_code not in (200, 206):
            raise DatastreamAPIError(
                status_code=resp.status_code,
                detail=resp.text,
            )

        body = resp.json()
        rows = [
            DatasetRow(
                timestamp=datetime.fromisoformat(r["timestamp"]),
                data=r["data"],
            )
            for r in body["rows"]
        ]

        return DatasetResponse(
            dataset_name=body["dataset_name"],
            dataset_version=DatasetVersion.parse(body["dataset_version"]),
            total_timestamps=body["total_timestamps"],
            returned_timestamps=body["returned_timestamps"],
            rows=rows,
        )


def get_data(
    name: str | DatasetName,
    version: str | DatasetVersion,
    start: datetime,
    end: datetime,
    *,
    build_data: bool = True,
    api_key: str | None = None,
) -> DatasetResponse:
    """Convenience function using default config."""
    return DatastreamClient(api_key=api_key).get_data(
        name, version, start, end, build_data=build_data
    )
