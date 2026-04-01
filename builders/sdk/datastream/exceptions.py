class DatastreamError(Exception):
    """Base exception for all datastream SDK errors."""


class DatastreamAPIError(DatastreamError):
    """Raised when the API returns a non-success status code."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API error {status_code}: {detail}")
