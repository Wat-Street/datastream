import os

_base_url: str = "http://localhost:3000/api/v1"
_api_key: str | None = None


def configure(base_url: str | None = None, api_key: str | None = None) -> None:
    """Set the default base URL and/or API key for the datastream API."""
    global _base_url, _api_key
    if base_url is not None:
        _base_url = base_url
    if api_key is not None:
        _api_key = api_key


def get_base_url() -> str:
    """Return the current default base URL."""
    return _base_url


def get_api_key() -> str | None:
    """Return the configured API key, falling back to the DATASTREAM_API_KEY env var."""
    if _api_key is not None:
        return _api_key
    return os.environ.get("DATASTREAM_API_KEY")
