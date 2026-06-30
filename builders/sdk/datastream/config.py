import os

# env var used as the api key when one isn't set explicitly via configure()
API_KEY_ENV = "DATASTREAM_API_KEY"

_base_url: str = "http://localhost:3000/api/v1"
_api_key: str | None = None


def configure(base_url: str | None = None, api_key: str | None = None) -> None:
    """Set the default base URL and/or api key for the datastream API.

    Only the arguments you pass are changed, so you can set one without
    clearing the other.
    """
    global _base_url, _api_key
    if base_url is not None:
        _base_url = base_url
    if api_key is not None:
        _api_key = api_key


def get_base_url() -> str:
    """Return the current default base URL."""
    return _base_url


def get_api_key() -> str | None:
    """Return the api key: the configured value, else DATASTREAM_API_KEY, else None."""
    if _api_key is not None:
        return _api_key
    return os.environ.get(API_KEY_ENV)
