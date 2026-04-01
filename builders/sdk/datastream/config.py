_base_url: str = "http://localhost:3000/api/v1"


def configure(base_url: str) -> None:
    """Set the default base URL for the datastream API."""
    global _base_url
    _base_url = base_url


def get_base_url() -> str:
    """Return the current default base URL."""
    return _base_url
