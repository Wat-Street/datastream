from fastapi.testclient import TestClient
from main import app

client: TestClient = TestClient(app)

ALLOWED_ORIGIN = "https://wat-street.github.io"


def _preflight(origin: str) -> dict[str, str]:
    """build preflight headers for a GET with an Authorization header."""
    return {
        "Origin": origin,
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "authorization",
    }


def test_preflight_allowed_origin() -> None:
    """an allowed origin gets a 200 preflight with cors headers, no auth needed."""
    res = client.options("/api/v1/datasets", headers=_preflight(ALLOWED_ORIGIN))
    assert res.status_code == 200
    assert res.headers["access-control-allow-origin"] == ALLOWED_ORIGIN
    assert "Authorization" in res.headers["access-control-allow-headers"]


def test_preflight_dev_origin() -> None:
    """the local vite dev server origin is allowed by default."""
    res = client.options(
        "/api/v1/datasets", headers=_preflight("http://localhost:5173")
    )
    assert res.status_code == 200
    assert res.headers["access-control-allow-origin"] == "http://localhost:5173"


def test_preflight_unknown_origin_rejected() -> None:
    """an unknown origin gets no cors headers."""
    res = client.options(
        "/api/v1/datasets", headers=_preflight("https://evil.example.com")
    )
    assert res.status_code == 400
    assert "access-control-allow-origin" not in res.headers
