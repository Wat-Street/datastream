import pytest
from core.auth import hash_key, load_key_map, verify_api_key
from fastapi.testclient import TestClient
from main import app

client: TestClient = TestClient(app)

VALID_KEY = "secret-key"


@pytest.fixture
def real_auth(monkeypatch: pytest.MonkeyPatch):
    """remove the autouse bypass and load a known key so the real dependency runs."""
    app.dependency_overrides.pop(verify_api_key, None)
    monkeypatch.setenv("API_KEYS", f"team-a:{hash_key(VALID_KEY)}")
    load_key_map.cache_clear()
    yield
    load_key_map.cache_clear()


def _auth(key: str) -> dict[str, str]:
    """build an Authorization header for the given raw key."""
    return {"Authorization": f"Bearer {key}"}


def test_status_open_without_key(real_auth: None) -> None:
    """the health check is unauthenticated."""
    assert client.get("/api/v1/status").status_code == 200


def test_datasets_missing_key_401(real_auth: None) -> None:
    """a protected endpoint with no header returns 401."""
    assert client.get("/api/v1/datasets").status_code == 401


def test_datasets_invalid_key_401(real_auth: None) -> None:
    """a protected endpoint with a wrong key returns 401."""
    assert client.get("/api/v1/datasets", headers=_auth("wrong")).status_code == 401


def test_build_missing_key_401(real_auth: None) -> None:
    """build requires a key too."""
    resp = client.post("/api/v1/build/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 401


def test_data_missing_key_401(real_auth: None) -> None:
    """data requires a key too."""
    resp = client.get("/api/v1/data/ds/0.1.0?start=2024-01-01&end=2024-01-31")
    assert resp.status_code == 401


def test_datasets_valid_key_passes_auth(real_auth: None) -> None:
    """a valid key passes the auth layer (not 401/403)."""
    resp = client.get("/api/v1/datasets", headers=_auth(VALID_KEY))
    assert resp.status_code not in (401, 403)
