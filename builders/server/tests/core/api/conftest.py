import pytest
from core.auth import verify_api_key
from main import app


@pytest.fixture(autouse=True)
def _bypass_auth():
    """override the api-key dependency so endpoint tests don't need a real key.

    the dedicated auth-enforcement tests pop this override to exercise the real
    dependency.
    """
    app.dependency_overrides[verify_api_key] = lambda: "test"
    yield
    app.dependency_overrides.pop(verify_api_key, None)
