import pytest


@pytest.fixture(autouse=True)
def _disable_auth_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """default the server test suite to auth-disabled

    keeps existing route and integration tests key-free; the auth tests opt back
    into enforcement explicitly by overriding this env var.
    """
    monkeypatch.setenv("DATASTREAM_AUTH_DISABLED", "true")
