import pytest
from core.api.security import (
    DISABLED_ENV,
    KEYS_ENV,
    auth_disabled,
    load_api_keys,
    require_api_key,
)
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials


def _creds(token: str) -> HTTPAuthorizationCredentials:
    """build bearer credentials as fastapi would hand them to the dependency"""
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


# --- load_api_keys parsing ---


def test_load_api_keys_parses_multiple_pairs(monkeypatch: pytest.MonkeyPatch) -> None:
    """multiple name=secret pairs map secret -> name."""
    monkeypatch.setenv(KEYS_ENV, "frontend=abc,sdk=def")
    assert load_api_keys() == {"abc": "frontend", "def": "sdk"}


def test_load_api_keys_strips_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    """surrounding whitespace on names and secrets is trimmed."""
    monkeypatch.setenv(KEYS_ENV, " frontend = abc , sdk = def ")
    assert load_api_keys() == {"abc": "frontend", "def": "sdk"}


def test_load_api_keys_skips_malformed_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """entries without an '=' or with an empty side are skipped."""
    monkeypatch.setenv(KEYS_ENV, "frontend=abc,garbage,=nokey,noval=")
    assert load_api_keys() == {"abc": "frontend"}


def test_load_api_keys_empty_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """unset env yields an empty map."""
    monkeypatch.delenv(KEYS_ENV, raising=False)
    assert load_api_keys() == {}


# --- auth_disabled flag ---


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_auth_disabled_truthy(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """recognized truthy values disable enforcement."""
    monkeypatch.setenv(DISABLED_ENV, value)
    assert auth_disabled() is True


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_auth_disabled_falsy(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """anything else keeps enforcement on."""
    monkeypatch.setenv(DISABLED_ENV, value)
    assert auth_disabled() is False


# --- require_api_key decision logic (called directly, no routes) ---


def test_require_api_key_disabled_bypasses(monkeypatch: pytest.MonkeyPatch) -> None:
    """with auth disabled the dependency allows the request and returns None."""
    monkeypatch.setenv(DISABLED_ENV, "true")
    monkeypatch.delenv(KEYS_ENV, raising=False)
    assert require_api_key(credentials=None) is None


def test_require_api_key_no_keys_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """enforcing with no keys configured rejects with 503."""
    monkeypatch.delenv(DISABLED_ENV, raising=False)
    monkeypatch.delenv(KEYS_ENV, raising=False)
    with pytest.raises(HTTPException) as exc:
        require_api_key(credentials=_creds("anything"))
    assert exc.value.status_code == 503


def test_require_api_key_valid_returns_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """a matching token returns the configured client name."""
    monkeypatch.delenv(DISABLED_ENV, raising=False)
    monkeypatch.setenv(KEYS_ENV, "frontend=abc,sdk=def")
    assert require_api_key(credentials=_creds("def")) == "sdk"


def test_require_api_key_invalid_token_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """a non-matching token is rejected with 401 + WWW-Authenticate."""
    monkeypatch.delenv(DISABLED_ENV, raising=False)
    monkeypatch.setenv(KEYS_ENV, "sdk=def")
    with pytest.raises(HTTPException) as exc:
        require_api_key(credentials=_creds("wrong"))
    assert exc.value.status_code == 401
    assert exc.value.headers == {"WWW-Authenticate": "Bearer"}


def test_require_api_key_missing_credentials_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """no credentials (missing or non-bearer header) is rejected with 401."""
    monkeypatch.delenv(DISABLED_ENV, raising=False)
    monkeypatch.setenv(KEYS_ENV, "sdk=def")
    with pytest.raises(HTTPException) as exc:
        require_api_key(credentials=None)
    assert exc.value.status_code == 401
