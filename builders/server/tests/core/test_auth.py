import pytest
from core.auth import generate_key, hash_key, load_key_map, verify_api_key
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials


def _creds(token: str) -> HTTPAuthorizationCredentials:
    """Build bearer credentials for calling verify_api_key directly."""
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def test_hash_key_deterministic() -> None:
    """Same input hashes to the same digest, different inputs differ."""
    assert hash_key("abc") == hash_key("abc")
    assert hash_key("abc") != hash_key("abd")


def test_load_key_map_single(monkeypatch: pytest.MonkeyPatch) -> None:
    """A single label:hash pair loads into a {hash: label} map."""
    monkeypatch.setenv("API_KEYS", f"default:{hash_key('k1')}")
    load_key_map.cache_clear()
    assert load_key_map() == {hash_key("k1"): "default"}


def test_load_key_map_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    """Multiple comma-separated pairs all load, blanks are skipped."""
    monkeypatch.setenv(
        "API_KEYS", f"team-a:{hash_key('k1')}, team-b:{hash_key('k2')} ,"
    )
    load_key_map.cache_clear()
    assert load_key_map() == {
        hash_key("k1"): "team-a",
        hash_key("k2"): "team-b",
    }


def test_load_key_map_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset or empty API_KEYS yields an empty map."""
    monkeypatch.delenv("API_KEYS", raising=False)
    load_key_map.cache_clear()
    assert load_key_map() == {}


def test_load_key_map_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    """An entry without a colon separator raises ValueError."""
    monkeypatch.setenv("API_KEYS", "no-separator-here")
    load_key_map.cache_clear()
    with pytest.raises(ValueError):
        load_key_map()


def test_verify_api_key_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    """A known key returns its team label."""
    monkeypatch.setenv("API_KEYS", f"team-a:{hash_key('secret')}")
    load_key_map.cache_clear()
    assert verify_api_key(_creds("secret")) == "team-a"


def test_verify_api_key_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown key raises 401."""
    monkeypatch.setenv("API_KEYS", f"team-a:{hash_key('secret')}")
    load_key_map.cache_clear()
    with pytest.raises(HTTPException) as exc:
        verify_api_key(_creds("wrong"))
    assert exc.value.status_code == 401


def test_verify_api_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing credentials raise 401 (not 403)."""
    monkeypatch.setenv("API_KEYS", f"team-a:{hash_key('secret')}")
    load_key_map.cache_clear()
    with pytest.raises(HTTPException) as exc:
        verify_api_key(None)
    assert exc.value.status_code == 401


def test_generate_key_round_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    """A generated key verifies against the env line it produces."""
    raw, env_line = generate_key("team-a")
    assert raw.startswith("dsk_")
    monkeypatch.setenv("API_KEYS", env_line)
    load_key_map.cache_clear()
    assert verify_api_key(_creds(raw)) == "team-a"
