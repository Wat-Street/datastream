"""API key authentication: hashed-key loading and the bearer-token dependency."""

import hashlib
import os
import secrets
from functools import lru_cache

import structlog
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = structlog.get_logger()

# env var holding valid keys as comma-separated "label:sha256hex" pairs
API_KEYS_ENV = "API_KEYS"

# prefix for generated raw keys, makes them greppable in secret scanners and logs
KEY_PREFIX = "dsk_"

# auto_error=False so a missing header returns our 401 (HTTPBearer's default is 403)
bearer = HTTPBearer(auto_error=False)


def hash_key(raw: str) -> str:
    """Return the sha256 hex digest of a raw API key."""
    return hashlib.sha256(raw.encode()).hexdigest()


@lru_cache(maxsize=1)
def load_key_map() -> dict[str, str]:
    """Parse the API_KEYS env var into a {sha256hex: label} map.

    Format is comma-separated ``label:sha256hex`` pairs. Blank entries are
    skipped; malformed entries raise ValueError. Cached; call
    ``load_key_map.cache_clear()`` to re-read (used in tests).
    """
    raw = os.environ.get(API_KEYS_ENV, "")
    key_map: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        # rsplit so labels may contain colons even though hashes never do
        label, sep, digest = entry.rpartition(":")
        if not sep or not label or not digest:
            raise ValueError(f"malformed API_KEYS entry: {entry!r}")
        key_map[digest] = label
    return key_map


def verify_api_key(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> str:
    """FastAPI dependency: validate the bearer token and return its team label.

    Raises 401 on a missing or unknown key. Looks up the sha256 of the presented
    token in the key map. Comparing the hash (not the raw key) is timing-safe
    here: the compared value is already a sha256 of the secret, so timing cannot
    recover the key. Binds the matched label to the log context as ``team``.
    """
    if credentials is None:
        raise HTTPException(status_code=401, detail="missing api key")
    label = load_key_map().get(hash_key(credentials.credentials))
    if label is None:
        raise HTTPException(status_code=401, detail="invalid api key")
    structlog.contextvars.bind_contextvars(team=label)
    return label


def generate_key(label: str = "default") -> tuple[str, str]:
    """Return a (raw key, ``label:hash`` env line) pair for a fresh random key.

    Only the env line goes in API_KEYS; hand the raw key to the client.
    """
    raw = KEY_PREFIX + secrets.token_urlsafe(32)
    return raw, f"{label}:{hash_key(raw)}"
