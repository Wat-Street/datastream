import os
import secrets
from typing import Annotated

import structlog
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = structlog.get_logger()

# env var holding the configured keys, format: "name=secret,name2=secret2"
KEYS_ENV = "DATASTREAM_API_KEYS"
# env var that bypasses enforcement entirely (local dev / tests)
DISABLED_ENV = "DATASTREAM_AUTH_DISABLED"

# parse "Authorization: Bearer <token>" without raising on a missing/other scheme,
# so we can return our own consistent 401 instead of fastapi's default
bearer_scheme = HTTPBearer(auto_error=False)

_TRUTHY = {"1", "true", "yes", "on"}


def auth_disabled() -> bool:
    """whether key enforcement is bypassed via DATASTREAM_AUTH_DISABLED"""
    return os.environ.get(DISABLED_ENV, "").strip().lower() in _TRUTHY


def load_api_keys() -> dict[str, str]:
    """parse DATASTREAM_API_KEYS into a {secret: name} map, skipping malformed entries

    read fresh from the environment on each call so tests and runtime config changes
    take effect without a restart; the parse is a cheap comma split
    """
    raw = os.environ.get(KEYS_ENV, "")
    keys: dict[str, str] = {}
    for entry in raw.split(","):
        name, sep, secret = entry.partition("=")
        if not sep:
            continue
        name, secret = name.strip(), secret.strip()
        if name and secret:
            keys[secret] = name
    return keys


def _match_key(token: str, keys: dict[str, str]) -> str | None:
    """constant-time match of token against every secret; returns client name or None

    every configured secret is compared with no early-out so a match does not leak
    through response timing
    """
    matched: str | None = None
    for secret, name in keys.items():
        if secrets.compare_digest(token, secret):
            matched = name
    return matched


def require_api_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> str | None:
    """fastapi dependency enforcing a valid bearer api key

    returns the matched client name (and binds it to the log context for auditing).
    fails closed: when enforcing with no keys configured, all requests are rejected.
    """
    if auth_disabled():
        return None

    keys = load_api_keys()
    if not keys:
        # enforcing with nothing configured is a misconfiguration, not open access
        logger.error("auth enforced but no api keys configured")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="authentication is not configured",
        )

    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing api key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    name = _match_key(credentials.credentials, keys)
    if name is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid api key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    structlog.contextvars.bind_contextvars(api_client=name)
    return name
