import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from core.api.routes import public_router, router
from core.auth import load_key_map, verify_api_key
from core.db.connection import close_pool, open_pool
from core.runtime.config import SCRIPTS_DIR
from core.runtime.registry import load_all_configs
from core.runtime.venv_management import setup_builder_venvs
from fastapi import Depends, FastAPI, Request, Response
from log_config import setup_logging as _setup_logging

logger = structlog.get_logger()

_setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup: validate api keys, create venvs, open DB pool. Shutdown: close pool."""
    # fail-closed: refuse to start without at least one api key on a public service
    key_map = load_key_map()
    if not key_map:
        raise RuntimeError("no API_KEYS configured; refusing to start unauthenticated")
    logger.info("api keys loaded", count=len(key_map))
    load_all_configs(SCRIPTS_DIR)
    setup_builder_venvs(SCRIPTS_DIR)
    open_pool(os.environ["DATABASE_URL"])
    yield
    close_pool()


app = FastAPI(lifespan=lifespan)
# public_router stays open (health check); everything else requires a valid api key
app.include_router(public_router, prefix="/api/v1")
app.include_router(router, prefix="/api/v1", dependencies=[Depends(verify_api_key)])


@app.middleware("http")
async def request_context_middleware(request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
    """Bind a unique request_id to structlog context for each request."""
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=str(uuid.uuid4()))
    return await call_next(request)
