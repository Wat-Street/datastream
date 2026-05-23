import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from core.api.routes import router
from core.db.connection import close_pool, open_pool
from core.runtime.config import SCRIPTS_DIR
from core.runtime.registry import load_all_configs
from core.runtime.venv_management import setup_builder_venvs
from fastapi import FastAPI, Request, Response
from log_config import setup_logging as _setup_logging

_setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup: create per-builder venvs and open DB pool. Shutdown: close pool."""
    load_all_configs(SCRIPTS_DIR)
    setup_builder_venvs(SCRIPTS_DIR)
    open_pool(os.environ["DATABASE_URL"])
    yield
    close_pool()


app = FastAPI(lifespan=lifespan)
app.include_router(router, prefix="/api/v1")


@app.middleware("http")
async def request_context_middleware(request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
    """Bind a unique request_id to structlog context for each request."""
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=str(uuid.uuid4()))
    return await call_next(request)
