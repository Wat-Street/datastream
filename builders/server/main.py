import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from api.routes import router
from fastapi import FastAPI
from runtime.config import SCRIPTS_DIR
from runtime.venv_management import setup_builder_venvs

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Startup: create per-builder venvs."""
    setup_builder_venvs(SCRIPTS_DIR)
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(router, prefix="/api/v1")
