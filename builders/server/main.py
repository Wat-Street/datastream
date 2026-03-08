import logging

from api.routes import router
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.include_router(router)
