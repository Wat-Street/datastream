"""Central structlog configuration for the builder server."""

import logging
import os
import sys

import structlog


class _StatusFilter(logging.Filter):
    """Drop uvicorn access log entries for the /status health check endpoint."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "/status" not in record.getMessage()


def setup_logging() -> None:
    """Configure structlog and route stdlib logging through the same pipeline."""
    json_mode = os.environ.get("LOG_FORMAT", "").lower() == "json"

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if json_mode:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    # suppress /status from uvicorn access logs
    logging.getLogger("uvicorn.access").addFilter(_StatusFilter())
