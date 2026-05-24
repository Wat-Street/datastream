import time
from collections.abc import Callable

import structlog

logger = structlog.get_logger()


def retry_with_backoff[T](
    fn: Callable[[], T],
    *,
    max_retries: int,
    initial_delay: float,
    backoff_factor: float = 2.0,
    description: str,
) -> T:
    """Call fn(), retrying with exponential backoff on failure."""
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exception = exc

            if attempt == max_retries:
                break

            delay = initial_delay * (backoff_factor**attempt)
            logger.warning(
                "retrying after failure",
                description=description,
                attempt=attempt + 1,
                max_retries=max_retries,
                delay_seconds=delay,
                error=str(exc),
            )
            time.sleep(delay)

    raise last_exception  # type: ignore[misc] # always set when we reach here
