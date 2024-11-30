import functools
import time
from typing import Callable, ParamSpec, TypeVar

from loguru import logger

P = ParamSpec("P")  # For function parameters
R = TypeVar("R")  # For return type


def async_timed():
    """Decorator to time an async function."""

    def wrapper(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            logger.info(f"starting {func} with args {args} {kwargs}")
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                end = time.perf_counter()
                total = end - start
                logger.info(f"finished {func} in {total:.4f} second(s)")

        return wrapped

    return wrapper
