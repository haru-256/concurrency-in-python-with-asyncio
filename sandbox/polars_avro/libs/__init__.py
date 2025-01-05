from typing import Awaitable, Callable, ParamSpec, TypeVar
from loguru import logger

import functools
import time

P = ParamSpec("P")  # For function parameters
SR = TypeVar("SR")  # For sync return type
AR = TypeVar("AR", bound=Awaitable)  # For return type


def async_timed():
    """Decorator to time an async function."""

    def wrapper(func: Callable[P, AR]) -> Callable[P, Awaitable[AR]]:
        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> AR:
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


def sync_timed():
    """Decorator to time an sync function."""

    def wrapper(func: Callable[P, SR]) -> Callable[P, SR]:
        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> SR:
            logger.info(f"starting {func} with args {args} {kwargs}")
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                end = time.perf_counter()
                total = end - start
                logger.info(f"finished {func} in {total:.4f} second(s)")

        return wrapped

    return wrapper
