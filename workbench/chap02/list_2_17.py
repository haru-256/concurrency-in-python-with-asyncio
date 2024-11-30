import asyncio

from loguru import logger
from utils import async_timed


@async_timed()
async def delay(delay_seconds: int) -> int:
    """delay function to sleep for a given number of seconds.

    Args:
        delay_seconds: The number of seconds to sleep.

    Returns:
        The number of seconds slept
    """
    logger.info(f"sleeping for {delay_seconds} second(s)")
    await asyncio.sleep(delay_seconds)
    logger.info(f"finished sleeping for {delay_seconds} second(s)")
    return delay_seconds


@async_timed()
async def main():
    task_one = asyncio.create_task(delay(2))
    task_two = asyncio.create_task(delay(3))

    await task_one
    await task_two


asyncio.run(main())
