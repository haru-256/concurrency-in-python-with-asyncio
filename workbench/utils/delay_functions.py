import asyncio

from loguru import logger


async def delay(delay_seconds: int) -> int:
    logger.info(f"sleeping for {delay_seconds} second(s)")
    await asyncio.sleep(delay_seconds)
    logger.info(f"finished sleeping for {delay_seconds} second(s)")
    return delay_seconds
