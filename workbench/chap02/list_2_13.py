import asyncio

from loguru import logger
from utils import delay


async def main():
    task = asyncio.create_task(delay(10))

    try:
        result = await asyncio.wait_for(asyncio.shield(task), 5)
        logger.info(f"{result=} in try")
    except TimeoutError:
        logger.warning("Task took longer than five seconds, it will finish soon!")
        logger.info(f"Was the task cancelled? {task.cancelled()}")
        result = await task
        logger.info(f"{result=} in except")


asyncio.run(main())
