import asyncio
from asyncio import Future

from loguru import logger


def make_request() -> Future:
    future = Future()
    asyncio.create_task(set_future_value(future))
    return future


async def set_future_value(future: Future) -> None:
    await asyncio.sleep(1)
    future.set_result(42)


async def main() -> None:
    future = make_request()
    logger.info(f"Is the future done? {future.done()}")
    value = await future
    logger.info(f"Is the future done? {future.done()}")
    logger.info(value)


asyncio.run(main())
