import asyncio
import time
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from loguru import logger


def count(count_to: int) -> int:
    start = time.perf_counter()
    counter = 0
    while counter < count_to:
        counter = counter + 1
    end = time.perf_counter()
    logger.info(f"Finished counting to {count_to} in {end-start}")
    return counter


async def main() -> None:
    with ProcessPoolExecutor() as process_pool:
        loop: AbstractEventLoop = asyncio.get_event_loop()
        # numbers = [1, 3, 5, 22, 1_000_000_000]
        numbers = [1_000_000_000, 1, 3, 5, 22]  # NOTE: 順不同で終わった順に表示される
        calls: list[partial[int]] = [partial(count, number) for number in numbers]
        call_coros = [loop.run_in_executor(process_pool, call) for call in calls]

        results = await asyncio.gather(*call_coros)
        for result in results:
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
