import time
from concurrent.futures import ProcessPoolExecutor

from loguru import logger


def count(count_to: int) -> int:
    start = time.perf_counter()
    counter = 0
    while counter < count_to:
        counter = counter + 1
    end = time.perf_counter()
    logger.info(f"Finished counting to {count_to} in {end-start}")
    return counter


def say_hello(name: str, sleep: int) -> str:
    time.sleep(sleep)
    return f"Hello, {name}!"


if __name__ == "__main__":
    with ProcessPoolExecutor() as process_pool:
        numbers = [1, 3, 5, 22, 1_000_000_000]
        for result in process_pool.map(count, numbers):
            print(result)
