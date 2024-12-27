import time
from multiprocessing import Pool


def say_hello(name: str, sleep: int) -> str:
    time.sleep(sleep)
    return f"Hello, {name}!"


if __name__ == "__main__":
    with Pool() as process_pool:
        hi_jeff = process_pool.apply(say_hello, args=("Jeff", 5))
        hi_john = process_pool.apply(say_hello, args=("John", 10))
        print(hi_jeff)
        print(hi_john)
