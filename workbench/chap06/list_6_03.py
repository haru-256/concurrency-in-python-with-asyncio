import time
from multiprocessing import Pool


def say_hello(name: str, sleep: int) -> str:
    time.sleep(sleep)
    return f"Hello, {name}!"


if __name__ == "__main__":
    with Pool() as process_pool:
        hi_jeff = process_pool.apply_async(say_hello, args=("Jeff", 10))
        hi_john = process_pool.apply_async(say_hello, args=("John", 5))
        print(hi_jeff.get())
        print(hi_john.get())  # 先に終わるが、こちらが先に表示される訳では無い
