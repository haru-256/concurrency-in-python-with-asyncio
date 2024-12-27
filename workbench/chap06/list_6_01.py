import time
from multiprocessing import Process


def count(count_to: int) -> int:
    start = time.perf_counter()
    counter = 0
    while counter < count_to:
        counter = counter + 1
    end = time.perf_counter()
    print(f"Finished counting to {count_to} in {end-start}")
    return counter


if __name__ == "__main__":
    start_time = time.perf_counter()

    to_one_hundred_million = Process(target=count, args=(100000000,))
    to_two_hundred_million = Process(target=count, args=(200000000,))

    to_one_hundred_million.start()
    to_two_hundred_million.start()

    to_one_hundred_million.join()
    to_two_hundred_million.join()

    end_time = time.time()
    print(f"Completed in {end_time-start_time}")
