import asyncio


async def sleep(job_id: int, seconds: int, exception: bool = False) -> int:
    print(f"Starting job {job_id}")
    await asyncio.sleep(seconds)
    if exception:
        raise Exception("Exception")
    print(f"Finish job {job_id}")
    return job_id


async def run_by_task() -> None:
    task1 = asyncio.create_task(sleep(job_id=1, seconds=1))
    task2 = asyncio.create_task(sleep(job_id=2, seconds=2))
    task3 = asyncio.create_task(sleep(job_id=3, seconds=3))
    result1 = await task1
    result2 = await task2
    result3 = await task3
    results = [result1, result2, result3]

    print(f"Results: {results}")


async def run_by_gather() -> None:
    results = await asyncio.gather(
        sleep(job_id=1, seconds=1),
        sleep(job_id=2, seconds=2, exception=True),
        sleep(job_id=3, seconds=3),
    )
    print(f"Results: {results}")


async def run_by_task_group() -> None:
    try:
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(sleep(job_id=1, seconds=1))
            task2 = tg.create_task(sleep(job_id=2, seconds=2, exception=True))
            task3 = tg.create_task(sleep(job_id=3, seconds=3))
        print(f"{task1.result()=}")
        print(f"{task2.result()=}")
        print(f"{task3.result()=}")
    except* Exception as e:
        print(f"Exception: {e.exceptions}")


def main():
    # asyncio.run(run_by_gather())
    # asyncio.run(run_by_task_group())
    asyncio.run(run_by_task())


if __name__ == "__main__":
    main()
