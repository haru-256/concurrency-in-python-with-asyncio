import asyncio

from utils import delay


async def hello_every_second():
    for i in range(2):
        await asyncio.sleep(1)
        print("I'm running other code while I'm waiting!")


async def main():
    first_delay = asyncio.create_task(delay(3))
    second_delay = asyncio.create_task(delay(3))
    await hello_every_second()
    await first_delay
    await second_delay


async def main_v2():
    first_delay = asyncio.create_task(delay(3))
    second_delay = asyncio.create_task(delay(3))
    await first_delay
    await second_delay
    await hello_every_second()  # This will not run until the first two delays are done. because hello_every_second is not a task.


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(main_v2())
