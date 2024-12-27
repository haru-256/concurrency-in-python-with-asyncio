import asyncio
import pathlib
from io import BytesIO
from typing import Any

import aiofiles
from fastavro import parse_schema, reader, writer
from fastavro.types import AvroMessage, Schema
from loguru import logger
from utils import async_timed


@async_timed()
async def write_avro_file(
    file: pathlib.Path, schema: Schema, records: list[dict[str, Any]]
):
    """Write Avro file with the given schema and records.

    Args:
        file: The file path to write the Avro file.
        schema: The Avro schema.
        records: The list of records to write.
    """
    with open(file, mode="wb") as f:
        await asyncio.sleep(3)
        # Use fastavro's writer to serialize data into the file
        await asyncio.to_thread(writer, f, schema, records)


async def write_avro_files(
    args: list[tuple[pathlib.Path, Schema, list[dict[str, Any]]]],
):
    """Write Avro files with the given schema and records.

    Args:
        args: The list of tuples containing the file path, schema, and records.
    """
    try:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(write_avro_file(file, schema, records))
                for file, schema, records in args
            ]
        for task in tasks:
            await task
    except* Exception as e:
        logger.error(f"Exception: {e.exceptions}")


@async_timed()
async def read_avro_file(file: pathlib.Path):
    """Read Avro file and return the records. Execute the blocking operation in another thread by asyncio.to_thread.

    Args:
        file: The file path to read the Avro file.

    Returns:
        The list of records.
    """
    await asyncio.sleep(3)

    def sync_read_avro_file(file: pathlib.Path) -> list[AvroMessage]:
        """Synchronously reads all records from an Avro file."""
        with open(file, "rb") as fo:
            avro_reader = reader(fo)  # blocking IO-bound operation
            records = [record for record in avro_reader]  # CPU-bound operation
        return records

    # Use asyncio.to_thread to offload the blocking operation to a thread
    content = await asyncio.to_thread(sync_read_avro_file, file)
    return content


@async_timed()
async def read_avro_file_v2(file: pathlib.Path):
    """Read Avro file and return the records. Execute by using aiofiles.

    Args:
        file: The file path to read the Avro file.

    Returns:
        The list of records.
    """
    await asyncio.sleep(3)
    async with aiofiles.open(file, mode="rb") as fo:
        # `aiofiles` doesn't work with fastavro directly, so read the file into memory
        byte_content = await fo.read()  # non-blocking IO-bound operation
        avro_reader = reader(BytesIO(byte_content))  # CPU-bound operation
    content = list(avro_reader)
    return content


async def read_avro_files(files: list[pathlib.Path]) -> list[list[AvroMessage]]:
    """Read Avro files and return the records

    Args:
        files: to read the Avro files.

    Returns:
        The list of records.
    """
    try:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(read_avro_file_v2(file)) for file in files]
        results = [task.result() for task in tasks]
    except* Exception as e:
        logger.error(f"Exception: {e.exceptions}")
    return results


async def main():
    # Define the Avro schema
    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "email", "type": ["null", "string"], "default": None},
        ],
    }
    parsed_schema = parse_schema(schema)

    # Sample data to write
    records = [
        {"name": "Alice", "age": 30, "email": None},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
    ]

    # Run the async function
    save_dir = pathlib.Path("./data")
    save_dir.mkdir(exist_ok=True)
    await write_avro_files(
        args=[
            (save_dir / "1.avro", parsed_schema, records),
            (save_dir / "2.avro", parsed_schema, records),
        ]
    )

    # Read the saved Avro file
    contents = await read_avro_files(files=[save_dir / "1.avro", save_dir / "2.avro"])
    logger.info(f"Read contents: {contents}")


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
