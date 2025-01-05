from typing import Literal
import polars as pl
import numpy as np
import pathlib
from loguru import logger
import asyncio
from multiprocessing import get_context

from pydantic import Field, BaseModel
from pydantic_settings import (
    BaseSettings,
    CliApp,
    CliSubCommand,
)
import time
from libs import sync_timed, async_timed
from concurrent.futures import ProcessPoolExecutor


class Init(BaseModel):
    """Init Avro files to conduct benchmarking"""

    num_avro_files: int = Field(
        default=300, description="Number of Avro files to generate"
    )
    num_records: int = Field(default=10000, description="Number of records per file")
    ndim: int = Field(default=768, description="Number of dimensions")
    save_dir: pathlib.Path = Field(
        default=pathlib.Path("./data"), description="Save dir"
    )

    def cli_cmd(self) -> None:
        logger.info(f"Args: {self.model_dump()}")
        _init_avro_files(
            num_avro_files=self.num_avro_files,
            num_records=self.num_records,
            save_dir=self.save_dir,
            ndim=self.ndim,
        )


class Run(BaseModel):
    """Run the benchmarking"""

    version: Literal["v1", "v2"] = Field(
        default="v1", description="The version to run the benchmarking"
    )
    save_dir: pathlib.Path = Field(
        default=pathlib.Path("./data"), description="Save dir"
    )

    def cli_cmd(self) -> None:
        logger.info(f"Args: {self.model_dump()}")
        _run_benchmarking(version=self.version, save_dir=self.save_dir)


class Cmd(BaseSettings):
    """cmd settings"""

    init: CliSubCommand[Init]
    run: CliSubCommand[Run]

    def cli_cmd(self) -> None:
        CliApp.run_subcommand(self)


def _init_avro_files(
    num_avro_files: int, num_records: int, save_dir: pathlib.Path, ndim: int
) -> None:
    """Init Avro files to conduct benchmarking

    Args:
        num_avro_files: Number of Avro files to generate
        num_records: Number of records per file
        save_dir: Save dir
        ndim: Number of dimensions
    """
    if not save_dir.exists():
        save_dir.mkdir(exist_ok=True)

    for i in range(num_avro_files):
        save_path = save_dir / f"{i:05d}.avro"
        if i % 10 == 0:
            logger.info(f"Generating Avro file {save_path}...")
        df = pl.from_dict({"embedding": np.random.rand(num_records, ndim).tolist()})
        df.write_avro(save_path, name="embedding")


def _run_benchmarking(version: Literal["v1", "v2"], save_dir: pathlib.Path) -> None:
    """Run the benchmarking to read Avro files

    Args:
        version: version to run the benchmarking
    """
    if version == "v1":
        dfs = _run_benchmarking_v1(save_dir)
    elif version == "v2":
        dfs = asyncio.run(_run_benchmarking_v2(save_dir))
    else:
        raise ValueError(f"Invalid version: {version}")

    df = pl.concat(dfs)
    logger.info(f"Read {len(dfs)} Avro files, {len(df)} records")


def _read_avro_sync(avro_file: pathlib.Path) -> pl.DataFrame:
    logger.info(f"Start  | Read Avro file {avro_file}...")
    df = pl.read_avro(avro_file)
    logger.info(f"Finish | Read Avro file {avro_file}...")
    return df


@sync_timed()
def _run_benchmarking_v1(save_dir: pathlib.Path) -> list[pl.DataFrame]:
    """Run the benchmarking to read Avro files by synchronous way

    Args:
        save_dir: Save dir

    Returns:
        list[pl.DataFrame]: List of DataFrames
    """
    avro_files = sorted(list(save_dir.glob("*.avro")))
    dfs = [_read_avro_sync(avro_file) for avro_file in avro_files]
    return dfs


@async_timed()
async def _run_benchmarking_v2(save_dir: pathlib.Path) -> list[pl.DataFrame]:
    """Run the benchmarking to read Avro files by async way

    Args:
        save_dir: Save dir

    Returns:
        list[pl.DataFrame]: List of DataFrames
    """

    avro_files = sorted(list(save_dir.glob("*.avro")))
    with ProcessPoolExecutor(max_workers=7, max_tasks_per_child=5) as pool:
        loop = asyncio.get_event_loop()
        call_coros = [
            loop.run_in_executor(pool, _read_avro_sync, avro_file)
            for avro_file in avro_files
        ]
        dfs = await asyncio.gather(*call_coros)
    return dfs


def main():
    CliApp.run(Cmd, cli_cmd_method_name="cli_cmd")


if __name__ == "__main__":
    main()
