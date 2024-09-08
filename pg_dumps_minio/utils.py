import csv
import hashlib
import os
import shutil
from collections.abc import Buffer, Generator
from typing import NamedTuple

from loguru import logger


def make_dirs(*dirs: str) -> None:
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def append_to_csv(
    data: list[NamedTuple], filename: str, with_header: bool = False
) -> None:
    with open(filename, mode="a", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if with_header:
            writer.writerow(data[0]._fields)
        writer.writerows(data)


def cleanup_dirs(root_path: str) -> None:
    tmp_dir = os.path.join(root_path, "temp")
    dumps_dir = os.path.join(root_path, "dumps")
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    if os.path.exists(dumps_dir):
        shutil.rmtree(dumps_dir)
    logger.debug("cleanup success")


def get_file_md5_hash(filepath: str) -> str:
    hasher = hashlib.md5()  # noqa: S324
    for block in file_as_blockiter(filepath):
        hasher.update(block)
    return hasher.hexdigest()


def file_as_blockiter(
    filepath: str, block_size: int = 65536
) -> Generator[Buffer]:
    with open(filepath, "rb") as f:
        block = f.read(block_size)
        while len(block) > 0:
            yield block
            block = f.read(block_size)


def get_md5_hash(string: str) -> str:
    return hashlib.md5(string.encode()).hexdigest()  # noqa: S324
