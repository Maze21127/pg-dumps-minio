import csv
import os
import shutil
from typing import NamedTuple

from loguru import logger


def make_dirs(*dirs: str) -> None:
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def append_to_csv(
    data: NamedTuple, filename: str, with_header: bool = False
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
