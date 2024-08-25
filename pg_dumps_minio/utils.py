import csv
import os
from typing import NamedTuple


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
