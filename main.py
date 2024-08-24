import csv
import datetime as dt
import os
import shutil
from typing import NamedTuple

import boto3.session
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import NamedTupleCursor
from pydantic import HttpUrl, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings

load_dotenv(".env")


class Settings(BaseSettings):
    S3_ENDPOINT: HttpUrl
    S3_BUCKET: str
    S3_ACCESS_KEY: SecretStr
    S3_SECRET_KEY: SecretStr

    DB_DSN: PostgresDsn
    DB_SCHEMA: str


settings = Settings()


def get_tables(cursor: NamedTupleCursor, schema: str) -> list[str]:
    cursor.execute(
        "select * from information_schema.tables where table_schema=%s",
        (schema,),
    )
    data = cursor.fetchall()
    return [i.table_name for i in data]


def get_data(
    cursor: NamedTupleCursor, schema: str, table: str, limit: int, offset: int
) -> tuple:
    cursor.execute(
        f"select * from {schema}.{table} limit {limit} offset {offset}"
    )
    data = cursor.fetchall()
    return data


def write_to_csv(
    data: NamedTuple, filename: str, with_header: bool = False
) -> None:
    with open(filename, mode="a", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        if with_header:
            writer.writerow(data[0]._fields)
        writer.writerows(data)


def dump_table(
    cursor: NamedTupleCursor, schema: str, table: str, path: str
) -> None:
    batch_size = int(os.getenv("BATCH_SIZE", "10000"))
    offset = 0
    filename = f"{path}.csv"
    with_header = True
    while True:
        data = get_data(cursor, schema, table, batch_size, offset)

        if not len(data):
            break
        write_to_csv(data, filename, with_header=with_header)
        with_header = False
        offset += batch_size
    print(f"Created {filename}")


def dump_tables(schema_name: str, db_path: str) -> None:
    conn = psycopg2.connect(settings.DB_DSN.unicode_string())
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        tables = get_tables(cur, schema=schema_name)
        for table in tables:
            path = os.path.join(db_path, table)
            dump_table(cur, schema_name, table, path)


def make_dirs(*dirs: str) -> None:
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def send_to_s3(file_path: str, filename: str) -> None:
    _session = boto3.session.Session()
    _client = _session.client(
        service_name="s3",
        endpoint_url=settings.S3_ENDPOINT.unicode_string(),
        aws_access_key_id=settings.S3_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=settings.S3_SECRET_KEY.get_secret_value(),
    )
    print("S3 client init success")
    _client.upload_file(file_path, settings.S3_BUCKET, filename)
    print(f"send {filename} to {settings.S3_BUCKET}")


def cleanup_dirs(root_path: str) -> None:
    shutil.rmtree(os.path.join(root_path, "temp"))
    shutil.rmtree(os.path.join(root_path, "dumps"))
    print("cleanup success")


def main() -> None:
    root_path = "/var/tmp/pg_dumps_minio"
    db_name = settings.DB_DSN.path[1:]
    schema = settings.DB_SCHEMA
    db_dir = os.path.join(root_path, "temp", db_name)
    dumps_dir = os.path.join(root_path, "dumps")
    schema_dir = os.path.join(root_path, db_dir, schema)

    make_dirs(root_path, db_dir, schema_dir, dumps_dir)
    dump_tables(schema, schema_dir)

    export_file = os.path.join(dumps_dir, db_name)
    export_format = "zip"

    shutil.make_archive(export_file, export_format, db_dir)

    exported_file_path = f"{export_file}.{export_format}"

    export_filename = (
        f"{int(dt.datetime.now().timestamp())}_"
        f"{exported_file_path.split('/')[-1]}"
    )
    send_to_s3(exported_file_path, export_filename)
    cleanup_dirs(root_path)


if __name__ == "__main__":
    main()
