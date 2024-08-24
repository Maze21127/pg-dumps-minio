import datetime as dt
import os
import shutil

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


def dump_table(
    cursor: NamedTupleCursor, schema: str, table: str, path: str
) -> None:
    query = (
        f"COPY (SELECT * FROM {schema}.{table}) TO '{path}.csv' "
        f"WITH (FORMAT CSV, HEADER)"
    )
    cursor.execute(query)
    print(f"Created {path}.csv")


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


def cleanup_dirs() -> None:
    shutil.rmtree("temp")
    shutil.rmtree("dumps")
    print("cleanup dirs success")


def main() -> None:
    settings = Settings()
    current_dir = os.getcwd()
    db_name = settings.DB_DSN.path[1:]
    schema = settings.DB_SCHEMA
    db_dir = os.path.join(current_dir, "temp", db_name)
    dumps_dir = os.path.join(current_dir, "dumps")
    schema_dir = os.path.join(current_dir, db_dir, schema)

    make_dirs(db_dir, schema_dir, dumps_dir)
    dump_tables(schema, schema_dir)

    export_file = os.path.join(dumps_dir, db_name)
    export_format = "zip"

    shutil.make_archive(export_file, export_format, dumps_dir)

    exported_file_path = f"{export_file}.{export_format}"

    filename = (
        f"{int(dt.datetime.now().timestamp())}_"
        f"{exported_file_path.split('/')[-1]}"
    )
    send_to_s3(exported_file_path, filename)
    cleanup_dirs()


if __name__ == "__main__":
    main()
