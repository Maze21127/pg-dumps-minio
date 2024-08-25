import csv
import datetime as dt
import os
import shutil
from typing import Final, NamedTuple, Optional, Self

import boto3.session
import psycopg2
from botocore.client import BaseClient
from dotenv import load_dotenv
from loguru import logger
from psycopg2.extras import NamedTupleCursor
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    PostgresDsn,
    SecretStr,
    model_validator,
)
from pydantic_settings import BaseSettings

load_dotenv(".env")


class DatabaseSettings(BaseModel):
    name: str
    dsn: PostgresDsn
    db_schema: Optional[str] = None


class Settings(BaseSettings):
    S3_ENDPOINT: HttpUrl
    S3_BUCKET: str
    S3_ACCESS_KEY: SecretStr
    S3_SECRET_KEY: SecretStr

    DATABASES: list[DatabaseSettings] = Field(default_factory=list)

    @model_validator(mode="after")
    def read_all_databases(self) -> Self:
        databases = {}
        for k, v in os.environ.items():
            if k.startswith("DB_DSN_"):
                name = k.removeprefix("DB_DSN_")
                databases[name] = DatabaseSettings(dsn=v, name=name)

        for k, v in os.environ.items():
            if k.startswith("DB_SCHEMA_"):
                name = k.removeprefix("DB_SCHEMA_")
                databases[name].db_schema = v
        self.DATABASES = list(databases.values())
        return self


settings = Settings()
ROOT_PATH: Final[str] = "/var/tmp/pg_dumps_minio"
DUMPS_DIR: Final[str] = os.path.join(ROOT_PATH, "dumps")
EXPORT_FORMAT = "zip"


def get_schemas(cursor: NamedTupleCursor) -> list[str]:
    excluded = {"information_schema", "pg_catalog", "pg_toast"}
    cursor.execute("select * from information_schema.schemata")
    data = cursor.fetchall()
    return [i.schema_name for i in data if i.schema_name not in excluded]


def get_tables(cursor: NamedTupleCursor, schema: str) -> list[str]:
    cursor.execute(
        "select * from information_schema.tables where table_schema=%s",
        (schema,),
    )
    data = cursor.fetchall()
    return [i.table_name for i in data]


def get_data(
    cursor: NamedTupleCursor, schema: str, table: str, limit: int, offset: int
) -> NamedTuple:
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
    logger.info(f"Created {filename}")


def dump_tables(
    cursor: NamedTupleCursor,
    db_dir: str,
    schema: Optional[str] = None,
) -> None:
    schemas = get_schemas(cursor) if schema is None else [schema]
    for schema_name in schemas:
        schema_dir = os.path.join(ROOT_PATH, db_dir, schema_name)
        make_dirs(schema_dir)
        tables = get_tables(cursor, schema=schema_name)
        for table in tables:
            path = os.path.join(schema_dir, table)
            dump_table(cursor, schema_name, table, path)


def make_dirs(*dirs: str) -> None:
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def send_to_s3(client: BaseClient, file_path: str, filename: str) -> None:
    client.upload_file(file_path, settings.S3_BUCKET, filename)
    logger.info(
        f"send {filename} to {settings.S3_ENDPOINT}{settings.S3_BUCKET}"
    )


def cleanup_dirs() -> None:
    shutil.rmtree(os.path.join(ROOT_PATH, "temp"))
    shutil.rmtree(os.path.join(ROOT_PATH, "dumps"))
    logger.debug("cleanup success")


def create_dump(db_settings: DatabaseSettings, s3_client: BaseClient) -> None:
    db_name: Final[str] = db_settings.dsn.path[1:]
    db_dir: Final[str] = os.path.join(ROOT_PATH, "temp", db_name)
    make_dirs(db_dir)
    conn = psycopg2.connect(db_settings.dsn.unicode_string())
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        dump_tables(cur, db_dir, db_settings.db_schema)

    export_file = os.path.join(DUMPS_DIR, db_name)
    shutil.make_archive(export_file, EXPORT_FORMAT, db_dir)
    exported_file_path = f"{export_file}.{EXPORT_FORMAT}"
    export_filename = (
        f"{int(dt.datetime.now().timestamp())}_"
        f"{exported_file_path.split('/')[-1]}"
    )
    export_filename = os.path.join(db_name, export_filename)
    send_to_s3(s3_client, exported_file_path, export_filename)


def main() -> None:
    _session = boto3.session.Session()
    endpoint_url = settings.S3_ENDPOINT.unicode_string()
    client = _session.client(
        service_name="s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.S3_ACCESS_KEY.get_secret_value(),
        aws_secret_access_key=settings.S3_SECRET_KEY.get_secret_value(),
    )
    logger.debug("S3 client init success")
    make_dirs(ROOT_PATH, DUMPS_DIR)
    for db in settings.DATABASES:
        create_dump(db, client)
    cleanup_dirs()


if __name__ == "__main__":
    main()
