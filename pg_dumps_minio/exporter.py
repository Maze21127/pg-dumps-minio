import datetime as dt
import os
import shutil
from collections import defaultdict
from typing import Final, Optional

import boto3
import psycopg2
from loguru import logger
from psycopg2.extras import NamedTupleCursor

from pg_dumps_minio.pg_manager import PgManager
from pg_dumps_minio.utils import (
    append_to_csv,
    get_file_md5_hash,
    get_md5_hash,
    make_dirs,
)
from settings import DatabaseSettings, Settings


class Exporter:
    def __init__(
        self, settings: Settings, root_path: Optional[str] = None
    ) -> None:
        self.settings = settings
        self._s3_session: boto3.session.Session | None = None
        self._s3_client = None

        self.root_path = root_path or "/var/tmp/pg_dumps_minio"  # noqa: S108
        self._dumps_dir: Final[str] = os.path.join(self.root_path, "dumps")
        self._export_format = "zip"
        self._batch_size = int(os.getenv("BATCH_SIZE", "10000"))
        self._hashes: dict[str, list[str]] = defaultdict(list)

    def export_all(self) -> None:
        for db in self.settings.databases:
            try:
                self.export_one(db)
            except Exception as e:
                logger.error(f"Failed to export {db}: {e}")
                logger.exception(e)

    def export_one(
        self,
        db_settings: DatabaseSettings,
    ) -> None:
        db_name: Final[str] = db_settings.dsn.path.removeprefix("/")
        db_dir: Final[str] = os.path.join(self.root_path, "temp", db_name)
        make_dirs(db_dir)

        conn = psycopg2.connect(db_settings.dsn.unicode_string())
        with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
            pg_manager = PgManager(cur, db_name)
            self._dump_tables(pg_manager, db_dir, db_settings.db_schema)

        self._send_to_s3(db_dir, db_name)

    def _send_to_s3(self, db_dir: str, db_name: str) -> None:
        export_file = os.path.join(self._dumps_dir, db_name)
        shutil.make_archive(export_file, self._export_format, db_dir)

        filepath, filename = self._generate_final_filename(db_name, export_file)

        self._init_s3_client()

        self._s3_client.upload_file(filepath, self.settings.S3_BUCKET, filename)
        logger.info(
            f"send {filename} to {self.settings.S3_ENDPOINT}"
            f"{self.settings.S3_BUCKET}"
        )

    def _dump_tables(
        self,
        pg_manager: PgManager,
        db_dir: str,
        schema: Optional[str] = None,
    ) -> None:
        schemas = pg_manager.get_schemas() if schema is None else [schema]
        for schema_name in schemas:
            schema_dir = os.path.join(self.root_path, db_dir, schema_name)
            make_dirs(schema_dir)
            tables = pg_manager.get_tables(schema_name)
            for table in tables:
                path = os.path.join(schema_dir, table)
                self._dump_table(pg_manager, schema_name, table, path)

    def _dump_table(
        self, pg_manager: PgManager, schema: str, table: str, path: str
    ) -> None:
        offset = 0
        filename = f"{path}.csv"
        with_header = True
        while True:
            data = pg_manager.get_data(schema, table, self._batch_size, offset)

            if not len(data):
                break
            append_to_csv(data, filename, with_header=with_header)
            with_header = False
            offset += self._batch_size

        logger.info(f"Created {filename}")
        self._hashes[pg_manager.db_name].append(get_file_md5_hash(filename))

    def _generate_final_filename(
        self, db_name: str, filename: str
    ) -> tuple[str, str]:
        exported_file_path = f"{filename}.{self._export_format}"
        export_filename = os.path.join(
            db_name, self._generate_filename(exported_file_path, db_name)
        )
        return exported_file_path, export_filename

    def _generate_filename(self, filename: str, db_name: str) -> str:
        timestamp = int(dt.datetime.now().timestamp())
        filename = filename.split("/")[-1]
        if self.settings.get_settings_for_db(db_name):
            md5_hash = get_md5_hash("".join(self._hashes.get(db_name)))
            return f"{md5_hash}_{filename}"
        return f"{timestamp}_{filename}"

    @logger.catch
    def _init_s3_client(self) -> None:
        if self._s3_client:
            return
        self._s3_session = boto3.session.Session()
        self._s3_client = self._s3_session.client(
            service_name="s3",
            endpoint_url=self.settings.S3_ENDPOINT.unicode_string(),
            aws_access_key_id=self.settings.S3_ACCESS_KEY.get_secret_value(),
            aws_secret_access_key=self.settings.S3_SECRET_KEY.get_secret_value(),
        )
        logger.debug("Init s3 client")
