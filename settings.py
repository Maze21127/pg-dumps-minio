import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    PostgresDsn,
    SecretStr,
    model_validator,
)
from pydantic_settings import BaseSettings
from typing_extensions import Self

load_dotenv(".env")


class DatabaseSettings(BaseModel):
    name: str
    dsn: PostgresDsn
    db_schema: Optional[str] = None
    ignore_duplicates: bool = False


class Settings(BaseSettings):
    S3_ENDPOINT: HttpUrl
    S3_BUCKET: str
    S3_ACCESS_KEY: SecretStr
    S3_SECRET_KEY: SecretStr

    databases: list[DatabaseSettings] = Field(default_factory=list)

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
                if name in databases:
                    databases[name].db_schema = v
        for k, v in os.environ.items():
            if k.startswith("IGNORE_DUPLICATES_"):
                name = k.removeprefix("IGNORE_DUPLICATES_")
                if name in databases:
                    databases[name].ignore_duplicates = v

        self.databases = list(databases.values())
        return self

    def get_settings_for_db(self, db_name: str) -> DatabaseSettings:
        for database in self.databases:
            if database.dsn.path.removeprefix("/") == db_name:
                return database
        raise ValueError(f"Database {db_name} does not exist")
