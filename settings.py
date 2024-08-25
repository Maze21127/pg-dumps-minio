import os
from typing import Optional, Self

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
        self.databases = list(databases.values())
        return self
