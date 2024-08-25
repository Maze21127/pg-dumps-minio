from typing import NamedTuple, Optional

from psycopg2.extras import NamedTupleCursor


class PgManager:
    def __init__(self, cursor: NamedTupleCursor) -> None:
        self._cursor = cursor

    def get_schemas(
        self,
    ) -> list[str]:
        excluded = {"information_schema", "pg_catalog", "pg_toast"}
        data = self._fetch_data("select * from information_schema.schemata")
        return [i.schema_name for i in data if i.schema_name not in excluded]

    def get_tables(self, schema: str) -> list[str]:
        data = self._fetch_data(
            "select * from information_schema.tables where table_schema=%s",
            (schema,),
        )
        return [i.table_name for i in data]

    def get_data(
        self,
        schema: str,
        table: str,
        limit: int,
        offset: int,
    ) -> NamedTuple:
        query = f"select * from {schema}.{table} limit {limit} offset {offset}"
        data = self._fetch_data(query)
        return data

    def _fetch_data(
        self,
        query: str,
        variables: Optional[tuple[str]] = None,
    ) -> list[NamedTuple]:
        self._cursor.execute(query, variables)
        data = self._cursor.fetchall()
        return data
