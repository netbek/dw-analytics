from ..types import CHSettings
from .base import BaseAdapter
from clickhouse_connect.driver.client import Client
from collections.abc import Generator
from contextlib import contextmanager
from package.types import CHIdentifier, CHTableIdentifier
from sqlmodel import create_engine, MetaData, Table
from typing import List, Optional

import clickhouse_connect
import pydash


@contextmanager
def get_clickhouse_client(dsn: str) -> Generator[Client | None]:
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        yield client
    finally:
        client.close()


class CHAdapter(BaseAdapter):
    def __init__(self, settings: CHSettings) -> None:
        super().__init__(settings)

    def get_client():
        raise NotImplementedError()

    def has_database(self, database: str) -> bool:
        with get_clickhouse_client(self.dsn) as client:
            result = client.query(
                "select 1 from system.databases where name = {database:String};",
                parameters={"database": database},
            )
            return bool(result.result_rows)

    def create_database(self, database: str) -> None:
        if self.has_database(database):
            return

        with get_clickhouse_client(self.dsn) as client:
            client.command(
                "create database {database:Identifier};",
                parameters={"database": database},
            )

    def drop_database(self, database: str) -> None:
        with get_clickhouse_client(self.dsn) as client:
            client.command(
                "drop database if exists {database:Identifier};",
                parameters={"database": database},
            )

    def has_schema():
        raise NotImplementedError()

    def has_table(self, table: str, database: Optional[str] = None) -> bool:
        if database is None:
            database = self.settings.database

        with get_clickhouse_client(self.dsn) as client:
            result = client.query(
                "select 1 from system.tables where database = {database:String} and name = {table:String};",
                parameters={"database": database, "table": table},
            )
            return bool(result.result_rows)

    def get_table_schema(self, table: str, database: Optional[str] = None) -> Table:
        if database is None:
            database = self.settings.database

        engine = create_engine(self.dsn, echo=False)
        metadata = MetaData(schema=database)
        metadata.reflect(bind=engine)

        return metadata.tables.get(f"{database}.{table}")

    def set_table_replica_identity(
        self, table: str, replica_identity: str, database: Optional[str] = None
    ) -> None:
        raise NotImplementedError()

    def create_table(self, table: str, statement: str, database: Optional[str] = None) -> None:
        if database is None:
            database = self.settings.database

        if self.has_table(table=table, database=database):
            return

        with get_clickhouse_client(self.dsn) as client:
            client.command(statement)

    def get_create_table_statement(self, table: str, database: Optional[str] = None) -> None:
        if database is None:
            database = self.settings.database

        with get_clickhouse_client(self.dsn) as client:
            statement = client.command(
                "show create table {database:Identifier}.{table:Identifier};",
                parameters={
                    "database": database,
                    "table": table,
                },
            )
            statement = statement.replace("\\n", "\n")

        return statement

    def drop_table(self, table: str, database: Optional[str] = None) -> None:
        if database is None:
            database = self.settings.database

        quoted_table = CHTableIdentifier(database=database, table=table).to_string()

        with get_clickhouse_client(self.dsn) as client:
            client.command(f"drop table if exists {quoted_table};")

    def list_tables(self, database: Optional[str] = None) -> List[Table]:
        if database is None:
            database = self.settings.database

        engine = create_engine(self.dsn, echo=False)
        metadata = MetaData(schema=database)
        metadata.reflect(bind=engine)

        return pydash.sort_by(list(metadata.tables.values()), lambda table: table.name)

    def has_user(self, username: str) -> bool:
        with get_clickhouse_client(self.dsn) as client:
            result = client.query(
                "select 1 from system.users where name = {username:String};",
                parameters={"username": username},
            )
            return bool(result.result_rows)

    def create_user(self, username: str, password: str) -> None:
        if self.has_user(username):
            return

        quoted_username = CHIdentifier.quote(username)

        with get_clickhouse_client(self.dsn) as client:
            client.command(
                f"create user {quoted_username} identified by %(password)s;",
                parameters={"password": password},
            )

    def drop_user(self, username: str) -> None:
        quoted_username = CHIdentifier.quote(username)

        with get_clickhouse_client(self.dsn) as client:
            client.command(
                f"drop user if exists {quoted_username};",
                parameters={"username": username},
            )

    def grant_user_privileges(self, username: str, database: str) -> None:
        raise NotImplementedError()

    def revoke_user_privileges(self, username: str, database: str) -> None:
        raise NotImplementedError()

    def has_publication(self, publication: str) -> bool:
        raise NotImplementedError()

    def create_publication(self, publication: str, tables: List[str]) -> None:
        raise NotImplementedError()

    def drop_publication(self, publication: str) -> None:
        raise NotImplementedError()
