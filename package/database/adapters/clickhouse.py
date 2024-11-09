from .base import BaseAdapter
from clickhouse_connect.driver.client import Client
from collections.abc import Generator
from contextlib import contextmanager
from package.types import CHIdentifier, CHSettings, CHTableIdentifier
from sqlmodel import create_engine, MetaData, Table
from typing import List, Optional

import clickhouse_connect
import pydash


class CHAdapter(BaseAdapter):
    def __init__(self, settings: CHSettings) -> None:
        super().__init__(settings)

    @classmethod
    def create_url(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        driver: Optional[str] = None,
        secure: Optional[bool] = None,
    ) -> str:
        if driver:
            scheme = f"clickhouse+{driver}"
        else:
            scheme = "clickhouse"

        return f"{scheme}://{username}:{password}@{host}:{port}/{database}"

    @property
    def url(self) -> str:
        return self.create_url(
            self.settings.host,
            self.settings.port,
            self.settings.username,
            self.settings.password,
            self.settings.database,
            self.settings.driver,
            self.settings.secure,
        )

    @contextmanager
    def get_client(self) -> Generator[Client | None]:
        client = clickhouse_connect.get_client(dsn=self.url)

        yield client

        client.close()

    def has_database(self, database: str) -> bool:
        statement = "select 1 from system.databases where name = {database:String};"

        with self.get_client() as client:
            result = client.query(statement, parameters={"database": database})
            return bool(result.result_rows)

    def create_database(self, database: str) -> None:
        if self.has_database(database):
            return

        statement = "create database {database:Identifier};"

        with self.get_client() as client:
            client.command(statement, parameters={"database": database})

    def drop_database(self, database: str) -> None:
        if not self.has_database(database):
            return

        statement = "drop database {database:Identifier};"

        with self.get_client() as client:
            client.command(statement, parameters={"database": database})

    def has_schema(self, schema: str, database: Optional[str] = None) -> bool:
        raise NotImplementedError()

    def create_schema(self, schema: str, database: Optional[str] = None) -> None:
        raise NotImplementedError()

    def drop_schema(self, schema: str, database: Optional[str] = None) -> None:
        raise NotImplementedError()

    def has_table(self, table: str, database: Optional[str] = None) -> bool:
        if database is None:
            database = self.settings.database

        statement = "select 1 from system.tables where database = {database:String} and name = {table:String};"

        with self.get_client() as client:
            result = client.query(statement, parameters={"database": database, "table": table})
            return bool(result.result_rows)

    def create_table(self, table: str, statement: str, database: Optional[str] = None) -> None:
        if database is None:
            database = self.settings.database

        if self.has_table(table=table, database=database):
            return

        with self.get_client() as client:
            client.command(statement)

    def get_create_table_statement(self, table: str, database: Optional[str] = None) -> None:
        if database is None:
            database = self.settings.database

        statement = "show create table {database:Identifier}.{table:Identifier};"

        with self.get_client() as client:
            statement = client.command(statement, parameters={"database": database, "table": table})
            statement = statement.replace("\\n", "\n")

        return statement

    def drop_table(self, table: str, database: Optional[str] = None) -> None:
        if database is None:
            database = self.settings.database

        if not self.has_table(table=table, database=database):
            return

        quoted_table = CHTableIdentifier(database=database, table=table).to_string()
        statement = f"drop table {quoted_table};"

        with self.get_client() as client:
            client.command(statement)

    def get_table(self, table: str, database: Optional[str] = None) -> Table:
        if database is None:
            database = self.settings.database

        url = self.create_url(**self.settings.model_dump())
        engine = create_engine(url, echo=False)
        metadata = MetaData(schema=database)
        metadata.reflect(bind=engine)

        return metadata.tables.get(f"{database}.{table}")

    def get_table_replica_identity(self, table: str, database: Optional[str] = None) -> None:
        raise NotImplementedError()

    def set_table_replica_identity(
        self, table: str, replica_identity: str, database: Optional[str] = None
    ) -> None:
        raise NotImplementedError()

    def list_tables(self, database: Optional[str] = None) -> List[Table]:
        if database is None:
            database = self.settings.database

        url = self.create_url(**self.settings.model_dump())
        engine = create_engine(url, echo=False)
        metadata = MetaData(schema=database)
        metadata.reflect(bind=engine)

        return pydash.sort_by(list(metadata.tables.values()), lambda table: table.name)

    def has_user(self, username: str) -> bool:
        statement = "select 1 from system.users where name = {username:String};"

        with self.get_client() as client:
            result = client.query(statement, parameters={"username": username})
            return bool(result.result_rows)

    def create_user(self, username: str, password: str) -> None:
        if self.has_user(username):
            return

        quoted_username = CHIdentifier.quote(username)
        statement = f"create user {quoted_username} identified by %(password)s;"

        with self.get_client() as client:
            client.command(statement, parameters={"password": password})

    def drop_user(self, username: str) -> None:
        if not self.has_user(username):
            return

        quoted_username = CHIdentifier.quote(username)
        statement = f"drop user {quoted_username};"

        with self.get_client() as client:
            client.command(statement, parameters={"username": username})

    def grant_user_privileges(self, username: str, database: str) -> None:
        raise NotImplementedError()

    def revoke_user_privileges(self, username: str, database: str) -> None:
        raise NotImplementedError()

    def list_user_privileges(self, username: str) -> List[tuple]:
        raise NotImplementedError()

    def has_publication(self, publication: str) -> bool:
        raise NotImplementedError()

    def create_publication(self, publication: str, tables: List[str]) -> None:
        raise NotImplementedError()

    def drop_publication(self, publication: str) -> None:
        raise NotImplementedError()

    def list_publications(self) -> List[str]:
        raise NotImplementedError()
