from abc import ABC, abstractmethod
from clickhouse_connect.driver.client import Client as CHClient
from collections.abc import Generator
from contextlib import contextmanager
from jinja2 import Environment
from package.types import (
    CHIdentifier,
    CHTableIdentifier,
    DBSettings,
    PGIdentifier,
    PGTableIdentifier,
)
from sqlmodel import create_engine, MetaData
from sqlmodel import Session as DBSession  # noqa: F401
from sqlmodel import Table
from typing import Any, List, Optional, overload
from typing_extensions import deprecated

import clickhouse_connect
import psycopg2
import pydash
import re
import sqlparse

RE_HAS_JINJA = re.compile(r"({[{%#]|[#}%]})")

jinja_env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])


@contextmanager
def get_clickhouse_client(dsn: str) -> Generator[CHClient | None]:
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        yield client
    finally:
        client.close()


@contextmanager
def get_postgres_client(
    dsn: str, autocommit: bool = True
) -> Generator[tuple[psycopg2.extensions.connection, psycopg2.extensions.cursor], Any, None]:
    connection = psycopg2.connect(dsn=dsn)
    try:
        connection.autocommit = autocommit
        with connection.cursor() as cursor:
            yield connection, cursor
    finally:
        connection.close()


def create_connection_url(
    type: str,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    driver: Optional[str] = None,
):
    if driver:
        scheme = f"{type}+{driver}"
    else:
        scheme = type

    return f"{scheme}://{username}:{password}@{host}:{port}/{database}"


def render_statement(
    statement: str, context: Optional[dict[str, Any]] = None, pretty: bool = False
) -> str:
    if RE_HAS_JINJA.search(statement):
        statement = jinja_env.from_string(statement, context).render()

    statement = statement.strip()

    if pretty:
        statement = sqlparse.format(
            statement, reindent=True, keyword_case="lower", identifier_case="lower"
        )

    return statement


def get_create_table_statement(dsn: str, database: str, table: str) -> str:
    with get_clickhouse_client(dsn) as client:
        result = client.command(
            "show create table {database:Identifier}.{table:Identifier};",
            parameters={
                "database": database,
                "table": table,
            },
        )
        result = result.replace("\\n", "\n")

    return result


@deprecated("Use Adapter.get_table_schema()")
def get_table_schema(dsn: str, database: str, table: str) -> Table:
    engine = create_engine(dsn, echo=False)
    metadata = MetaData(schema=database)
    metadata.reflect(bind=engine)

    return metadata.tables.get(f"{database}.{table}")


@deprecated("Use Adapter.list_tables()")
def list_tables(dsn: str, database: str) -> List[Table]:
    engine = create_engine(dsn, echo=False)
    metadata = MetaData(schema=database)
    metadata.reflect(bind=engine)

    return pydash.sort_by(list(metadata.tables.values()), lambda table: table.name)


class Adapter(ABC):
    def __init__(self, settings: DBSettings):
        self.settings = settings
        self.dsn = create_connection_url(**settings.model_dump())

    @abstractmethod
    def get_client():
        pass

    @abstractmethod
    def has_database(self, database: str) -> bool:
        pass

    @abstractmethod
    def create_database(self, database: str) -> None:
        pass

    @abstractmethod
    def drop_database(self, database: str) -> None:
        pass

    @abstractmethod
    def has_schema():
        pass

    @overload
    @abstractmethod
    def has_table(self, table: str, database: Optional[str] = None) -> bool: ...

    @overload
    @abstractmethod
    def has_table(self, table: str, schema: Optional[str] = None) -> bool: ...

    @abstractmethod
    def has_table(self, *args, **kwargs) -> bool:
        pass

    @overload
    @abstractmethod
    def get_table_schema(self, table: str, database: Optional[str] = None) -> Table: ...

    @overload
    @abstractmethod
    def get_table_schema(self, table: str, schema: Optional[str] = None) -> Table: ...

    @abstractmethod
    def get_table_schema(self, *args, **kwargs) -> Table:
        pass

    @overload
    @abstractmethod
    def set_table_replica_identity(
        self, table: str, replica_identity: str, database: Optional[str] = None
    ) -> None: ...

    @overload
    @abstractmethod
    def set_table_replica_identity(
        self, table: str, replica_identity: str, schema: Optional[str] = None
    ) -> None: ...

    @abstractmethod
    def set_table_replica_identity(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def create_table(self, table: str, statement: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def create_table(self, table: str, statement: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def create_table(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def drop_table(self, table: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def drop_table(self, table: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def drop_table(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def list_tables(self, database: Optional[str] = None) -> List[Table]: ...

    @overload
    @abstractmethod
    def list_tables(self, schema: Optional[str] = None) -> List[Table]: ...

    @abstractmethod
    def list_tables(self, *args, **kwargs) -> List[Table]:
        pass

    @abstractmethod
    def has_user(self, username: str) -> bool:
        pass

    @abstractmethod
    def create_user(self, username: str, password: str) -> None:
        pass

    @abstractmethod
    def drop_user(self, username: str) -> None:
        pass

    @overload
    @abstractmethod
    def grant_user_privileges(self, username: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def grant_user_privileges(self, username: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def grant_user_privileges(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def revoke_user_privileges(self, username: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def revoke_user_privileges(self, username: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def revoke_user_privileges(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def has_publication(self, publication: str) -> bool:
        pass

    @abstractmethod
    def create_publication(self, publication: str, tables: List[str]) -> None:
        pass

    @abstractmethod
    def drop_publication(self, publication: str) -> None:
        pass


class CHAdapter(Adapter):
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

    def grant_user_privileges(self, username: str, database: Optional[str] = None) -> None:
        raise NotImplementedError()

    def revoke_user_privileges(self, username: str, database: Optional[str] = None) -> None:
        raise NotImplementedError()

    def has_publication(self, publication: str) -> bool:
        raise NotImplementedError()

    def create_publication(self, publication: str, tables: List[str]) -> None:
        raise NotImplementedError()

    def drop_publication(self, publication: str) -> None:
        raise NotImplementedError()


class PGAdapter(Adapter):
    def get_client():
        raise NotImplementedError()

    def has_database(self, database: str) -> bool:
        raise NotImplementedError()

    def create_database(self, database: str) -> None:
        raise NotImplementedError()

    def drop_database(self, database: str) -> None:
        raise NotImplementedError()

    def has_schema():
        raise NotImplementedError()

    def has_table(self, table: str, schema: Optional[str] = None) -> bool:
        if not schema:
            schema = "public"

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                "select 1 from pg_catalog.pg_tables where schemaname = %s and tablename = %s;",
                [schema, table],
            )
            return bool(cur.fetchall())

    def get_table_schema(self, table: str, schema: Optional[str] = None) -> Table:
        raise NotImplementedError()

    def set_table_replica_identity(
        self, table: str, replica_identity: str, schema: Optional[str] = None
    ) -> None:
        if not schema:
            schema = "public"

        if not self.has_table(table, schema=schema):
            return

        identifier = PGTableIdentifier.from_string(table)
        if schema is not None:
            identifier.schema_ = schema

        quoted_table = identifier.to_string()

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(f"alter table {quoted_table} replica identity {replica_identity};")

    def create_table(self, table: str, statement: str, schema: Optional[str] = None) -> None:
        raise NotImplementedError()

    def drop_table(self, table: str, schema: Optional[str] = None) -> None:
        raise NotImplementedError()

    def list_tables(self, schema: Optional[str] = None) -> List[Table]:
        raise NotImplementedError()

    def has_user(self, username: str) -> bool:
        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute("select 1 from pg_catalog.pg_user where usename = %s;", [username])
            return bool(cur.fetchall())

    def create_user(self, username: str, password: str, options: Optional[dict] = None) -> None:
        if self.has_user(username):
            return

        quoted_username = PGIdentifier.quote(username)

        computed_options = []
        if options:
            if options.get("login"):
                computed_options.append("login")
            if options.get("replication"):
                computed_options.append("replication")

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                f"create user {quoted_username} with {' '.join(computed_options)} password %(password)s;",
                {"password": password},
            )

    def drop_user(self, username: str) -> None:
        if not self.has_user(username):
            return

        quoted_username = PGIdentifier.quote(username)

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                f"""
                drop owned by {quoted_username} cascade;
                drop user {quoted_username};
                """
            )

    def grant_user_privileges(self, username: str, schema: Optional[str] = None) -> None:
        if not self.has_user(username):
            return

        if not schema:
            schema = "public"

        quoted_username = PGIdentifier.quote(username)
        quoted_schema = PGIdentifier.quote(schema)

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                f"""
                grant usage on schema {quoted_schema} to {quoted_username};
                grant select on all tables in schema {quoted_schema} to {quoted_username};
                alter default privileges in schema {quoted_schema} grant select on tables to {quoted_username};
                """
            )

    def revoke_user_privileges(self, username: str, schema: Optional[str] = None) -> None:
        if not self.has_user(username):
            return

        if not schema:
            schema = "public"

        quoted_username = PGIdentifier.quote(username)
        quoted_schema = PGIdentifier.quote(schema)

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                f"""
                alter default privileges for user {quoted_username} in schema {quoted_schema} revoke select on tables from {quoted_username};
                revoke select on all tables in schema {quoted_schema} from {quoted_username};
                revoke usage on schema {quoted_schema} from {quoted_username};
                -- reassign owned by {quoted_username} to postgres;
                """
            )

    def has_publication(self, publication: str) -> bool:
        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute("select 1 from pg_publication where pubname = %s;", [publication])
            return bool(cur.fetchall())

    def create_publication(self, publication: str, tables: List[str]) -> None:
        if self.has_publication(publication):
            self.drop_publication(publication)

        quoted_publication = PGIdentifier.quote(publication)
        quoted_tables = [PGTableIdentifier.from_string(table).to_string() for table in tables]

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                f"create publication {quoted_publication} for table {", ".join(quoted_tables)};"
            )

    def drop_publication(self, publication: str) -> None:
        if not self.has_publication(publication):
            return

        quoted_publication = PGIdentifier.quote(publication)

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(f"drop publication if exists {quoted_publication};")
