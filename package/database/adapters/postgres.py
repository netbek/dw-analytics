from ..types import PGSettings
from .base import BaseAdapter
from collections.abc import Generator
from contextlib import contextmanager
from package.types import PGIdentifier, PGTableIdentifier
from sqlmodel import create_engine, MetaData, Table
from typing import Any, List, Optional

import psycopg2

DEFAULT_SCHEMA = "public"


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


class PGAdapter(BaseAdapter):
    def __init__(self, settings: PGSettings) -> None:
        super().__init__(settings)

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
            schema = DEFAULT_SCHEMA

        with get_postgres_client(self.dsn) as (conn, cur):
            cur.execute(
                "select 1 from pg_catalog.pg_tables where schemaname = %s and tablename = %s;",
                [schema, table],
            )
            return bool(cur.fetchall())

    def get_table_schema(self, table: str, schema: Optional[str] = None) -> Table:
        if not schema:
            schema = DEFAULT_SCHEMA

        engine = create_engine(self.dsn, echo=False)
        metadata = MetaData(schema=schema)
        metadata.reflect(bind=engine)

        return metadata.tables.get(f"{schema}.{table}")

    def set_table_replica_identity(
        self, table: str, replica_identity: str, schema: Optional[str] = None
    ) -> None:
        if not schema:
            schema = DEFAULT_SCHEMA

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

    def get_create_table_statement(self, table: str, schema: Optional[str] = None) -> None:
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

    def grant_user_privileges(self, username: str, schema: str) -> None:
        if not self.has_user(username):
            return

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

    def revoke_user_privileges(self, username: str, schema: str) -> None:
        if not self.has_user(username):
            return

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
