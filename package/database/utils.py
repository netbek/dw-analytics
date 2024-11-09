from clickhouse_connect.driver.client import Client as CHClient
from collections.abc import Generator
from contextlib import contextmanager
from jinja2 import Environment
from sqlmodel import create_engine, MetaData, Table
from typing import Any, List, Optional
from typing_extensions import deprecated

import clickhouse_connect
import psycopg2
import pydash
import re
import sqlparse

RE_HAS_JINJA = re.compile(r"({[{%#]|[#}%]})")

jinja_env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])


def create_clickhouse_url(
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


@contextmanager
def get_clickhouse_client(dsn: str) -> Generator[CHClient | None]:
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        yield client
    finally:
        client.close()


def create_postgres_url(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    schema: str,
) -> str:
    scheme = "postgresql"

    return f"{scheme}://{username}:{password}@{host}:{port}/{database}"


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


@deprecated("Use Adapter.get_create_table_statement()")
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
