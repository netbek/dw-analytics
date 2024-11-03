from clickhouse_connect.driver.client import Client
from collections.abc import Generator
from contextlib import contextmanager
from jinja2 import Environment
from typing import Any, Optional

import clickhouse_connect
import psycopg2
import re
import sqlparse

RE_HAS_JINJA = re.compile(r"({[{%#]|[#}%]})")

jinja_env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])


@contextmanager
def get_clickhouse_client(dsn: str) -> Generator[Client | None]:
    client = clickhouse_connect.get_client(dsn=dsn)
    try:
        yield client
    finally:
        client.close()


@contextmanager
def get_postgres_client(dsn: str):
    conn = psycopg2.connect(dsn=dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            yield conn, cur
    finally:
        conn.close()


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
