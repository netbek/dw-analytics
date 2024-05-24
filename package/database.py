from jinja2 import Environment
from typing import Any, Dict, Optional

import clickhouse_connect
import re
import sqlparse

RE_HAS_JINJA = re.compile(r"({[{%#]|[#}%]})")

jinja_env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.disconnect()

    @property
    def client(self):
        return self._client

    def connect(self):
        self._client = clickhouse_connect.get_client(dsn=self._dsn)

    def disconnect(self):
        if self._client:
            self._client.close()

    def rollback(self):
        # https://clickhouse.com/docs/en/guides/developer/transactional#transactions-commit-and-rollback
        pass


def build_connection_url(
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
    statement: str, context: Optional[Dict[str, Any]] = None, pretty: bool = False
) -> str:
    if RE_HAS_JINJA.search(statement):
        statement = jinja_env.from_string(statement, context).render()

    statement = statement.strip()

    if pretty:
        statement = sqlparse.format(
            statement, reindent=True, keyword_case="lower", identifier_case="lower"
        )

    return statement
