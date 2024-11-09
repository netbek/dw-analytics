from jinja2 import Environment
from typing import Any, Optional

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
