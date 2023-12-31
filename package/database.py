from jinja2 import Environment
from sqlalchemy import create_engine, create_mock_engine, text, TextClause
from sqlalchemy.orm import sessionmaker
from typing import Any, Dict, Optional

import datetime
import re
import six
import sqlparse
import uuid

QUOTED_TYPES = (six.string_types, datetime.date, datetime.datetime, datetime.timedelta, uuid.UUID)
STRING_TYPES = (six.integer_types,)
RE_HAS_JINJA = re.compile(r"({[{%#]|[#}%]})")

jinja_env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])


class Database:
    def __init__(self, url: str):
        self._url = url
        self._engine = None
        self._session = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is not None:
            self._session.rollback()
        else:
            self._session.commit()
        self._session.close()

    def connect(self):
        self._engine = create_engine(self._url)
        self._session = sessionmaker(bind=self._engine)()

    def rollback(self):
        if self._session:
            self._session.rollback()

    def close(self):
        if self._session:
            self._session.close()

    def execute(
        self,
        statement: str,
        context: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        statement = _render_statement(statement, context=context, params=params)

        return self._session.execute(statement)

    def render_statement(
        self,
        statement: str,
        context: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        pretty: bool = False,
    ):
        statement = _render_statement(statement, context=context, params=params)
        stream = six.moves.cStringIO()
        engine = _create_mock_engine(self._engine, stream=stream)
        engine.execute(statement)
        string = stream.getvalue()

        if pretty:
            string = sqlparse.format(
                string, reindent=True, keyword_case="lower", identifier_case="lower"
            )

        return string


def _render_statement(
    statement: str,
    context: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> TextClause:
    if RE_HAS_JINJA.search(statement):
        statement = jinja_env.from_string(statement, context).render()

    statement = text(statement)

    if params:
        statement = statement.bindparams(**params)

    return statement


def _render_literal_value(value):
    if isinstance(value, QUOTED_TYPES):
        return f"'{str(value)}'"
    elif isinstance(value, STRING_TYPES):
        return str(value)
    else:
        return value


def _create_mock_engine(bind, stream=None):
    """
    Create a mock SQLAlchemy engine from the passed engine or bind URL.

    Adapted from https://github.com/kvesteri/sqlalchemy-utils/blob/e03646d51c5e9642152745326e2c308c0628f709/sqlalchemy_utils/functions/mock.py
    """
    if not isinstance(bind, six.string_types):
        bind_url = str(bind.url)
    else:
        bind_url = bind

    if stream is not None:

        def dump(sql, *args, **kwargs):
            class Compiler(type(sql._compiler(engine.dialect))):
                def visit_bindparam(self, bindparam, *args, **kwargs):
                    return self.render_literal_value(bindparam.value, bindparam.type)

                def render_literal_value(self, value, type_):
                    if isinstance(value, QUOTED_TYPES + STRING_TYPES):
                        return _render_literal_value(value)

                    elif isinstance(value, (list, tuple)):
                        return f'({", ".join(list(map(_render_literal_value, value)))})'

                    return super(Compiler, self).render_literal_value(value, type_)

            text = str(Compiler(engine.dialect, sql).process(sql))
            text = re.sub(r"\n+", "\n", text)
            text = text.strip("\n").strip()

            stream.write("\n%s;" % text)

    else:

        def dump(*args, **kw):
            return None

    engine = create_mock_engine(bind_url, executor=dump)

    return engine
