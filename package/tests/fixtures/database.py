from clickhouse_connect.driver.client import Client
from sqlalchemy import Engine
from sqlmodel import create_engine, Session, SQLModel, text
from typing import Any, Generator

import clickhouse_connect
import psycopg2
import pydash
import pytest


@pytest.fixture(scope="session")
def clickhouse_client(clickhouse_url: str) -> Generator[Client, Any, None]:
    client = clickhouse_connect.get_client(dsn=clickhouse_url)

    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="session")
def clickhouse_engine(clickhouse_url: str) -> Generator[Engine, Any, None]:
    yield create_engine(clickhouse_url, echo=True)


@pytest.fixture(scope="session")
def clickhouse_session(clickhouse_engine) -> Generator[Session, Any, None]:
    session = Session(clickhouse_engine)

    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="session")
def clickhouse_database(clickhouse_engine: Engine, clickhouse_session: Session):
    # Get database names
    schemas = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
    schemas = pydash.uniq(schemas)
    schemas = pydash.without(schemas, "default")

    # Create databases
    for schema in schemas:
        clickhouse_session.exec(text("DROP DATABASE IF EXISTS {};".format(f"`{schema}`")))
        clickhouse_session.exec(text("CREATE DATABASE {};".format(f"`{schema}`")))

    # Create tables
    SQLModel.metadata.create_all(clickhouse_engine)

    yield

    # Drop tables
    SQLModel.metadata.drop_all(clickhouse_engine)

    # Drop databases
    for schema in schemas:
        clickhouse_session.exec(text("DROP DATABASE IF EXISTS {};".format(f'"{schema}"')))


@pytest.fixture(scope="session")
def postgres_connection(
    postgres_url: str,
) -> Generator[psycopg2.extensions.connection, Any, None]:
    connection = psycopg2.connect(dsn=postgres_url)

    yield connection

    connection.close()


@pytest.fixture(scope="session")
def postgres_cursor(
    postgres_connection: psycopg2.extensions.connection,
) -> Generator[psycopg2.extensions.cursor, Any, None]:
    with postgres_connection.cursor() as cursor:
        yield cursor


@pytest.fixture(scope="session")
def postgres_engine(postgres_url: str) -> Generator[Engine, Any, None]:
    yield create_engine(postgres_url, echo=True)


@pytest.fixture(scope="session")
def postgres_session(postgres_engine: Engine) -> Generator[Session, Any, None]:
    session = Session(postgres_engine)

    try:
        yield session
    finally:
        session.close()
