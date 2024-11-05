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

    yield client

    client.close()


@pytest.fixture(scope="session")
def clickhouse_engine(clickhouse_url: str) -> Generator[Engine, Any, None]:
    yield create_engine(clickhouse_url, echo=False)


@pytest.fixture(scope="session")
def clickhouse_database(clickhouse_engine: Engine) -> Generator[None, Any, None]:
    session = Session(clickhouse_engine)

    # Get database names
    schemas = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
    schemas = pydash.uniq(schemas)
    schemas = pydash.without(schemas, "default")

    # Create databases
    for schema in schemas:
        session.exec(text(f"DROP DATABASE IF EXISTS `{schema}`;"))
        session.exec(text(f"CREATE DATABASE `{schema}`;"))

    # Create tables
    SQLModel.metadata.create_all(clickhouse_engine)

    session.close()

    yield

    session = Session(clickhouse_engine)

    # Drop databases
    for schema in schemas:
        session.exec(text(f"DROP DATABASE IF EXISTS `{schema}`;"))

    session.close()


@pytest.fixture(scope="function")
def clickhouse_session(
    clickhouse_engine: Engine, clickhouse_database: None
) -> Generator[Session, Any, None]:
    session = Session(clickhouse_engine)

    yield session

    for table in SQLModel.metadata.tables.values():
        session.exec(text(f"TRUNCATE TABLE `{table.schema}`.`{table.name}`;"))

    session.close()


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
    yield create_engine(postgres_url, echo=False)


@pytest.fixture(scope="session")
def postgres_session(postgres_engine: Engine) -> Generator[Session, Any, None]:
    session = Session(postgres_engine)

    try:
        yield session
    finally:
        session.close()
