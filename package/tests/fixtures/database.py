from package.database import CHClient, DBSession
from sqlalchemy import Engine
from sqlmodel import create_engine, SQLModel
from typing import Any, Generator

import clickhouse_connect
import psycopg2
import pydash
import pytest


@pytest.fixture(scope="session")
def clickhouse_client(clickhouse_url: str) -> Generator[CHClient, Any, None]:
    client = clickhouse_connect.get_client(dsn=clickhouse_url)

    yield client

    client.close()


@pytest.fixture(scope="session")
def clickhouse_engine(clickhouse_url: str) -> Generator[Engine, Any, None]:
    yield create_engine(clickhouse_url, echo=False)


@pytest.fixture(scope="session")
def clickhouse_database(
    clickhouse_engine: Engine, clickhouse_client: CHClient
) -> Generator[None, Any, None]:
    # Get database names
    schemas = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
    schemas = pydash.uniq(schemas)
    schemas = pydash.without(schemas, "default")

    # Create databases
    for schema in schemas:
        clickhouse_client.command(f"DROP DATABASE IF EXISTS `{schema}`;")
        clickhouse_client.command(f"CREATE DATABASE `{schema}`;")

    # Create tables
    SQLModel.metadata.create_all(clickhouse_engine)

    yield

    # Drop databases
    for schema in schemas:
        clickhouse_client.command(f"DROP DATABASE IF EXISTS `{schema}`;")


@pytest.fixture(scope="function")
def clickhouse_session(
    clickhouse_engine: Engine, clickhouse_client: CHClient, clickhouse_database: None
) -> Generator[DBSession, Any, None]:
    session = DBSession(clickhouse_engine)

    yield session

    session.close()

    for table in SQLModel.metadata.tables.values():
        clickhouse_client.command(f"TRUNCATE TABLE `{table.schema}`.`{table.name}`;")


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


@pytest.fixture(scope="function")
def postgres_session(postgres_engine: Engine) -> Generator[DBSession, Any, None]:
    session = DBSession(postgres_engine)

    yield session

    session.close()
