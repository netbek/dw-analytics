from package.config.settings import TestCHSettings, TestPGSettings
from package.database import CHAdapter, CHClient, DBSession, PGAdapter
from sqlalchemy import Engine
from sqlmodel import create_engine, SQLModel
from typing import Any, Generator

import clickhouse_connect
import psycopg2
import pydash
import pytest


@pytest.fixture(scope="session")
def ch_adapter(ch_settings: TestCHSettings) -> Generator[CHAdapter, Any, None]:
    yield CHAdapter(ch_settings)


@pytest.fixture(scope="session")
def ch_client(ch_settings: TestCHSettings) -> Generator[CHClient, Any, None]:
    client = clickhouse_connect.get_client(dsn=ch_settings.to_url())

    yield client

    client.close()


@pytest.fixture(scope="session")
def ch_engine(ch_settings: TestCHSettings) -> Generator[Engine, Any, None]:
    yield create_engine(ch_settings.to_url(), echo=False)


@pytest.fixture(scope="session")
def ch_database(ch_engine: Engine, ch_client: CHClient) -> Generator[None, Any, None]:
    # Get database names
    schemas = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
    schemas = pydash.uniq(schemas)
    schemas = pydash.without(schemas, "default")

    # Create databases
    for schema in schemas:
        ch_client.command(
            "DROP DATABASE IF EXISTS {schema:Identifier};", parameters={"schema": schema}
        )
        ch_client.command("CREATE DATABASE {schema:Identifier};", parameters={"schema": schema})

    # Create tables
    SQLModel.metadata.create_all(ch_engine)

    yield

    # Drop databases
    for schema in schemas:
        ch_client.command(
            "DROP DATABASE IF EXISTS {schema:Identifier};", parameters={"schema": schema}
        )


@pytest.fixture(scope="function")
def ch_session(
    ch_engine: Engine, ch_client: CHClient, ch_database: None
) -> Generator[DBSession, Any, None]:
    session = DBSession(ch_engine)

    yield session

    session.close()

    for table in SQLModel.metadata.tables.values():
        ch_client.command(
            "TRUNCATE TABLE {schema:Identifier}.{name:Identifier};",
            parameters={"schema": table.schema, "name": table.name},
        )


@pytest.fixture(scope="session")
def pg_adapter(pg_settings: TestPGSettings) -> Generator[PGAdapter, Any, None]:
    yield PGAdapter(pg_settings)


@pytest.fixture(scope="session")
def pg_connection(
    pg_settings: TestPGSettings,
) -> Generator[psycopg2.extensions.connection, Any, None]:
    connection = psycopg2.connect(dsn=pg_settings.to_url())

    yield connection

    connection.close()


@pytest.fixture(scope="session")
def pg_cursor(
    pg_connection: psycopg2.extensions.connection,
) -> Generator[psycopg2.extensions.cursor, Any, None]:
    with pg_connection.cursor() as cursor:
        yield cursor


@pytest.fixture(scope="session")
def pg_engine(pg_settings: TestPGSettings) -> Generator[Engine, Any, None]:
    yield create_engine(pg_settings.to_url(), echo=False)


@pytest.fixture(scope="function")
def pg_session(pg_engine: Engine, pg_database: None) -> Generator[DBSession, Any, None]:
    session = DBSession(pg_engine)

    yield session

    session.close()


@pytest.fixture(scope="session")
def pg_database(
    pg_settings: TestPGSettings, pg_cursor: psycopg2.extensions.cursor
) -> Generator[None, Any, None]:
    pass

    # pg_cursor.execute("drop database if exists ...")

    # # Get database names
    # schemas = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
    # schemas = pydash.uniq(schemas)
    # schemas = pydash.without(schemas, "default")

    # # Create databases
    # for schema in schemas:
    #     ch_client.command(
    #         "DROP DATABASE IF EXISTS {schema:Identifier};", parameters={"schema": schema}
    #     )
    #     ch_client.command("CREATE DATABASE {schema:Identifier};", parameters={"schema": schema})

    # # Create tables
    # SQLModel.metadata.create_all(ch_engine)

    # yield

    # # Drop databases
    # for schema in schemas:
    #     ch_client.command(
    #         "DROP DATABASE IF EXISTS {schema:Identifier};", parameters={"schema": schema}
    #     )
