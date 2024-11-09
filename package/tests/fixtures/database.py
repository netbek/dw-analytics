from package.config.settings import TestCHSettings, TestPGSettings
from package.database import CHAdapter, DBSession, PGAdapter
from sqlmodel import SQLModel
from typing import Any, Generator

import pydash
import pytest


@pytest.fixture(scope="session")
def ch_adapter(ch_settings: TestCHSettings) -> Generator[CHAdapter, Any, None]:
    yield CHAdapter(ch_settings)


@pytest.fixture(scope="session")
def ch_database(ch_adapter: CHAdapter) -> Generator[None, Any, None]:
    # Get database names
    schemas = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
    schemas = pydash.uniq(schemas)
    schemas = pydash.without(schemas, "default")

    with ch_adapter.get_client() as client:
        for schema in schemas:
            client.command(
                "DROP DATABASE IF EXISTS {schema:Identifier};", parameters={"schema": schema}
            )
            client.command("CREATE DATABASE {schema:Identifier};", parameters={"schema": schema})

    with ch_adapter.get_engine() as engine:
        SQLModel.metadata.create_all(engine)

    yield

    with ch_adapter.get_client() as client:
        for schema in schemas:
            client.command(
                "DROP DATABASE IF EXISTS {schema:Identifier};", parameters={"schema": schema}
            )


@pytest.fixture(scope="function")
def ch_session(ch_adapter: CHAdapter, ch_database: None) -> Generator[DBSession, Any, None]:
    with ch_adapter.get_engine() as engine:
        session = DBSession(engine)

    yield session

    session.close()

    with ch_adapter.get_client() as client:
        for table in SQLModel.metadata.tables.values():
            client.command(
                "TRUNCATE TABLE {schema:Identifier}.{name:Identifier};",
                parameters={"schema": table.schema, "name": table.name},
            )


@pytest.fixture(scope="session")
def pg_adapter(pg_settings: TestPGSettings) -> Generator[PGAdapter, Any, None]:
    yield PGAdapter(pg_settings)


@pytest.fixture(scope="function")
def pg_session(pg_adapter: PGAdapter) -> Generator[DBSession, Any, None]:
    with pg_adapter.get_engine() as engine:
        session = DBSession(engine)

    yield session

    session.close()
