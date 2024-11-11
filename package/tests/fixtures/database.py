from package.config.settings import get_settings
from package.database import CHAdapter, PGAdapter
from sqlmodel import Session, SQLModel
from typing import Any, Generator, List

import pydash
import pytest

settings = get_settings()


class DBTest:
    @pytest.fixture(scope="session")
    def ch_adapter(self) -> Generator[CHAdapter, Any, None]:
        yield CHAdapter(settings.test_ch)

    @pytest.fixture(scope="session")
    def ch_database(self, ch_adapter: CHAdapter) -> Generator[List[str], Any, None]:
        # Get database names
        databases = [table.schema for table in SQLModel.metadata.tables.values() if table.schema]
        databases = pydash.uniq(databases)
        databases = pydash.without(databases, "default")

        with ch_adapter.create_client() as client:
            for database in databases:
                client.command(
                    "DROP DATABASE IF EXISTS {database:Identifier};",
                    parameters={"database": database},
                )
                client.command(
                    "CREATE DATABASE {database:Identifier};",
                    parameters={"database": database},
                )

        with ch_adapter.create_engine() as engine:
            SQLModel.metadata.create_all(engine)

        yield databases

        with ch_adapter.create_client() as client:
            for database in databases:
                client.command(
                    "DROP DATABASE IF EXISTS {database:Identifier};",
                    parameters={"database": database},
                )

    @pytest.fixture(scope="function")
    def ch_session(
        self, ch_adapter: CHAdapter, ch_database: List[str]
    ) -> Generator[Session, Any, None]:
        with ch_adapter.create_engine() as engine:
            session = Session(engine)

        yield session

        session.close()

        with ch_adapter.create_client() as client:
            for table in SQLModel.metadata.tables.values():
                client.command(
                    "TRUNCATE TABLE {schema:Identifier}.{name:Identifier};",
                    parameters={"schema": table.schema, "name": table.name},
                )

    @pytest.fixture(scope="session")
    def pg_adapter(self) -> Generator[PGAdapter, Any, None]:
        yield PGAdapter(settings.test_pg)
