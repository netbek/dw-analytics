from package.config.settings import get_settings
from package.database import CHAdapter, PGAdapter
from sqlmodel import Session
from typing import Any, Generator

import pytest

settings = get_settings()


class DBTest:
    @pytest.fixture(scope="session")
    def ch_adapter(self) -> Generator[CHAdapter, Any, None]:
        yield CHAdapter(settings.test_clickhouse)

    @pytest.fixture(scope="function")
    def ch_session(self, ch_adapter: CHAdapter) -> Generator[Session, Any, None]:
        with ch_adapter.create_engine() as engine:
            session = Session(engine)

        yield session

        session.close()

    @pytest.fixture(scope="session")
    def pg_adapter(self) -> Generator[PGAdapter, Any, None]:
        yield PGAdapter(settings.test_postgres)
