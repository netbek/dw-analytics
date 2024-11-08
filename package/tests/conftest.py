from package.database import create_connection_url
from package.database.types import CHSettings, PGSettings
from package.tests.fixtures.database import *  # noqa: F403
from package.utils.environ_utils import get_env_var
from typing import Any, Generator

import pytest


@pytest.fixture(scope="session")
def ch_settings() -> Generator[CHSettings, Any, None]:
    yield CHSettings(
        driver=get_env_var("TEST_CLICKHOUSE_DRIVER"),
        host=get_env_var("TEST_CLICKHOUSE_HOST"),
        port=get_env_var("TEST_CLICKHOUSE_PORT"),
        username=get_env_var("TEST_CLICKHOUSE_USERNAME"),
        password=get_env_var("TEST_CLICKHOUSE_PASSWORD"),
        database=get_env_var("TEST_CLICKHOUSE_DATABASE"),
    )


@pytest.fixture(scope="session")
def ch_url(ch_settings: CHSettings) -> Generator[str, Any, None]:
    yield create_connection_url(**ch_settings.model_dump())


@pytest.fixture(scope="session")
def pg_settings() -> Generator[PGSettings, Any, None]:
    yield PGSettings(
        host="host.docker.internal",
        port="5432",
        username="postgres",
        password="postgres",
        database="postgres",
    )
