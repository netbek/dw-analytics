from package.database import create_connection_url, DBSettings
from package.tests.fixtures.database import *  # noqa: F403
from package.utils.environ_utils import get_env_var
from typing import Any, Generator

import pytest


@pytest.fixture(scope="session")
def ch_settings() -> Generator[DBSettings, Any, None]:
    yield DBSettings(
        type="clickhouse",
        driver=get_env_var("DEFAULT_TEST_CLICKHOUSE_DRIVER"),
        host=get_env_var("DEFAULT_TEST_CLICKHOUSE_HOST"),
        port=get_env_var("DEFAULT_TEST_CLICKHOUSE_PORT"),
        username=get_env_var("DEFAULT_TEST_CLICKHOUSE_USERNAME"),
        password=get_env_var("DEFAULT_TEST_CLICKHOUSE_PASSWORD"),
        database=get_env_var("DEFAULT_TEST_CLICKHOUSE_DATABASE"),
    )


@pytest.fixture(scope="session")
def ch_url(ch_settings: DBSettings) -> Generator[str, Any, None]:
    yield create_connection_url(**ch_settings.model_dump())
