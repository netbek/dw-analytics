from package.database import create_connection_url
from package.tests.fixtures.database import *  # noqa: F403
from typing import Any, Generator

import os
import pytest


@pytest.fixture(scope="session")
def ch_url() -> Generator[str, Any, None]:
    yield create_connection_url(
        type="clickhouse",
        driver=os.getenv("DEFAULT_TEST_CLICKHOUSE_DRIVER"),
        host=os.getenv("DEFAULT_TEST_CLICKHOUSE_HOST"),
        port=os.getenv("DEFAULT_TEST_CLICKHOUSE_PORT"),
        username=os.getenv("DEFAULT_TEST_CLICKHOUSE_USERNAME"),
        password=os.getenv("DEFAULT_TEST_CLICKHOUSE_PASSWORD"),
        database=os.getenv("DEFAULT_TEST_CLICKHOUSE_DATABASE"),
    )
