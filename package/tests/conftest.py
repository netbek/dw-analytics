from package.database import build_connection_url
from package.tests.fixtures.database import *  # noqa
from typing import Any, Generator

import os
import pytest


@pytest.fixture(scope="session")
def clickhouse_url() -> Generator[str, Any, None]:
    yield build_connection_url(
        type="clickhouse",
        driver=os.getenv("DEFAULT_TEST_CLICKHOUSE_DRIVER"),
        host=os.getenv("DEFAULT_TEST_CLICKHOUSE_HOST"),
        port=os.getenv("DEFAULT_TEST_CLICKHOUSE_PORT"),
        username=os.getenv("DEFAULT_TEST_CLICKHOUSE_USERNAME"),
        password=os.getenv("DEFAULT_TEST_CLICKHOUSE_PASSWORD"),
        database=os.getenv("DEFAULT_TEST_CLICKHOUSE_DATABASE"),
    )
