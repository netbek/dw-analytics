from ..config.constants import CLICKHOUSE_URL
from package.tests.conftest import (  # noqa: F401
    generate_test_database_connection_url,
    test_database_connection_url,
)

import pytest

pytest_plugins = "package.tests.fixtures.database"


@pytest.fixture(scope="session")
def database_connection_url() -> str:
    return CLICKHOUSE_URL
