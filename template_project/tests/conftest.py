from ..config.settings import get_settings
from package.tests.conftest import (  # noqa: F401
    generate_test_database_connection_url,
    test_database_connection_url,
)

import pytest

pytest_plugins = "package.tests.fixtures.database"
settings = get_settings()


@pytest.fixture(scope="session")
def database_connection_url() -> str:
    return settings.db.URL
