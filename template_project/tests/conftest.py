from package.database import build_connection_url
from package.project import Project
from package.tests.fixtures.database import *  # noqa
from typing import Any, Generator

import pytest

project = Project.from_path(__file__)


@pytest.fixture(scope="session")
def clickhouse_url() -> Generator[str, Any, None]:
    yield build_connection_url(**project.test_db_settings)
