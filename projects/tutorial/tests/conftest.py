from package.database import create_connection_url
from package.project import Project
from package.tests.fixtures.database import *  # noqa: F403
from typing import Any, Generator

import pytest

project = Project.from_path(__file__)


@pytest.fixture(scope="session")
def ch_url() -> Generator[str, Any, None]:
    yield create_connection_url(**project.test_ch_settings.model_dump())
