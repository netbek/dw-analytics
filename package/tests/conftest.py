from package.config.settings import TestCHSettings, TestPGSettings
from package.tests.fixtures.database import *  # noqa: F403
from typing import Any, Generator

import pytest


@pytest.fixture(scope="session")
def ch_settings() -> Generator[TestCHSettings, Any, None]:
    yield TestCHSettings()


@pytest.fixture(scope="session")
def pg_settings() -> Generator[TestPGSettings, Any, None]:
    yield TestPGSettings()
