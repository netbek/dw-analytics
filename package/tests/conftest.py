from package.constants import CLICKHOUSE_TEST_DATABASE, CLICKHOUSE_URL
from package.database import Database
from typing import Generator, Optional
from urllib.parse import urlsplit, urlunsplit

import pytest

pytest_plugins = "package.tests.fixtures.database"


@pytest.fixture(scope="session")
def generate_test_database_connection_url() -> Generator[Optional[str], None, None]:
    original_url = CLICKHOUSE_URL

    if not original_url:
        yield None
        return

    scheme, netloc, database, query, fragment = urlsplit(original_url)
    test_db_name = CLICKHOUSE_TEST_DATABASE or database.strip("/") + "_testing"
    quoted_db_name = f'"{test_db_name}"'

    with Database(original_url) as db:
        db.execute(f"drop database if exists {quoted_db_name}")
        db.execute(f"create database {quoted_db_name}")

    new_url = urlunsplit((scheme, netloc, test_db_name, query, fragment))

    yield new_url

    with Database(original_url) as db:
        db.execute(f"drop database if exists {quoted_db_name}")


@pytest.fixture(scope="session")
def test_database_connection_url(generate_test_database_connection_url):
    yield generate_test_database_connection_url
