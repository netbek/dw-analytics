from package.database import build_connection_url, get_client
from typing import Generator, Optional
from urllib.parse import urlsplit, urlunsplit

import os
import pytest

pytest_plugins = "package.tests.fixtures.database"


@pytest.fixture(scope="session")
def database_connection_url():
    return build_connection_url(
        type="clickhouse",
        driver=os.getenv("DEFAULT_TARGET_CLICKHOUSE_DRIVER"),
        host=os.getenv("DEFAULT_TARGET_CLICKHOUSE_HOST"),
        port=os.getenv("DEFAULT_TARGET_CLICKHOUSE_PORT"),
        username=os.getenv("DEFAULT_TARGET_CLICKHOUSE_USERNAME"),
        password=os.getenv("DEFAULT_TARGET_CLICKHOUSE_PASSWORD"),
        database=os.getenv("DEFAULT_TARGET_CLICKHOUSE_DATABASE"),
    )


@pytest.fixture(scope="session")
def generate_test_database_connection_url(
    database_connection_url: str,
) -> Generator[Optional[str], None, None]:
    if not database_connection_url:
        yield None
        return

    scheme, netloc, database, query, fragment = urlsplit(database_connection_url)
    test_db_name = database.strip("/") + "_testing"
    quoted_db_name = f'"{test_db_name}"'

    with get_client(database_connection_url) as client:
        client.command(f"drop database if exists {quoted_db_name}")
        client.command(f"create database {quoted_db_name}")

    new_url = urlunsplit((scheme, netloc, test_db_name, query, fragment))

    yield new_url

    with get_client(database_connection_url) as client:
        client.command(f"drop database if exists {quoted_db_name}")


@pytest.fixture(scope="session")
def test_database_connection_url(generate_test_database_connection_url):
    yield generate_test_database_connection_url
