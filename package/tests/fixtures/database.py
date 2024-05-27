import clickhouse_connect
import pytest


@pytest.fixture(scope="function")
def db_client(test_database_connection_url):
    client = clickhouse_connect.get_client(dsn=test_database_connection_url)
    try:
        yield client
    finally:
        client.close()
