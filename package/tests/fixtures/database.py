from package.database import Database

import pytest


@pytest.fixture(scope="function")
def db(test_database_connection_url):
    db = Database(test_database_connection_url)
    db.connect()

    yield db

    db.rollback()
    db.disconnect()
