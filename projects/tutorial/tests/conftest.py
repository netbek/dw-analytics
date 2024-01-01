from package.tests.conftest import (  # noqa: F401
    generate_test_database_connection_url,
    test_database_connection_url,
)

pytest_plugins = "package.tests.fixtures.database"
