from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from package.database import CHAdapter
from package.tests.asserts import assert_equal_ignoring_whitespace
from package.types import CHTableIdentifier
from sqlmodel import Session, text
from typing import Any, Generator

import os
import pytest


@pytest.fixture(scope="function")
def ch_table(ch_adapter: CHAdapter) -> Generator[str, Any, None]:
    table = "test_table"
    quoted_table = CHTableIdentifier(table=table).to_string()
    statement = f"""
    CREATE OR REPLACE TABLE {quoted_table}
    (
        id UInt64,
        updated_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY id
    """

    ch_adapter.create_table(table, statement)

    yield table

    ch_adapter.drop_table(table)


def test_clickhouse_client(ch_client: Client):
    database = os.environ["TEST_CLICKHOUSE_DATABASE"]
    actual = ch_client.query(
        "select 1 from system.databases where name = {database:String};",
        parameters={"database": database},
    ).result_rows
    assert actual == [(1,)]


def test_clickhouse_session(ch_session: Session):
    database = os.environ["TEST_CLICKHOUSE_DATABASE"]
    actual = ch_session.exec(
        text("select 1 from system.databases where name = :database;").bindparams(database=database)
    ).all()
    assert actual == [(1,)]


class TestCHAdapter:
    def test_has_database_non_existent_table(self, ch_adapter: CHAdapter):
        assert ch_adapter.has_database("non_existent_database") is False

    def test_has_database_existent_table(self, ch_adapter: CHAdapter):
        assert ch_adapter.has_database(ch_adapter.settings.database) is True

    def test_create_and_drop_database(self, ch_adapter: CHAdapter):
        database = "test_database"

        assert ch_adapter.has_database(database) is False

        ch_adapter.create_database(database)
        assert ch_adapter.has_database(database) is True

        ch_adapter.drop_database(database)
        assert ch_adapter.has_database(database) is False

    def test_has_table_non_existent_table(self, ch_adapter: CHAdapter):
        assert ch_adapter.has_table("non_existent_table") is False

    def test_has_table_existent_table(self, ch_adapter: CHAdapter, ch_table: str):
        assert ch_adapter.has_table(ch_table) is True

    def test_get_table_schema_non_existent_table(self, ch_adapter: CHAdapter):
        table = ch_adapter.get_table_schema("non_existent_table")
        assert table is None

    def test_get_table_schema_existent_table(self, ch_adapter: CHAdapter, ch_table: str):
        table = ch_adapter.get_table_schema(ch_table)
        assert set(["id", "updated_at"]) == set([column.name for column in table.columns])

    def test_create_and_drop_table(self, ch_adapter: CHAdapter):
        table = "test_table"
        quoted_table = CHTableIdentifier(table=table).to_string()
        statement = f"""
        CREATE OR REPLACE TABLE {quoted_table}
        (
            id UInt64,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY id
        """

        assert ch_adapter.has_table(table) is False

        ch_adapter.create_table(table, statement)
        assert ch_adapter.has_table(table) is True

        ch_adapter.drop_table(table)
        assert ch_adapter.has_table(table) is False

    def test_get_create_table_statement(self, ch_adapter: CHAdapter, ch_table: str):
        with pytest.raises(DatabaseError):
            ch_adapter.get_create_table_statement("non_existent_table")

        expected = f"""
        CREATE TABLE {ch_adapter.settings.database}.{ch_table}
        (
            `id` UInt64,
            `updated_at` DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY id
        SETTINGS index_granularity = 8192
        """
        assert_equal_ignoring_whitespace(ch_adapter.get_create_table_statement(ch_table), expected)

    def test_list_tables_empty_database(self, ch_adapter: CHAdapter):
        assert ch_adapter.list_tables() == []

    def test_list_tables_populated_database(self, ch_adapter: CHAdapter, ch_table: str):
        assert set([ch_table]) == set([table.name for table in ch_adapter.list_tables()])

    def test_has_user_non_existent_user(self, ch_adapter: CHAdapter):
        assert ch_adapter.has_user("non_existent_user") is False

    def test_has_user_existent_user(self, ch_adapter: CHAdapter):
        assert ch_adapter.has_user(ch_adapter.settings.username) is True

    def test_create_and_drop_user(self, ch_adapter: CHAdapter):
        username = "test_user"
        password = "secret"

        assert ch_adapter.has_user(username) is False

        ch_adapter.create_user(username, password)
        assert ch_adapter.has_user(username) is True

        ch_adapter.drop_user(username)
        assert ch_adapter.has_user(username) is False
