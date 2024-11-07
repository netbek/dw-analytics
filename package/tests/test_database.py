from clickhouse_connect.driver.client import Client
from package.database import CHClient, ClickHouseAdapter, DBSettings
from package.types import CHTableIdentifier
from sqlmodel import Session, text

import os


def test_clickhouse_client(ch_client: Client):
    db_name = os.environ["DEFAULT_TEST_CLICKHOUSE_DATABASE"]
    actual = ch_client.query(
        "select 1 from system.databases where name = {db_name:String};",
        parameters={"db_name": db_name},
    ).result_rows
    assert actual == [(1,)]


def test_clickhouse_session(ch_session: Session):
    db_name = os.environ["DEFAULT_TEST_CLICKHOUSE_DATABASE"]
    actual = ch_session.exec(
        text("select 1 from system.databases where name = :db_name;").bindparams(db_name=db_name)
    ).all()
    assert actual == [(1,)]


class TestClickHouseAdapter:
    def test_has_database(self, ch_settings: DBSettings):
        adapter = ClickHouseAdapter(ch_settings)
        assert adapter.has_database("non_existent_db") is False
        assert adapter.has_database(ch_settings.database) is True

    def test_create_and_drop_database(self, ch_settings: DBSettings):
        adapter = ClickHouseAdapter(ch_settings)
        database = "test_database"

        assert adapter.has_database(database) is False

        adapter.create_database(database)
        assert adapter.has_database(database) is True

        adapter.drop_database(database)
        assert adapter.has_database(database) is False

    def test_has_table(self, ch_settings: DBSettings, ch_client: CHClient):
        adapter = ClickHouseAdapter(ch_settings)

        assert adapter.has_table(database=ch_settings.database, table="non_existent_table") is False
        assert adapter.has_table(table="non_existent_table") is False

    def test_get_table_schema(self, ch_settings: DBSettings, ch_client: CHClient):
        adapter = ClickHouseAdapter(ch_settings)

        table = adapter.get_table_schema("non_existent_table")
        assert table is None

        table = "test_table"
        table_identifier = CHTableIdentifier(database=ch_settings.database, table=table).to_string()
        statement = f"""
        CREATE OR REPLACE TABLE {table_identifier}
        (
            id UInt64,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY id
        """
        ch_client.command(statement)

        table_schema = adapter.get_table_schema(table)
        assert set(["id", "updated_at"]) == set([column.name for column in table_schema.columns])

        adapter.drop_table(table)

    def test_create_and_drop_table(self, ch_settings: DBSettings, ch_client: CHClient):
        adapter = ClickHouseAdapter(ch_settings)

        table = "test_table"
        table_identifier = CHTableIdentifier(database=ch_settings.database, table=table).to_string()
        statement = f"""
        CREATE OR REPLACE TABLE {table_identifier}
        (
            id UInt64,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY id
        """

        assert adapter.has_table(table) is False

        adapter.create_table(table, statement)
        assert adapter.has_table(table) is True

        adapter.drop_table(table)
        assert adapter.has_table(table) is False

    def test_list_tables(self, ch_settings: DBSettings, ch_client: CHClient):
        adapter = ClickHouseAdapter(ch_settings)

        assert adapter.list_tables() == []
        assert adapter.list_tables(ch_settings.database) == []

        table = "test_table"
        table_identifier = CHTableIdentifier(database=ch_settings.database, table=table).to_string()
        statement = f"""
        CREATE OR REPLACE TABLE {table_identifier}
        (
            id UInt64,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY id
        """
        ch_client.command(statement)

        tables = adapter.list_tables()
        assert set([table]) == set([table.name for table in tables])

        adapter.drop_table(table)

    def test_has_user(self, ch_settings: DBSettings):
        adapter = ClickHouseAdapter(ch_settings)
        assert adapter.has_user("non_existent_user") is False
        assert adapter.has_user(ch_settings.username) is True

    def test_create_and_drop_user(self, ch_settings: DBSettings):
        adapter = ClickHouseAdapter(ch_settings)
        username = "test_user"
        password = "secret"

        assert adapter.has_user(username) is False

        adapter.create_user(username, password)
        assert adapter.has_user(username) is True

        adapter.drop_user(username)
        assert adapter.has_user(username) is False
