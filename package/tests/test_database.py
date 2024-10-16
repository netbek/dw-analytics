from clickhouse_connect.driver.client import Client

import os


def test_database_exists(db_client: Client):
    db_name = os.environ["DEFAULT_TARGET_CLICKHOUSE_DATABASE"]  # TODO Isolate from environment
    sql = f"select exists (select 1 from system.databases where name = '{db_name}_testing');"
    actual = db_client.query(sql).first_row[0]
    expected = 1
    assert actual == expected
