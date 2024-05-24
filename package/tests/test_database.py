import os


def test_database_exists(db):
    db_name = os.environ["DEFAULT_CLICKHOUSE_DATABASE"]  # TODO Isolate from environment
    sql = f"select exists (select 1 from system.databases where name = '{db_name}_testing');"
    actual = db.client.query(sql).first_row[0]
    expected = 1
    assert actual == expected
