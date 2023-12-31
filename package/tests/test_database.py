import os


def test_database_exists(db):
    db_name = os.environ["CLICKHOUSE_TEST_DATABASE"]
    sql = f"select exists (select 1 from system.databases where name = '{db_name}');"
    actual = db.execute(sql).scalar()
    expected = 1
    assert actual == expected
