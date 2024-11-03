from clickhouse_connect.driver.client import Client
from sqlmodel import Session, text

import os


def test_clickhouse_client(clickhouse_client: Client):
    db_name = os.environ["DEFAULT_TEST_CLICKHOUSE_DATABASE"]
    actual = clickhouse_client.query(
        "select 1 from system.databases where name = {db_name:String};",
        parameters={"db_name": db_name},
    ).result_rows
    assert actual == [(1,)]


def test_clickhouse_session(clickhouse_session: Session):
    db_name = os.environ["DEFAULT_TEST_CLICKHOUSE_DATABASE"]
    actual = clickhouse_session.exec(
        text("select 1 from system.databases where name = :db_name;").bindparams(db_name=db_name)
    ).all()
    assert actual == [(1,)]
