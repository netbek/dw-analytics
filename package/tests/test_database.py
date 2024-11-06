from clickhouse_connect.driver.client import Client
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
