from clickhouse_sqlalchemy import types
from package.config.settings import get_settings
from package.database import CHAdapter
from package.tests.fixtures.database import DBTest
from package.types import CHTableIdentifier, DbtSource
from package.utils.sqlmodel_utils import (
    create_model_code,
    get_pydantic_type,
    get_python_type,
    get_sqlalchemy_type,
    parse_create_table_statement,
)
from sqlalchemy import Column
from sqlmodel import Table
from typing import Any, Generator, List

import datetime
import pytest
import uuid

settings = get_settings()


model_code = """
\"""
Created from:

CREATE TABLE default.test_table
(
    `id` UInt64,
    `created_at` DateTime DEFAULT now(),
    `updated_at` Nullable(DateTime64(9)),
    `_peerdb_synced_at` DateTime64(9) DEFAULT now64(),
    `_peerdb_is_deleted` Int8,
    `_peerdb_version` Int64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
\"""

from clickhouse_sqlalchemy import engines
from package.sqlalchemy.clickhouse import types
from sqlmodel import Column, Field, SQLModel
import datetime


class TestTable(SQLModel, table=True):
    __tablename__ = 'test_table'
    __table_args__ = (engines.MergeTree(order_by=('id',), index_granularity=8192), {'schema': 'default'},)

    id: int = Field(sa_column=Column(name='id', type_=types.UInt64, primary_key=True))
    created_at: datetime.datetime = Field(sa_column=Column(name='created_at', type_=types.DateTime, nullable=False))
    updated_at: datetime.datetime | None = Field(sa_column=Column(name='updated_at', type_=types.Nullable(types.DateTime64(9)), nullable=True))
    peerdb_synced_at: datetime.datetime = Field(sa_column=Column(name='_peerdb_synced_at', type_=types.DateTime64(9), nullable=False))
    peerdb_is_deleted: int = Field(sa_column=Column(name='_peerdb_is_deleted', type_=types.Int8, nullable=False))
    peerdb_version: int = Field(sa_column=Column(name='_peerdb_version', type_=types.Int64, nullable=False))
"""

factory_code = """
from .test_table import TestTable
from package.polyfactory.factories.sqlmodel_factory import SQLModelFactory
from package.polyfactory.mixins import PeerDBMixin
import pydash


class TestTableFactory(PeerDBMixin, SQLModelFactory[TestTable]):
    __random_seed__ = 0

    @classmethod
    def id(cls) -> int:
        return int(pydash.unique_id())
"""


class TestCreateModelCode(DBTest):
    @pytest.fixture(scope="function")
    def ch_table(self, ch_adapter: CHAdapter) -> Generator[CHTableIdentifier, Any, None]:
        table = "test_table"
        quoted_table = CHTableIdentifier(table=table).to_string()
        statement = f"""
        create or replace table {quoted_table}
        (
            id UInt64,
            created_at DateTime default now(),
            updated_at Nullable(DateTime64(9)),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        engine = MergeTree
        order by id
        """

        ch_adapter.create_table(table, statement)

        yield ch_adapter.get_table(table)

        ch_adapter.drop_table(table)

    @pytest.fixture(scope="function")
    def dbt_source(self, ch_table: Table) -> Generator[DbtSource, Any, None]:
        source = {
            "name": ch_table.name,
            "resource_type": "source",
            "package_name": "test",
            "original_file_path": "models/sources.yml",
            "unique_id": f"source.test.{settings.test_clickhouse.database}.{ch_table.name}",
            "source_name": settings.test_clickhouse.database,
            "tags": [],
            "config": {"enabled": True},
            "original_config": {
                "name": ch_table.name,
                "meta": {
                    "python_class": "TestTable",
                },
                "columns": [
                    {"name": "id", "data_type": "UInt64"},
                    {"name": "created_at", "data_type": "DateTime"},
                    {"name": "updated_at", "data_type": "Nullable(DateTime64(9))"},
                    {"name": "_peerdb_synced_at", "data_type": "DateTime64(9)"},
                    {"name": "_peerdb_is_deleted", "data_type": "Int8"},
                    {"name": "_peerdb_version", "data_type": "Int64"},
                ],
            },
        }
        yield DbtSource(**source)

    def test_ok(self, ch_adapter: CHAdapter, ch_table: Table, dbt_source: DbtSource):
        result = create_model_code(ch_adapter.settings, ch_adapter.settings.database, dbt_source)

        assert result["model_code"].strip() == model_code.strip()
        assert result["factory_code"].strip() == factory_code.strip()
