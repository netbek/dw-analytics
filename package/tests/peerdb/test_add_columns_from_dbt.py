from package.config.settings import get_settings
from package.database import PGAdapter
from package.peerdb import add_columns_from_dbt
from package.tests.fixtures import DBTest
from sqlmodel import Table
from typing import Any, Generator, List

import pytest
import yaml

settings = get_settings()

table_defs = [
    (
        "table_1",
        """
        create table table_1 (
            id bigint,
            username text,
            password text,
            age smallint,
            modified_at timestamp(6)
        );
        """,
    ),
    (
        "table_2",
        """
        create table table_2 (
            id bigint,
            longitude double precision,
            latitude double precision,
            is_secret boolean,
            modified_at timestamp(6)
        );
        """,
    ),
    (
        "table_3",
        """
        create table table_3 (
            id bigint,
            ts timestamp(6),
            modified_at timestamp(6)
        );
        """,
    ),
]

peerdb_yaml = f"""
peers:
  source:
    type: 3
    postgres_config:
      host: {settings.test_pg.host}
      port: {settings.test_pg.port}
      user: {settings.test_pg.username}
      password: {settings.test_pg.password}
      database: {settings.test_pg.database}

  destination:
    type: 8
    clickhouse_config:
      host: {settings.test_ch.host}
      port: {settings.test_ch.port}
      user: {settings.test_ch.username}
      password: {settings.test_ch.password}
      database: {settings.test_ch.database}

mirrors:
  cdc_small:
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.table_1
      destination_table_identifier: table_1

  cdc_large:
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.table_2
      destination_table_identifier: table_2
    - source_table_identifier: public.table_3
      destination_table_identifier: table_3
"""

dbt_sources_yaml = f"""
version: 2

sources:
- name: {settings.test_ch.database}
  loader: peerdb
  tables:
  - name: table_1
    columns:
    - name: id
      data_type: Int64
    - name: username
      data_type: String
    - name: _peerdb_is_deleted
      data_type: Int8
    - name: _peerdb_synced_at
      data_type: DateTime64(9)
    - name: _peerdb_version
      data_type: Int64

  - name: table_2
    columns:
    - name: id
      data_type: Int64
    - name: longitude
      data_type: Float64
    - name: latitude
      data_type: Float64
    - name: _peerdb_is_deleted
      data_type: Int8
    - name: _peerdb_synced_at
      data_type: DateTime64(9)
    - name: _peerdb_version
      data_type: Int64

  - name: table_3
    columns:
    - name: id
      data_type: Int64
    - name: ts
      data_type: DateTime64(6)
    - name: _peerdb_is_deleted
      data_type: Int8
    - name: _peerdb_synced_at
      data_type: DateTime64(9)
    - name: _peerdb_version
      data_type: Int64
"""


class TestPeerDBMissingPeers(DBTest):
    def test_func(self):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        del peerdb_config["peers"]
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)

        with pytest.raises(Exception) as exc:
            add_columns_from_dbt(peerdb_config, dbt_sources_config)

        assert str(exc.value) == "Peers not found in PeerDB config"


class TestPeerDBMissingSourcePeer(DBTest):
    def test_func(self):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        del peerdb_config["peers"]["source"]
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)

        with pytest.raises(Exception) as exc:
            add_columns_from_dbt(peerdb_config, dbt_sources_config)

        assert str(exc.value) == "Peer 'source' not found in PeerDB config"


class TestPeerDBMissingDestinationPeer(DBTest):
    def test_func(self):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        del peerdb_config["peers"]["destination"]
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)

        with pytest.raises(Exception) as exc:
            add_columns_from_dbt(peerdb_config, dbt_sources_config)

        assert str(exc.value) == "Peer 'destination' not found in PeerDB config"


class TestDbtMissingDatabase(DBTest):
    @pytest.fixture(scope="function")
    def pg_tables(self, pg_adapter: PGAdapter) -> Generator[List[Table], Any, None]:
        for table_def in table_defs:
            pg_adapter.create_table(*table_def)

        table_names = [table_def[0] for table_def in table_defs]
        tables = [table for table in pg_adapter.list_tables() if table.name in table_names]

        yield tables

        for table_name in table_names:
            pg_adapter.drop_table(table_name)

    def test_func(self, pg_tables: List[Table]):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)
        dbt_sources_config["sources"][0]["name"] = "foo"

        with pytest.raises(Exception) as exc:
            add_columns_from_dbt(peerdb_config, dbt_sources_config)

        assert str(exc.value) == "Destination 'default' not found in dbt config"


class TestSourceMissingTable(DBTest):
    @pytest.fixture(scope="function")
    def pg_tables(self, pg_adapter: PGAdapter) -> Generator[List[Table], Any, None]:
        for table_def in table_defs[:1]:
            pg_adapter.create_table(*table_def)

        table_names = [table_def[0] for table_def in table_defs[:1]]
        tables = [table for table in pg_adapter.list_tables() if table.name in table_names]

        yield tables

        for table_name in table_names:
            pg_adapter.drop_table(table_name)

    def test_func(self, pg_tables: List[Table]):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)

        with pytest.raises(Exception) as exc:
            add_columns_from_dbt(peerdb_config, dbt_sources_config)

        assert str(exc.value) == "Table 'public.table_2' not found in database of peer 'source'"


class TestDbtMissingTable(DBTest):
    @pytest.fixture(scope="function")
    def pg_tables(self, pg_adapter: PGAdapter) -> Generator[List[Table], Any, None]:
        for table_def in table_defs:
            pg_adapter.create_table(*table_def)

        table_names = [table_def[0] for table_def in table_defs]
        tables = [table for table in pg_adapter.list_tables() if table.name in table_names]

        yield tables

        for table_name in table_names:
            pg_adapter.drop_table(table_name)

    def test_func(self, pg_tables: List[Table]):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)
        dbt_sources_config["sources"][0]["tables"] = dbt_sources_config["sources"][0]["tables"][:1]

        with pytest.raises(Exception) as exc:
            add_columns_from_dbt(peerdb_config, dbt_sources_config)

        assert str(exc.value) == "Destination table 'table_2' not found in dbt config"


class TestOK(DBTest):
    @pytest.fixture(scope="function")
    def pg_tables(self, pg_adapter: PGAdapter) -> Generator[List[Table], Any, None]:
        for table_def in table_defs:
            pg_adapter.create_table(*table_def)

        table_names = [table_def[0] for table_def in table_defs]
        tables = [table for table in pg_adapter.list_tables() if table.name in table_names]

        yield tables

        for table_name in table_names:
            pg_adapter.drop_table(table_name)

    def test_func(self, pg_tables: List[Table]):
        peerdb_config = yaml.safe_load(peerdb_yaml)
        dbt_sources_config = yaml.safe_load(dbt_sources_yaml)
        actual = add_columns_from_dbt(peerdb_config, dbt_sources_config)
        expected = {
            "peers": {
                "source": {
                    "type": 3,
                    "postgres_config": {
                        "host": settings.test_pg.host,
                        "port": settings.test_pg.port,
                        "user": settings.test_pg.username,
                        "password": settings.test_pg.password,
                        "database": settings.test_pg.database,
                    },
                },
                "destination": {
                    "type": 8,
                    "clickhouse_config": {
                        "host": settings.test_ch.host,
                        "port": settings.test_ch.port,
                        "user": settings.test_ch.username,
                        "password": settings.test_ch.password,
                        "database": settings.test_ch.database,
                    },
                },
            },
            "mirrors": {
                "cdc_small": {
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_1",
                            "destination_table_identifier": "table_1",
                            "exclude": ["password", "age", "modified_at"],
                        }
                    ],
                },
                "cdc_large": {
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_2",
                            "destination_table_identifier": "table_2",
                            "exclude": ["is_secret", "modified_at"],
                        },
                        {
                            "source_table_identifier": "public.table_3",
                            "destination_table_identifier": "table_3",
                            "exclude": ["modified_at"],
                        },
                    ],
                },
            },
        }

        assert actual == expected
