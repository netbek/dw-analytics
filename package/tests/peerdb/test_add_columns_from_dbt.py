from package.config.settings import get_settings
from package.peerdb import add_columns_from_dbt
from package.tests.fixtures import DBTest

import yaml

settings = get_settings()

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
    - name: timestamp
      data_type: DateTime64(6)
    - name: _peerdb_is_deleted
      data_type: Int8
    - name: _peerdb_synced_at
      data_type: DateTime64(9)
    - name: _peerdb_version
      data_type: Int64
"""


class TestAddColumnsFromDbt(DBTest):
    def test_empty_config(self):
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
                            "include": [
                                "id",
                                "username",
                                "_peerdb_is_deleted",
                                "_peerdb_synced_at",
                                "_peerdb_version",
                            ],
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
                            "include": [
                                "id",
                                "longitude",
                                "latitude",
                                "_peerdb_is_deleted",
                                "_peerdb_synced_at",
                                "_peerdb_version",
                            ],
                        },
                        {
                            "source_table_identifier": "public.table_3",
                            "destination_table_identifier": "table_3",
                            "include": [
                                "id",
                                "timestamp",
                                "_peerdb_is_deleted",
                                "_peerdb_synced_at",
                                "_peerdb_version",
                            ],
                        },
                    ],
                },
            },
        }

        assert actual == expected
