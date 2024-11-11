from package.config.settings import get_settings
from package.database import PGAdapter
from package.peerdb import process_config
from package.tests.fixtures import DBTest
from package.types import PGTableIdentifier
from sqlmodel import Table
from typing import Any, Generator, List

import pytest
import unittest
import yaml

settings = get_settings()

load_config__non_existent_table_yaml = f"""
peers:
  source:
    type: 3
    postgres_config:
      host: {settings.test_pg.host}
      port: {settings.test_pg.port}
      user: {settings.test_pg.username}
      password: {settings.test_pg.password}
      database: {settings.test_pg.database}

mirrors:
  cdc_small:
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.non_existent_table
      destination_table_identifier: non_existent_table
"""

load_config__existent_tables_yaml = f"""
peers:
  source:
    type: 3
    postgres_config:
      host: {settings.test_pg.host}
      port: {settings.test_pg.port}
      user: {settings.test_pg.username}
      password: {settings.test_pg.password}
      database: {settings.test_pg.database}

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
      include:
      - id
      - username
    - source_table_identifier: public.table_3
      destination_table_identifier: table_3
      exclude:
      - password
      - updated_at
"""

process_config__empty_config_yaml = """
"""

process_config__peers_yaml = f"""
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
      disable_tls: true
"""

process_config__mirrors_yaml = """
mirrors:
  +do_initial_snapshot: false
  +resync: false
  cdc_large:
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.table_1
      destination_table_identifier: table_1
    - source_table_identifier: public.table_2
      destination_table_identifier: table_2
    resync: true
  cdc_small:
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.table_3
      destination_table_identifier: table_3
"""

process_config__publications_yaml = """
mirrors:
  +do_initial_snapshot: false
  +resync: false
  cdc_large:
    publication_name: publication_1
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.table_1
      destination_table_identifier: table_1
    - source_table_identifier: public.table_2
      destination_table_identifier: table_2
    resync: true
  cdc_small:
    publication_name: publication_2
    source_name: source
    destination_name: destination
    table_mappings:
    - source_table_identifier: public.table_3
      destination_table_identifier: table_3
publications:
  publication_1:
  - private.table_1
  - private.table_2
  publication_2:
  - private.table_3
"""


class TestLoadConfig(DBTest):
    @pytest.fixture(scope="function")
    def pg_tables(self, pg_adapter: PGAdapter) -> Generator[List[Table], Any, None]:
        tables = ["table_1", "table_2", "table_3"]

        for table in tables:
            quoted_table = PGTableIdentifier(table=table).to_string()
            statement = f"""
            create table if not exists {quoted_table} (
                id bigint,
                username varchar,
                password varchar,
                updated_at timestamp default now()
            );
            """

            pg_adapter.create_table(table, statement)

        yield [table for table in pg_adapter.list_tables() if table.name in tables]

        for table in tables:
            pg_adapter.drop_table(table)

    def test_non_existent_table(self, pg_tables: List[Table]):
        """
        Test that an exception is raised if a mirror config has a source table that doesn't exist
        in the source database.
        """
        config = yaml.safe_load(load_config__non_existent_table_yaml) or {}

        with pytest.raises(Exception) as exc:
            process_config(config)

        assert str(exc.value) == "Source table 'public.non_existent_table' not found"

    def test_existent_tables(self, pg_tables: List[Table]):
        """Test that computed 'exclude' value is correct."""
        config = yaml.safe_load(load_config__existent_tables_yaml) or {}
        actual = process_config(config)
        expected = {
            "mirrors": {
                "cdc_small": {
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_1",
                            "destination_table_identifier": "table_1",
                            "exclude": [],
                        },
                    ],
                    "flow_job_name": "cdc_small",
                },
                "cdc_large": {
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_2",
                            "destination_table_identifier": "table_2",
                            "exclude": ["password", "updated_at"],
                        },
                        {
                            "source_table_identifier": "public.table_3",
                            "destination_table_identifier": "table_3",
                            "exclude": ["password", "updated_at"],
                        },
                    ],
                    "flow_job_name": "cdc_large",
                },
            },
        }

        assert actual["mirrors"] == expected["mirrors"]


class TestConfigProcess(unittest.TestCase):
    def test_empty_config(self):
        config = yaml.safe_load(process_config__empty_config_yaml) or {}
        actual = process_config(config)
        expected = {
            "mirrors": {},
            "peers": {},
            "publication_schemas": [],
            "publications": {},
            "users": {},
        }

        self.assertDictEqual(actual, expected)

    def test_peers(self):
        config = yaml.safe_load(process_config__peers_yaml) or {}
        actual = process_config(config)
        expected = {
            "mirrors": {},
            "peers": {
                "source": {
                    "name": "source",
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
                    "name": "destination",
                    "type": 8,
                    "clickhouse_config": {
                        "host": settings.test_ch.host,
                        "port": settings.test_ch.port,
                        "user": settings.test_ch.username,
                        "password": settings.test_ch.password,
                        "database": settings.test_ch.database,
                        "disable_tls": True,
                    },
                },
            },
            "publication_schemas": [],
            "publications": {},
            "users": {},
        }

        self.assertDictEqual(actual, expected)

    def test_mirrors(self):
        config = yaml.safe_load(process_config__mirrors_yaml) or {}
        actual = process_config(config)
        expected = {
            "mirrors": {
                "cdc_large": {
                    "flow_job_name": "cdc_large",
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_1",
                            "destination_table_identifier": "table_1",
                        },
                        {
                            "source_table_identifier": "public.table_2",
                            "destination_table_identifier": "table_2",
                        },
                    ],
                    "do_initial_snapshot": False,
                    "resync": True,
                },
                "cdc_small": {
                    "flow_job_name": "cdc_small",
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_3",
                            "destination_table_identifier": "table_3",
                        }
                    ],
                    "do_initial_snapshot": False,
                    "resync": False,
                },
            },
            "peers": {},
            "publication_schemas": ["public"],
            "publications": {},
            "users": {},
        }

        self.assertDictEqual(actual, expected)

    def test_publications(self):
        config = yaml.safe_load(process_config__publications_yaml) or {}
        actual = process_config(config)
        expected = {
            "mirrors": {
                "cdc_large": {
                    "flow_job_name": "cdc_large",
                    "publication_name": "publication_1",
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_1",
                            "destination_table_identifier": "table_1",
                        },
                        {
                            "source_table_identifier": "public.table_2",
                            "destination_table_identifier": "table_2",
                        },
                    ],
                    "do_initial_snapshot": False,
                    "resync": True,
                },
                "cdc_small": {
                    "flow_job_name": "cdc_small",
                    "publication_name": "publication_2",
                    "source_name": "source",
                    "destination_name": "destination",
                    "table_mappings": [
                        {
                            "source_table_identifier": "public.table_3",
                            "destination_table_identifier": "table_3",
                        }
                    ],
                    "do_initial_snapshot": False,
                    "resync": False,
                },
            },
            "peers": {},
            "publication_schemas": ["private", "public"],
            "publications": {
                "publication_1": {
                    "name": "publication_1",
                    "table_identifiers": [
                        "private.table_1",
                        "private.table_2",
                    ],
                },
                "publication_2": {
                    "name": "publication_2",
                    "table_identifiers": [
                        "private.table_3",
                    ],
                },
            },
            "users": {},
        }

        self.assertDictEqual(actual, expected)
