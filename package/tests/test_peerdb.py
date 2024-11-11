from package.peerdb import process_config

import unittest
import yaml

process_config__empty_yaml = """
"""

process_config__peers_yaml = """
peers:
  source:
    type: 3
    postgres_config:
      host: host.docker.internal
      port: 5432
  destination:
    type: 8
    clickhouse_config:
      host: host.docker.internal
      port: 8123
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


class TestConfigProcess(unittest.TestCase):
    def test_empty(self):
        config = yaml.safe_load(process_config__empty_yaml) or {}
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
                        "host": "host.docker.internal",
                        "port": 5432,
                    },
                },
                "destination": {
                    "name": "destination",
                    "type": 8,
                    "clickhouse_config": {
                        "host": "host.docker.internal",
                        "port": 8123,
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
