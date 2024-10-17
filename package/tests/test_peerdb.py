from package.peerdb import PeerDBConfig

import unittest


class TestConfigProcess:
    def test_peers(self):
        actual = PeerDBConfig.process(
            {
                "peers": {
                    "source": {
                        "type": 3,
                        "postgres_config": {
                            "host": "host.docker.internal",
                            "port": 5432,
                        },
                    },
                    "destination": {
                        "type": 8,
                        "clickhouse_config": {
                            "host": "host.docker.internal",
                            "port": 8123,
                        },
                    },
                },
            }
        )

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

        unittest.TestCase().assertDictEqual(actual, expected)

    def test_mirrors(self):
        actual = PeerDBConfig.process(
            {
                "mirrors": {
                    "+do_initial_snapshot": False,
                    "+resync": False,
                    "cdc_large": {
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
                        "resync": True,
                    },
                    "cdc_small": {
                        "source_name": "source",
                        "destination_name": "destination",
                        "table_mappings": [
                            {
                                "source_table_identifier": "public.table_3",
                                "destination_table_identifier": "table_3",
                            }
                        ],
                    },
                },
            }
        )

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

        unittest.TestCase().assertDictEqual(actual, expected)

    def test_publications(self):
        actual = PeerDBConfig.process(
            {
                "peers": {},
                "mirrors": {
                    "+do_initial_snapshot": False,
                    "+resync": False,
                    "cdc_large": {
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
                        "resync": True,
                    },
                    "cdc_small": {
                        "publication_name": "publication_2",
                        "source_name": "source",
                        "destination_name": "destination",
                        "table_mappings": [
                            {
                                "source_table_identifier": "public.table_3",
                                "destination_table_identifier": "table_3",
                            }
                        ],
                    },
                },
                "publications": {
                    "publication_1": [
                        "public.table_1",
                        "public.table_2",
                    ],
                    "publication_2": [
                        "public.table_3",
                    ],
                },
            }
        )

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
            "publication_schemas": ["public"],
            "publications": {
                "publication_1": {
                    "name": "publication_1",
                    "table_identifiers": [
                        "public.table_1",
                        "public.table_2",
                    ],
                },
                "publication_2": {
                    "name": "publication_2",
                    "table_identifiers": [
                        "public.table_3",
                    ],
                },
            },
            "users": {},
        }

        unittest.TestCase().assertDictEqual(actual, expected)
