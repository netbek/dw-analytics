from package.utils.sqlmodel_utils import parse_create_table_statement


class TestParseCreateTableStatement:
    def test_no_properties(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": None,
            "version": None,
            "is_deleted": None,
            "primary_key": [],
            "order_by": [],
            "settings": {},
        }

        assert actual == expected

    def test_engine_version(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree(_peerdb_version)
        PRIMARY KEY id
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": "_peerdb_version",
            "is_deleted": None,
            "primary_key": ["id"],
            "order_by": [],
            "settings": {},
        }

        assert actual == expected

    def test_engine_version_and_is_deleted(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree(_peerdb_version, _peerdb_is_deleted)
        PRIMARY KEY id
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": "_peerdb_version",
            "is_deleted": "_peerdb_is_deleted",
            "primary_key": ["id"],
            "order_by": [],
            "settings": {},
        }

        assert actual == expected

    def test_primary_key_string(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree
        PRIMARY KEY id
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": None,
            "is_deleted": None,
            "primary_key": ["id"],
            "order_by": [],
            "settings": {},
        }

        assert actual == expected

    def test_primary_key_tuple(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree
        PRIMARY KEY (id, country)
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": None,
            "is_deleted": None,
            "primary_key": ["id", "country"],
            "order_by": [],
            "settings": {},
        }

        assert actual == expected

    def test_order_by_string(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree
        ORDER BY id
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": None,
            "is_deleted": None,
            "primary_key": [],
            "order_by": ["id"],
            "settings": {},
        }

        assert actual == expected

    def test_order_by_tuple(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            country String,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (id, country)
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": None,
            "is_deleted": None,
            "primary_key": [],
            "order_by": ["id", "country"],
            "settings": {},
        }

        assert actual == expected

    def test_settings(self):
        statement = """
        CREATE TABLE test_table
        (
            id UInt64,
            updated_at DateTime default now(),
            _peerdb_synced_at DateTime64(9) DEFAULT now64(),
            _peerdb_is_deleted Int8,
            _peerdb_version Int64
        )
        ENGINE = ReplacingMergeTree
        SETTINGS allow_nullable_key = 1, index_granularity = 8192
        """
        actual = parse_create_table_statement(statement)
        del actual["columns"]  # TODO Remove
        expected = {
            "engine": "ReplacingMergeTree",
            "version": None,
            "is_deleted": None,
            "primary_key": [],
            "order_by": [],
            "settings": {"allow_nullable_key": "1", "index_granularity": "8192"},
        }

        assert actual == expected
