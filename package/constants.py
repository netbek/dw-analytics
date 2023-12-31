from enum import Enum
from typing import Optional

import os
import prefect


def build_database_connection_url(
    type: str,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    driver: Optional[str] = None,
):
    if driver:
        scheme = f"{type}+{driver}"
    else:
        scheme = type

    return f"{scheme}://{username}:{password}@{host}:{port}/{database}"


def get_database_connection_settings_for_active_profile():
    profile_name = prefect.context.get_settings_context().profile.name

    # TODO Remove after removing default profile from Prefect and adding dev profile
    if profile_name == "default":
        profile_name = "dev"

    profile_name_env = profile_name.upper()

    driver = os.getenv(f"CLICKHOUSE_{profile_name_env}_DRIVER")
    host = os.getenv(f"CLICKHOUSE_{profile_name_env}_HOST")
    port = os.getenv(f"CLICKHOUSE_{profile_name_env}_PORT")
    username = os.getenv(f"CLICKHOUSE_{profile_name_env}_USERNAME")
    password = os.getenv(f"CLICKHOUSE_{profile_name_env}_PASSWORD")
    database = os.getenv(f"CLICKHOUSE_{profile_name_env}_DATABASE")

    return {
        "driver": driver,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "database": database,
    }


# Filesystem
HOME_DIR = os.environ["HOME"]
PROJECTS_DIR = os.path.join(HOME_DIR, "projects")
TEMPLATE_PROJECT_DIR = os.path.join(HOME_DIR, "template_project")

# Database
CLICKHOUSE_URL = build_database_connection_url(
    type="clickhouse", **get_database_connection_settings_for_active_profile()
)

CLICKHOUSE_TEST_DATABASE = os.environ["CLICKHOUSE_TEST_DATABASE"]

# dbt
DBT_PROFILES_DIR = os.environ["DBT_PROFILES_DIR"]
DBT_PROFILES_FILE = os.path.join(DBT_PROFILES_DIR, "profiles.yml")

# Prefect
PREFECT_HOME = os.environ["PREFECT_HOME"]
PREFECT_PROFILES_PATH = os.path.join(PREFECT_HOME, "profiles.toml")
PREFECT_PROVISION_PATH = os.path.join(PREFECT_HOME, "provision.yml")

"""
Map from dbt-codegen to ClickHouse data types that are case-sensitive.
See https://clickhouse.com/docs/en/operations/system-tables/data_type_families
Query:
    select '"' || lower(name) || '": "' || name || '",'
    from system.data_type_families
    where not case_insensitive
    order by name;
"""
CODEGEN_TO_CLICKHOUSE_DATA_TYPES = {
    "aggregatefunction": "AggregateFunction",
    "array": "Array",
    "enum16": "Enum16",
    "enum8": "Enum8",
    "fixedstring": "FixedString",
    "float32": "Float32",
    "float64": "Float64",
    "ipv4": "IPv4",
    "ipv6": "IPv6",
    "int128": "Int128",
    "int16": "Int16",
    "int256": "Int256",
    "int32": "Int32",
    "int64": "Int64",
    "int8": "Int8",
    "intervalday": "IntervalDay",
    "intervalhour": "IntervalHour",
    "intervalmicrosecond": "IntervalMicrosecond",
    "intervalmillisecond": "IntervalMillisecond",
    "intervalminute": "IntervalMinute",
    "intervalmonth": "IntervalMonth",
    "intervalnanosecond": "IntervalNanosecond",
    "intervalquarter": "IntervalQuarter",
    "intervalsecond": "IntervalSecond",
    "intervalweek": "IntervalWeek",
    "intervalyear": "IntervalYear",
    "lowcardinality": "LowCardinality",
    "map": "Map",
    "multipolygon": "MultiPolygon",
    "nested": "Nested",
    "nothing": "Nothing",
    "nullable": "Nullable",
    "object": "Object",
    "point": "Point",
    "polygon": "Polygon",
    "ring": "Ring",
    "simpleaggregatefunction": "SimpleAggregateFunction",
    "string": "String",
    "tuple": "Tuple",
    "uint128": "UInt128",
    "uint16": "UInt16",
    "uint256": "UInt256",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "uint8": "UInt8",
    "uuid": "UUID",
}

# TODO Consolidate constants and enums
ACTION_CREATE = "create"
ACTION_DELETE = "delete"
ACTION_PAUSE = "pause"
ACTION_RESUME = "resume"


class DeploymentAction(str, Enum):
    delete = "delete"
    pause = "pause"
    resume = "resume"
