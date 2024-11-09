from package.config.constants import HOME_DIR
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import os


class TestCHSettings(BaseSettings):
    __test__ = False  # Exclude from Pytest collection
    model_config = SettingsConfigDict(
        env_file=os.path.join(HOME_DIR, ".env_files/database.env"), extra="ignore"
    )

    host: str = Field(validation_alias="test_clickhouse_host", serialization_alias="host")
    port: int = Field(validation_alias="test_clickhouse_port", serialization_alias="port")
    username: str = Field(
        validation_alias="test_clickhouse_username", serialization_alias="username"
    )
    password: str = Field(
        validation_alias="test_clickhouse_password", serialization_alias="password"
    )
    database: str = Field(
        validation_alias="test_clickhouse_database", serialization_alias="database"
    )
    secure: bool = Field(validation_alias="test_clickhouse_secure", serialization_alias="secure")
    driver: str = Field(validation_alias="test_clickhouse_driver", serialization_alias="driver")


class TestPGSettings(BaseSettings):
    __test__ = False  # Exclude from Pytest collection
    model_config = SettingsConfigDict(
        env_file=os.path.join(HOME_DIR, ".env_files/database.env"), extra="ignore"
    )

    host: str = Field(validation_alias="test_postgres_host", serialization_alias="host")
    port: int = Field(validation_alias="test_postgres_port", serialization_alias="port")
    username: str = Field(validation_alias="test_postgres_username", serialization_alias="username")
    password: str = Field(validation_alias="test_postgres_password", serialization_alias="password")
    database: str = Field(validation_alias="test_postgres_database", serialization_alias="database")
    schema_: str = Field(validation_alias="test_postgres_schema", serialization_alias="schema")
