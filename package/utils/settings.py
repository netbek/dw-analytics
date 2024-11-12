from package.config.constants import HOME_DIR
from package.types import (
    CHSettings,
    DbtSettings,
    NotebookSettings,
    PeerDBSettings,
    PGSettings,
    PrefectSettings,
)
from package.utils.yaml_utils import safe_load_file
from pathlib import Path
from pydantic import Field
from pydantic_settings import SettingsConfigDict

import os


def create_pg_settings(env_prefix: str) -> PGSettings:
    class Settings(PGSettings):
        model_config = SettingsConfigDict(
            env_file=os.path.join(HOME_DIR, ".env_files/database.env"), extra="ignore"
        )

        host: str = Field(
            validation_alias=f"{env_prefix}postgres_host",
            serialization_alias="host",
        )
        port: int = Field(
            validation_alias=f"{env_prefix}postgres_port",
            serialization_alias="port",
        )
        username: str = Field(
            validation_alias=f"{env_prefix}postgres_username",
            serialization_alias="username",
        )
        password: str = Field(
            validation_alias=f"{env_prefix}postgres_password",
            serialization_alias="password",
        )
        database: str = Field(
            validation_alias=f"{env_prefix}postgres_database",
            serialization_alias="database",
        )
        schema_: str = Field(
            validation_alias=f"{env_prefix}postgres_schema",
            serialization_alias="schema",
        )

    return Settings


def create_ch_settings(env_prefix: str) -> CHSettings:
    class Settings(CHSettings):
        model_config = SettingsConfigDict(
            env_file=os.path.join(HOME_DIR, ".env_files/database.env"), extra="ignore"
        )

        host: str = Field(
            validation_alias=f"{env_prefix}clickhouse_host",
            serialization_alias="host",
        )
        port: int = Field(
            validation_alias=f"{env_prefix}clickhouse_port",
            serialization_alias="port",
        )
        username: str = Field(
            validation_alias=f"{env_prefix}clickhouse_username",
            serialization_alias="username",
        )
        password: str = Field(
            validation_alias=f"{env_prefix}clickhouse_password",
            serialization_alias="password",
        )
        database: str = Field(
            validation_alias=f"{env_prefix}clickhouse_database",
            serialization_alias="database",
        )
        secure: bool = Field(
            validation_alias=f"{env_prefix}clickhouse_secure",
            serialization_alias="secure",
        )
        driver: str = Field(
            validation_alias=f"{env_prefix}clickhouse_driver",
            serialization_alias="driver",
        )

    return Settings


def create_dbt_settings(directory: Path | str, config_path: Path | str) -> DbtSettings:
    class Settings(DbtSettings):
        directory: Path | str = Field(default_factory=lambda: directory)
        config: dict = Field(default_factory=lambda: safe_load_file(config_path))

    return Settings


def create_notebook_settings(directory: Path | str) -> NotebookSettings:
    class Settings(NotebookSettings):
        directory: Path | str = Field(default_factory=lambda: directory)

    return Settings


def create_peerdb_settings(config_path: Path | str) -> PeerDBSettings:
    class Settings(PeerDBSettings):
        model_config = SettingsConfigDict(
            env_file=os.path.join(HOME_DIR, ".env_files/database.env"), extra="ignore"
        )

        api_url: str = Field(validation_alias="peerdb_api_url")
        config: dict = Field(default_factory=lambda: safe_load_file(config_path))

    return Settings


def create_prefect_settings(config_path: Path | str) -> PrefectSettings:
    class Settings(PrefectSettings):
        config: dict = Field(default_factory=lambda: safe_load_file(config_path))

    return Settings
