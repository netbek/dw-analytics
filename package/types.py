from enum import StrEnum
from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Dict, List, Optional


class CHSettings(BaseSettings):
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: Optional[bool] = Field(default=False)
    driver: Optional[str] = Field(default=None)

    @property
    def uri(self) -> str:
        from package.database import CHAdapter

        return CHAdapter.create_uri(**self.model_dump(by_alias=True))


class PGSettings(BaseSettings):
    host: str
    port: int
    username: str
    password: str
    database: str
    schema_: str = Field(serialization_alias="schema")

    @property
    def uri(self) -> str:
        from package.database import PGAdapter

        return PGAdapter.create_uri(**self.model_dump(by_alias=True))


class DbtSettings(BaseSettings):
    directory: Path | str
    config: dict


class PeerDBSettings(BaseSettings):
    config: dict


class PrefectSettings(BaseSettings):
    config: dict


class NotebookSettings(BaseSettings):
    directory: Path | str


class CHIdentifier:
    @classmethod
    def quote(cls, identifier: str) -> str:
        return f"`{identifier}`"

    @classmethod
    def unquote(cls, identifier: str) -> str:
        return identifier.strip("`")


class CHTableIdentifier(CHIdentifier, BaseModel):
    database: Optional[str] = Field(default=None, serialization_alias="database")
    table: str = Field(serialization_alias="table")

    @classmethod
    def from_string(cls, identifier: str) -> "CHTableIdentifier":
        parts = [cls.unquote(part) for part in identifier.split(".")]

        if len(parts) == 2:
            return cls(database=parts[0], table=parts[1])
        elif len(parts) == 1:
            return cls(table=parts[0])
        else:
            raise ValueError()

    def to_string(self) -> str:
        if self.database is not None:
            return f"{self.quote(self.database)}.{self.quote(self.table)}"
        else:
            return self.quote(self.table)


class PGIdentifier:
    @classmethod
    def quote(cls, identifier: str) -> str:
        return f'"{identifier}"'

    @classmethod
    def unquote(cls, identifier: str) -> str:
        return identifier.strip('"')


class PGTableIdentifier(PGIdentifier, BaseModel):
    database: Optional[str] = Field(default=None, serialization_alias="database")
    schema_: Optional[str] = Field(default=None, serialization_alias="schema")
    table: str = Field(serialization_alias="table")

    @classmethod
    def from_string(cls, identifier: str) -> "PGTableIdentifier":
        parts = [cls.unquote(part) for part in identifier.split(".")]

        if len(parts) == 3:
            return cls(database=parts[0], schema_=parts[1], table=parts[2])
        elif len(parts) == 2:
            return cls(schema_=parts[0], table=parts[1])
        elif len(parts) == 1:
            return cls(table=parts[0])
        else:
            raise ValueError()

    def to_string(self) -> str:
        if self.database is not None and self.schema_ is not None:
            return (
                f"{self.quote(self.database)}.{self.quote(self.schema_)}.{self.quote(self.table)}"
            )
        elif self.schema_ is not None:
            return f"{self.quote(self.schema_)}.{self.quote(self.table)}"
        else:
            return self.quote(self.table)


class DbtResourceType(StrEnum):
    MODEL = "model"
    SOURCE = "source"


class DbtColumnMeta(BaseModel):
    sqlalchemy_type: str


class DbtColumn(BaseModel):
    data_type: str
    meta: Optional[DbtColumnMeta] = None
    name: str


class DbtSourceTableMeta(BaseModel):
    class_name: str


class DbtSourceOriginalConfig(BaseModel):
    columns: Optional[List[DbtColumn]] = None
    loaded_at_field: Optional[str] = None
    meta: Optional[DbtSourceTableMeta] = None
    name: str


class DbtSourceConfig(BaseModel):
    enabled: bool


class DbtSource(BaseModel):
    config: DbtSourceConfig
    name: str
    original_config: Optional[DbtSourceOriginalConfig] = None
    original_file_path: str
    package_name: str
    resource_type: DbtResourceType
    source_name: str
    tags: List[str]
    unique_id: str


class DbtPersistDocs(BaseModel):
    columns: Optional[bool] = None


class DbtContract(BaseModel):
    alias_types: bool
    enforced: bool


class DbtModelConfig(BaseModel):
    access: str
    alias: Optional[str] = None
    batch_filter: Optional[str] = None
    batch_size: Optional[int] = None
    column_types: Dict[str, str]
    contract: DbtContract
    database: Optional[str] = None
    docs: Dict[str, str | bool]
    enabled: bool
    engine: Optional[str] = None
    full_refresh: Optional[bool]
    grants: Dict[str, List[str]]
    group: Optional[str] = None
    incremental_strategy: Optional[str] = None
    materialized: str
    meta: Dict[str, str | int | float | bool | None]
    on_configuration_change: str
    on_schema_change: str
    order_by: Optional[str] = None
    packages: List[str]
    persist_docs: DbtPersistDocs
    post_hook: Optional[List[str]] = None
    pre_hook: Optional[List[str]] = None
    quoting: Dict[str, bool]
    range_max: Optional[str] = None
    range_min: Optional[str] = None
    schema_: Optional[str] = Field(default=None, serialization_alias="schema")
    tags: List[str]
    unique_key: Optional[str] = None


class DbtDependsOn(BaseModel):
    macros: List[str]
    nodes: List[str]


class DbtModel(BaseModel):
    alias: str
    config: DbtModelConfig
    depends_on: DbtDependsOn
    name: str
    original_file_path: str
    package_name: str
    resource_type: str
    tags: List[str]
    unique_id: str
