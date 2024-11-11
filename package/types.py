from pathlib import Path
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import List, Optional


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

    @property
    def processed_config(self) -> dict:
        from package.peerdb import process_config

        return process_config(self.config)


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


class DbtColumnMeta(BaseModel):
    sqlalchemy_type: str


class DbtColumn(BaseModel):
    name: str
    data_type: str
    meta: Optional[DbtColumnMeta] = None


class DbtSourceTableMeta(BaseModel):
    class_name: str


class DbtSourceTable(BaseModel):
    name: str
    meta: Optional[DbtSourceTableMeta] = None
    loaded_at_field: Optional[str] = None
    columns: Optional[List[DbtColumn]] = None


class DbtSource(BaseModel):
    name: str
    loader: Optional[str] = None
    tables: List[DbtSourceTable]


class DbtSourcesConfig(BaseModel):
    version: int
    sources: List[DbtSource]

    def get_sources_by_name(self, *names: str) -> List[DbtSource]:
        return [source for source in self.sources if source.name in names]
