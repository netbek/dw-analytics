from pydantic import BaseModel, Field
from typing import List, Optional


class CHSettings(BaseModel):
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: Optional[bool] = False
    driver: Optional[str] = None

    def to_url(self) -> str:
        from package.database import CHAdapter

        return CHAdapter.create_url(**self.model_dump())


class CHIdentifier:
    @classmethod
    def quote(cls, identifier: str) -> str:
        return f"`{identifier}`"

    @classmethod
    def unquote(cls, identifier: str) -> str:
        return identifier.strip("`")


class CHTableIdentifier(CHIdentifier, BaseModel):
    database: Optional[str] = None
    table: str

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


class PGSettings(BaseModel):
    driver: Optional[str] = None
    host: str
    port: str
    username: str
    password: str
    database: str
    schema_: str = Field(serialization_alias="schema")

    def to_url(self) -> str:
        from package.database import PGAdapter

        return PGAdapter.create_url(**self.model_dump(by_alias=True))


class PGIdentifier:
    @classmethod
    def quote(cls, identifier: str) -> str:
        return f'"{identifier}"'

    @classmethod
    def unquote(cls, identifier: str) -> str:
        return identifier.strip('"')


class PGTableIdentifier(PGIdentifier, BaseModel):
    database: Optional[str] = None
    schema_: Optional[str] = None
    table: str

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
