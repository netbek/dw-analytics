from enum import StrEnum
from pydantic import BaseModel
from typing import Optional


class Type(StrEnum):
    CLICKHOUSE = "clickhouse"
    POSTGRESQL = "postgresql"


class CHSettings(BaseModel):
    type: Type = Type.CLICKHOUSE
    driver: Optional[str] = None
    host: str
    port: str
    username: str
    password: str
    database: str


class PGSettings(BaseModel):
    type: Type = Type.POSTGRESQL
    driver: Optional[str] = None
    host: str
    port: str
    username: str
    password: str
    database: str
    schema_: str
