from pydantic import BaseModel
from typing import List, Optional


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
    columns: Optional[List[DbtColumn]] = None
    meta: Optional[DbtSourceTableMeta] = None


class DbtSource(BaseModel):
    name: str
    tables: List[DbtSourceTable]


class DbtSourcesConfig(BaseModel):
    version: int
    sources: List[DbtSource]

    def get_sources_by_name(self, *names: str) -> List[DbtSource]:
        return [source for source in self.sources if source.name in names]
