from typing import Optional, TypedDict


class DbtSourceTableMeta(TypedDict):
    class_name: str


class DbtSourceTable(TypedDict):
    name: str
    meta: Optional[DbtSourceTableMeta]
