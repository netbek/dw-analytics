from typing import TypedDict


class DbtSourceTableMeta(TypedDict):
    class_name: str


class DbtSourceTable(TypedDict):
    name: str
    meta: DbtSourceTableMeta
