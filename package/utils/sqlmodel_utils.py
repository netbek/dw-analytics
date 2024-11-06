from clickhouse_sqlalchemy import types
from package.database import get_create_table_statement, get_table_schema
from package.types import DbtSourceTable
from package.utils.python_utils import is_python_keyword
from sqlalchemy import Column
from typing import Any

import os
import pydash
import re
import uuid

CLICKHOUSE_TYPES = [
    "AggregateFunction",
    "Array",
    "Boolean",
    "Date",
    "Date32",
    "DateTime",
    "DateTime64",
    "Decimal",
    "Enum",
    "Enum16",
    "Enum8",
    "Float",
    "Float32",
    "Float64",
    "IPv4",
    "IPv6",
    "Int",
    "Int128",
    "Int16",
    "Int256",
    "Int32",
    "Int64",
    "Int8",
    "LowCardinality",
    "Map",
    "Nested",
    "Nullable",
    "SimpleAggregateFunction",
    "String",
    "Tuple",
    "UInt128",
    "UInt16",
    "UInt256",
    "UInt32",
    "UInt64",
    "UInt8",
    "UUID",
]

SQLALCHEMY_TO_CLICKHOUSE_TYPES = {
    "Bool": "Boolean",
}

INDENT = pydash.repeat(" ", 4)

FIELD_KWARGS = {
    "_peerdb_is_deleted": {"ge": 0, "le": 1},
    "_peerdb_version": {"ge": 0},
}


def parse_create_table_statement(statement: str) -> dict:
    pattern = (
        r"(PRIMARY\s+KEY|ORDER\s+BY)\s*\(([\w\s,]+)\)|(PRIMARY\s+KEY|ORDER\s+BY)\s+(\w+)|"
        r"ENGINE\s*=\s*([\w]+)\s*\(?([\w\s,]*)\)?"
    )

    results = {
        "engine": None,
        "version": None,
        "primary_key": [],
        "order_by": [],
    }

    # Find all matches for PRIMARY KEY, ORDER BY, and ENGINE
    matches = re.finditer(pattern, statement, re.IGNORECASE)

    for match in matches:
        # Check for PRIMARY KEY or ORDER BY
        key_type = match.group(1) or match.group(3)
        columns = match.group(2) or match.group(4)

        if key_type:
            key_type = pydash.snake_case(key_type)

            if columns:
                results[key_type].extend([col.strip() for col in columns.split(",")])

        # Check for ENGINE
        engine_name = match.group(5)
        engine_params = match.group(6)

        if engine_name:
            results["engine"] = engine_name

            if engine_params:
                results["version"] = engine_params.strip()

    return results


def to_field_name(column_name: str) -> str:
    return column_name.lstrip("_")


def get_pydantic_type(column: Column) -> str:
    python_type = get_python_type(column)

    if python_type is None:
        pydantic_type = "None"
    elif python_type.__module__ in ["datetime"]:
        pydantic_type = f"{python_type.__module__}.{python_type.__qualname__}"
    else:
        pydantic_type = python_type.__qualname__

    if column.nullable:
        pydantic_type = f"{pydantic_type} | None"

    return pydantic_type


def get_python_type(column: Column) -> Any:
    try:
        nested_type = getattr(column.type, "nested_type")
    except AttributeError:
        nested_type = None

    if nested_type:
        type_ = nested_type
    else:
        type_ = column.type

    if isinstance(column.type, types.UUID):
        python_type = uuid.UUID
    else:
        try:
            python_type = getattr(type_, "python_type")
        except NotImplementedError:
            python_type = None

    return python_type


def get_class_import_string(class_) -> str | None:
    if class_.__module__ == "builtins":
        return None
    elif class_.__module__ in ["datetime"]:
        return "import datetime"
    else:
        return f"from {class_.__module__} import {class_.__name__}"


class ASTNode:
    pass


class ArgumentNode(ASTNode):
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return self.value


class SimpleTypeNode(ASTNode):
    def __init__(self, type_name: str):
        self.type_name = type_name

    def __repr__(self):
        return f"types.{self.type_name}"


class NestedTypeNode(ASTNode):
    def __init__(self, modifier: str, inner: ASTNode):
        self.modifier = modifier
        self.inner = inner

    def __repr__(self):
        return f"types.{self.modifier}({self.inner})"


def get_sqlalchemy_type(column: Column) -> str:
    def parse_inner(string: str) -> ASTNode | None:
        if "(" in string and ")" in string:
            modifier, inner = string.split("(", 1)
            inner = inner.rsplit(")", 1)[0]  # Remove the outermost parentheses
            return NestedTypeNode(modifier.strip(), parse_inner(inner.strip()))
        else:
            string = string.strip()

            if string in SQLALCHEMY_TO_CLICKHOUSE_TYPES:
                string = SQLALCHEMY_TO_CLICKHOUSE_TYPES[string]

            if string in CLICKHOUSE_TYPES:
                return SimpleTypeNode(string)
            else:
                return ArgumentNode(string)

    return str(parse_inner(str(column.type)))


def serialize_dict(data: dict) -> str:
    return ", ".join(f"{key}={value}" for key, value in data.items())


def create_class_filename(class_name: str) -> str:
    filename = pydash.snake_case(class_name)

    # Fix keywords
    if is_python_keyword(filename):
        filename = filename + "_"

    return filename


def create_factory_name(model_name: str) -> str:
    return f"{model_name}Factory"


def create_model_code(dsn: str, database: str, table: DbtSourceTable) -> str:
    """Create the code of a SQLModel class from a table schema."""
    table_name = table.name
    model_name = table.meta.class_name
    schema = get_table_schema(dsn, database, table_name)
    statement = get_create_table_statement(dsn, database, table_name)
    parsed_statement = parse_create_table_statement(statement)
    table_kwargs = {"schema": database}
    engine = parsed_statement["engine"]
    engine_kwargs = {}

    if parsed_statement["version"]:
        engine_kwargs["version"] = f"'{parsed_statement["version"]}'"

    if parsed_statement["order_by"]:
        engine_kwargs["order_by"] = tuple(parsed_statement["order_by"])

    if parsed_statement["primary_key"]:
        engine_kwargs["primary_key"] = tuple(parsed_statement["primary_key"])

    imports = [
        "from clickhouse_sqlalchemy import engines",
        "from package.sqlalchemy.clickhouse import types",
        "from sqlmodel import Column, Field, SQLModel",
    ]

    columns = []

    for column in schema.columns:
        dbt_column = pydash.find(table.columns, lambda x: x.name == column.name)
        field_name = to_field_name(column.name)
        python_type = get_python_type(column)
        pydantic_type = get_pydantic_type(column)

        if dbt_column and dbt_column.meta and dbt_column.meta.sqlalchemy_type:
            sqlalchemy_type = dbt_column.meta.sqlalchemy_type
        else:
            sqlalchemy_type = get_sqlalchemy_type(column)

        sqlalchemy_column_kwargs = {
            "name": f"'{column.name}'",
            "type_": sqlalchemy_type,
        }

        if column.primary_key:
            sqlalchemy_column_kwargs["primary_key"] = True

        if not column.primary_key:
            sqlalchemy_column_kwargs["nullable"] = column.nullable

        field_kwargs = {
            "sa_column": f"Column({serialize_dict(sqlalchemy_column_kwargs)})",
        }

        if column.name in FIELD_KWARGS:
            field_kwargs.update(FIELD_KWARGS[column.name])

        column_def = f"{field_name}: {pydantic_type} = Field({serialize_dict(field_kwargs)})"
        columns.append(column_def)

        class_import = get_class_import_string(python_type)
        if class_import:
            imports.append(class_import)

    # Create model code
    lines = []

    # Add statement for reference
    lines.append('"""\nCreated from:\n\n' + statement + '\n"""')
    lines.append("")

    # Add imports
    imports = sorted(list(set(imports)))
    for class_import in imports:
        lines.append(class_import)

    # Add table class
    lines.append("")
    lines.append("")
    lines.append(f"class {model_name}(SQLModel, table=True):")
    lines.append(INDENT + f"__tablename__ = '{table_name}'")
    lines.append(
        INDENT
        + f"__table_args__ = (engines.{engine}({serialize_dict(engine_kwargs)}), {table_kwargs},)"
    )
    lines.append("")

    # Add columns
    for column in columns:
        lines.append(INDENT + column)

    return "\n".join(lines) + "\n"


def create_model_file(dsn: str, database: str, table: DbtSourceTable, directory: str) -> None:
    model_name = table.meta.class_name
    filename = create_class_filename(model_name)
    file_path = os.path.join(directory, f"{filename}.py")
    code = create_model_code(dsn, database, table)

    with open(file_path, "wt") as fp:
        fp.write(code)


def create_factory_code(table: DbtSourceTable, random_seed: int = 0) -> str:
    """Create the code of a Pydantic model factory."""
    model_name = table.meta.class_name
    model_filename = create_class_filename(model_name)
    factory_name = create_factory_name(model_name)

    imports = [
        f"from .{model_filename} import {model_name}",
        "from package.polyfactory.factories.sqlmodel_factory import SQLModelFactory",
    ]

    lines = []

    # Add imports
    imports = sorted(list(set(imports)))
    for import_ in imports:
        lines.append(import_)

    # Add factory class
    lines.append("")
    lines.append("")
    lines.append(f"class {factory_name}(SQLModelFactory[{model_name}]):")
    lines.append(INDENT + f"__random_seed__ = {random_seed}")

    return "\n".join(lines) + "\n"


def create_factory_file(table: DbtSourceTable, directory: str) -> None:
    model_name = table.meta.class_name
    factory_name = create_factory_name(model_name)
    filename = create_class_filename(factory_name)
    file_path = os.path.join(directory, f"{filename}.py")
    code = create_factory_code(table)

    with open(file_path, "wt") as fp:
        fp.write(code)


def create_init_file(tables: list[DbtSourceTable], directory: str) -> None:
    file_path = os.path.join(directory, "__init__.py")
    all = []
    imports = []

    for table in tables:
        class_name = table.meta.class_name

        model_filename = create_class_filename(class_name)
        all.append(class_name)
        imports.append(f"from .{model_filename} import {class_name}")

        factory_name = create_factory_name(class_name)
        factory_filename = create_class_filename(factory_name)
        all.append(factory_name)
        imports.append(f"from .{factory_filename} import {factory_name}")

    lines = [f"__all__ = {all}", "", "\n".join(imports), ""]
    code = "\n".join(lines)

    with open(file_path, "wt") as fp:
        fp.write(code)
