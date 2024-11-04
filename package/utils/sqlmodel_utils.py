from clickhouse_sqlalchemy import types
from package.database import get_clickhouse_client
from package.utils.python_utils import is_python_keyword
from sqlmodel import create_engine, MetaData, Table

# import pydantic
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

PYTHON_TYPE = {
    # "_peerdb_synced_at": pydantic.PastDatetime,
}

FIELD_KWARGS = {
    "_peerdb_is_deleted": {"ge": 0, "le": 1},
    "_peerdb_version": {"ge": 0},
}


def get_table_ddl(dsn: str, database: str, table: str) -> str:
    with get_clickhouse_client(dsn) as client:
        result = client.command(
            "show create table {database:Identifier}.{table:Identifier};",
            parameters={
                "database": database,
                "table": table,
            },
        )
        result = result.replace("\\n", "\n")

    return result


def get_table_schema(dsn: str, database: str, table: str) -> Table:
    engine = create_engine(dsn, echo=False)
    metadata = MetaData(schema=database)
    metadata.reflect(bind=engine)

    return metadata.tables.get(f"{database}.{table}")


def parse_ddl(ddl: str) -> dict:
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
    matches = re.finditer(pattern, ddl, re.IGNORECASE)

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


def to_field_type(python_type) -> str:
    if python_type is None:
        return "None"
    elif python_type.__module__ in ["datetime"]:
        return f"{python_type.__module__}.{python_type.__qualname__}"
    else:
        return python_type.__qualname__


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


def get_sqlalchemy_type(type_str: str) -> ASTNode:
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

    return parse_inner(type_str)


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


def create_model_code(dsn: str, database: str, table: str, model_name: str) -> str:
    """Create a SQLModel class from a table schema."""
    schema = get_table_schema(dsn, database, table)

    ddl = get_table_ddl(dsn, database, table)
    parsed_ddl = parse_ddl(ddl)
    table_kwargs = {"schema": database}
    engine = parsed_ddl["engine"]
    engine_kwargs = {}

    if parsed_ddl["version"]:
        engine_kwargs["version"] = f"'{parsed_ddl["version"]}'"

    if parsed_ddl["order_by"]:
        engine_kwargs["order_by"] = tuple(parsed_ddl["order_by"])

    if parsed_ddl["primary_key"]:
        engine_kwargs["primary_key"] = tuple(parsed_ddl["primary_key"])

    imports = [
        "from clickhouse_sqlalchemy import types, engines",
        "from sqlmodel import Column, Field, SQLModel",
    ]

    columns = []

    for column in schema.columns:
        try:
            nested_type = getattr(column.type, "nested_type")
        except AttributeError:
            nested_type = None

        type_ = nested_type if nested_type else column.type

        try:
            python_type = getattr(type_, "python_type")
        except NotImplementedError:
            python_type = None

        if column.name in PYTHON_TYPE:
            python_type = PYTHON_TYPE[column.name]
        elif isinstance(column.type, types.UUID):
            python_type = uuid.UUID

        field_name = to_field_name(column.name)
        field_type = to_field_type(python_type)

        sa_column_kwargs = {
            "name": f"'{column.name}'",
            "type_": get_sqlalchemy_type(str(column.type)),
        }

        if column.primary_key:
            sa_column_kwargs["primary_key"] = True

        if column.nullable:
            sa_column_kwargs["nullable"] = True
            field_type = f"{field_type} | None"

        field_kwargs = {
            "sa_column": f"Column({serialize_dict(sa_column_kwargs)})",
        }

        if column.name in FIELD_KWARGS:
            field_kwargs.update(FIELD_KWARGS[column.name])

        column_def = f"{field_name}: {field_type} = Field({serialize_dict(field_kwargs)})"
        columns.append(column_def)

        class_import = get_class_import_string(python_type)
        if class_import:
            imports.append(class_import)

    # Create model code
    lines = []

    # Add DDL for reference
    lines.append('"""\nCreated from:\n\n' + ddl + '\n"""')
    lines.append("")

    # Add imports
    imports = sorted(list(set(imports)))
    for class_import in imports:
        lines.append(class_import)

    # Add table class
    lines.append("")
    lines.append("")
    lines.append(f"class {model_name}(SQLModel, table=True):")
    lines.append(INDENT + f"__tablename__ = '{table}'")
    lines.append(
        INDENT
        + f"__table_args__ = (engines.{engine}({serialize_dict(engine_kwargs)}), {table_kwargs},)"
    )
    lines.append("")

    # Add columns
    for column in columns:
        lines.append(INDENT + column)

    return "\n".join(lines) + "\n"


def create_factory_code(model_name: str, random_seed: int = 0) -> str:
    """Create a Pydantic model factory."""
    model_filename = create_class_filename(model_name)
    factory_name = create_factory_name(model_name)

    imports = [
        f"from .{model_filename} import {model_name}",
        "from polyfactory.factories.pydantic_factory import ModelFactory",
    ]

    lines = []

    # Add imports
    imports = sorted(list(set(imports)))
    for import_ in imports:
        lines.append(import_)

    # Add factory class
    lines.append("")
    lines.append("")
    lines.append(f"class {factory_name}(ModelFactory[{model_name}]):")
    lines.append(INDENT + f"__random_seed__ = {random_seed}")

    return "\n".join(lines) + "\n"
