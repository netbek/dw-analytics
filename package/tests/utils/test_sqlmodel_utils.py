from clickhouse_sqlalchemy import types
from package.utils.sqlmodel_utils import get_pydantic_type, get_python_type, get_sqlalchemy_type
from sqlalchemy import Column

import datetime
import pytest
import uuid


@pytest.fixture
def boolean_column() -> Column:
    return Column(type_=types.Boolean, nullable=False)


@pytest.fixture
def nullable_boolean_column() -> Column:
    return Column(type_=types.Nullable(types.Boolean), nullable=True)


@pytest.fixture
def int64_column() -> Column:
    return Column(type_=types.Int64, nullable=False)


@pytest.fixture
def nullable_int64_column() -> Column:
    return Column(type_=types.Nullable(types.Int64), nullable=True)


@pytest.fixture
def datetime64_column() -> Column:
    return Column(type_=types.DateTime64(9), nullable=False)


@pytest.fixture
def nullable_datetime64_column() -> Column:
    return Column(type_=types.Nullable(types.DateTime64(9)), nullable=True)


@pytest.fixture
def string_column() -> Column:
    return Column(type_=types.String, nullable=False)


@pytest.fixture
def nullable_string_column() -> Column:
    return Column(type_=types.Nullable(types.String), nullable=True)


@pytest.fixture
def uuid_column() -> Column:
    return Column(type_=types.UUID, nullable=False)


@pytest.fixture
def nullable_uuid_column() -> Column:
    return Column(type_=types.Nullable(types.UUID), nullable=True)


class TestGetPydanticType:
    def test_boolean_column(self, boolean_column):
        assert get_pydantic_type(boolean_column) == "bool"

    def test_nullable_boolean_column(self, nullable_boolean_column):
        assert get_pydantic_type(nullable_boolean_column) == "bool | None"

    def test_int64_column(self, int64_column):
        assert get_pydantic_type(int64_column) == "int"

    def test_nullable_int64_column(self, nullable_int64_column):
        assert get_pydantic_type(nullable_int64_column) == "int | None"

    def test_datetime64(self, datetime64_column):
        assert get_pydantic_type(datetime64_column) == "datetime.datetime"

    def test_nullable_datetime64(self, nullable_datetime64_column):
        assert get_pydantic_type(nullable_datetime64_column) == "datetime.datetime | None"

    def test_string_column(self, string_column):
        assert get_pydantic_type(string_column) == "str"

    def test_nullable_string_column(self, nullable_string_column):
        assert get_pydantic_type(nullable_string_column) == "str | None"

    def test_uuid(self, uuid_column):
        assert get_pydantic_type(uuid_column) == "UUID"

    def test_nullable_uuid(self, nullable_uuid_column):
        assert get_pydantic_type(nullable_uuid_column) == "UUID | None"


class TestGetPythonType:
    def test_boolean(self, boolean_column):
        assert isinstance(get_python_type(boolean_column), type(bool))

    def test_nullable_boolean(self, nullable_boolean_column):
        assert isinstance(get_python_type(nullable_boolean_column), type(bool))

    def test_int64_column(self, int64_column):
        assert isinstance(get_python_type(int64_column), type(int))

    def test_nullable_int64_column(self, nullable_int64_column):
        assert isinstance(get_python_type(nullable_int64_column), type(int))

    def test_datetime64(self, datetime64_column):
        assert get_python_type(datetime64_column) == datetime.datetime

    def test_nullable_datetime64(self, nullable_datetime64_column):
        assert get_python_type(nullable_datetime64_column) == datetime.datetime

    def test_string(self, string_column):
        assert isinstance(get_python_type(string_column), type(str))

    def test_nullable_string(self, nullable_string_column):
        assert isinstance(get_python_type(nullable_string_column), type(str))

    def test_uuid(self, uuid_column):
        assert get_python_type(uuid_column) == uuid.UUID

    def test_nullable_uuid(self, nullable_uuid_column):
        assert get_python_type(nullable_uuid_column) == uuid.UUID


class TestGetSQLAlchemyType:
    def test_boolean_column(self, boolean_column):
        assert get_sqlalchemy_type(boolean_column) == "types.Boolean"

    def test_nullable_boolean_column(self, nullable_boolean_column):
        assert get_sqlalchemy_type(nullable_boolean_column) == "types.Nullable(types.Boolean)"

    def test_int64_column(self, int64_column):
        assert get_sqlalchemy_type(int64_column) == "types.Int64"

    def test_nullable_int64_column(self, nullable_int64_column):
        assert get_sqlalchemy_type(nullable_int64_column) == "types.Nullable(types.Int64)"

    def test_datetime64(self, datetime64_column):
        assert get_sqlalchemy_type(datetime64_column) == "types.DateTime64(9)"

    def test_nullable_datetime64(self, nullable_datetime64_column):
        assert (
            get_sqlalchemy_type(nullable_datetime64_column) == "types.Nullable(types.DateTime64(9))"
        )

    def test_string_column(self, string_column):
        assert get_sqlalchemy_type(string_column) == "types.String"

    def test_nullable_string_column(self, nullable_string_column):
        assert get_sqlalchemy_type(nullable_string_column) == "types.Nullable(types.String)"

    def test_uuid(self, uuid_column):
        assert get_sqlalchemy_type(uuid_column) == "types.UUID"

    def test_nullable_uuid(self, nullable_uuid_column):
        assert get_sqlalchemy_type(nullable_uuid_column) == "types.Nullable(types.UUID)"
