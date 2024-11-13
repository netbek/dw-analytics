from abc import ABC, abstractmethod
from contextlib import contextmanager
from package.types import CHSettings, PGSettings
from sqlmodel import create_engine, Session, Table
from typing import Any, Generator, List, Optional, overload


class BaseAdapter(ABC):
    def __init__(self, settings: CHSettings | PGSettings) -> None:
        self.settings = settings

    @overload
    @classmethod
    @abstractmethod
    def create_uri(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        schema: str,
    ) -> str: ...

    @overload
    @classmethod
    @abstractmethod
    def create_uri(
        cls,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        driver: Optional[str] = None,
        secure: Optional[bool] = None,
    ) -> str: ...

    @classmethod
    @abstractmethod
    def create_uri(cls, *args, **kwargs) -> str:
        pass

    @abstractmethod
    def create_client():
        pass

    @contextmanager
    def create_engine(self):
        engine = create_engine(self.uri, echo=False)

        yield engine

        engine.dispose()

    @contextmanager
    def create_session(self) -> Generator[Session, Any, None]:
        with self.create_engine() as engine:
            session = Session(engine)

        yield session

        session.close()

    @abstractmethod
    def has_database(self, database: str) -> bool:
        pass

    @abstractmethod
    def create_database(self, database: str, replace: Optional[bool] = False) -> None:
        pass

    @abstractmethod
    def drop_database(self, database: str) -> None:
        pass

    @abstractmethod
    def has_schema(self, schema: str, database: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    def create_schema(
        self, schema: str, database: Optional[str] = None, replace: Optional[bool] = False
    ) -> None:
        pass

    @abstractmethod
    def drop_schema(self, schema: str, database: Optional[str] = None) -> None:
        pass

    @overload
    @abstractmethod
    def has_table(self, table: str, database: Optional[str] = None) -> bool: ...

    @overload
    @abstractmethod
    def has_table(
        self, table: str, database: Optional[str] = None, schema: Optional[str] = None
    ) -> bool: ...

    @abstractmethod
    def has_table(self, *args, **kwargs) -> bool:
        pass

    @overload
    @abstractmethod
    def create_table(
        self,
        table: str,
        statement: str,
        database: Optional[str] = None,
        replace: Optional[bool] = False,
    ) -> None: ...

    @overload
    @abstractmethod
    def create_table(
        self,
        table: str,
        statement: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        replace: Optional[bool] = False,
    ) -> None: ...

    @abstractmethod
    def create_table(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def get_create_table_statement(self, table: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def get_create_table_statement(
        self, table: str, database: Optional[str] = None, schema: Optional[str] = None
    ) -> None: ...

    @abstractmethod
    def get_create_table_statement(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def drop_table(self, table: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def drop_table(
        self, table: str, database: Optional[str] = None, schema: Optional[str] = None
    ) -> None: ...

    @abstractmethod
    def drop_table(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def get_table(self, table: str, database: Optional[str] = None) -> Table: ...

    @overload
    @abstractmethod
    def get_table(
        self, table: str, database: Optional[str] = None, schema: Optional[str] = None
    ) -> Table: ...

    @abstractmethod
    def get_table(self, *args, **kwargs) -> Table:
        pass

    @overload
    @abstractmethod
    def get_table_replica_identity(self, table: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def get_table_replica_identity(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None: ...

    @abstractmethod
    def get_table_replica_identity(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def set_table_replica_identity(
        self, table: str, replica_identity: str, database: Optional[str] = None
    ) -> None: ...

    @overload
    @abstractmethod
    def set_table_replica_identity(
        self,
        table: str,
        replica_identity: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None: ...

    @abstractmethod
    def set_table_replica_identity(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def drop_tables(self, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def drop_tables(self, database: Optional[str] = None, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def drop_tables(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def list_tables(self, database: Optional[str] = None) -> List[Table]: ...

    @overload
    @abstractmethod
    def list_tables(
        self, database: Optional[str] = None, schema: Optional[str] = None
    ) -> List[Table]: ...

    @abstractmethod
    def list_tables(self, *args, **kwargs) -> List[Table]:
        pass

    @abstractmethod
    def has_user(self, username: str) -> bool:
        pass

    @overload
    @abstractmethod
    def create_user(
        self, username: str, password: str, replace: Optional[bool] = False
    ) -> None: ...

    @overload
    @abstractmethod
    def create_user(
        self,
        username: str,
        password: str,
        options: Optional[dict] = None,
        replace: Optional[bool] = False,
    ) -> None: ...

    @abstractmethod
    def create_user(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def drop_user(self, username: str) -> None:
        pass

    @overload
    @abstractmethod
    def grant_user_privileges(self, username: str, database: str) -> None: ...

    @overload
    @abstractmethod
    def grant_user_privileges(self, username: str, schema: str) -> None: ...

    @abstractmethod
    def grant_user_privileges(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def revoke_user_privileges(self, username: str, database: str) -> None: ...

    @overload
    @abstractmethod
    def revoke_user_privileges(self, username: str, schema: str) -> None: ...

    @abstractmethod
    def revoke_user_privileges(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def list_user_privileges(self, username: str) -> List[tuple]:
        pass

    @abstractmethod
    def has_publication(self, publication: str) -> bool:
        pass

    @abstractmethod
    def create_publication(self, publication: str, tables: List[str]) -> None:
        pass

    @abstractmethod
    def drop_publication(self, publication: str) -> None:
        pass

    @abstractmethod
    def list_publications(self) -> List[str]:
        pass
