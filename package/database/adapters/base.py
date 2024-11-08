from ..types import CHSettings, PGSettings
from ..utils import create_connection_url
from abc import ABC, abstractmethod
from sqlmodel import Table
from typing import List, Optional, overload


class BaseAdapter(ABC):
    def __init__(self, settings: CHSettings | PGSettings) -> None:
        self.settings = settings
        self.dsn = create_connection_url(**settings.model_dump())

    @abstractmethod
    def get_client():
        pass

    @abstractmethod
    def has_database(self, database: str) -> bool:
        pass

    @abstractmethod
    def create_database(self, database: str) -> None:
        pass

    @abstractmethod
    def drop_database(self, database: str) -> None:
        pass

    @abstractmethod
    def has_schema():
        pass

    @overload
    @abstractmethod
    def has_table(self, table: str, database: Optional[str] = None) -> bool: ...

    @overload
    @abstractmethod
    def has_table(self, table: str, schema: Optional[str] = None) -> bool: ...

    @abstractmethod
    def has_table(self, *args, **kwargs) -> bool:
        pass

    @overload
    @abstractmethod
    def get_table_schema(self, table: str, database: Optional[str] = None) -> Table: ...

    @overload
    @abstractmethod
    def get_table_schema(self, table: str, schema: Optional[str] = None) -> Table: ...

    @abstractmethod
    def get_table_schema(self, *args, **kwargs) -> Table:
        pass

    @overload
    @abstractmethod
    def set_table_replica_identity(
        self, table: str, replica_identity: str, database: Optional[str] = None
    ) -> None: ...

    @overload
    @abstractmethod
    def set_table_replica_identity(
        self, table: str, replica_identity: str, schema: Optional[str] = None
    ) -> None: ...

    @abstractmethod
    def set_table_replica_identity(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def create_table(self, table: str, statement: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def create_table(self, table: str, statement: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def create_table(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def get_create_table_statement(self, table: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def get_create_table_statement(self, table: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def get_create_table_statement(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def drop_table(self, table: str, database: Optional[str] = None) -> None: ...

    @overload
    @abstractmethod
    def drop_table(self, table: str, schema: Optional[str] = None) -> None: ...

    @abstractmethod
    def drop_table(self, *args, **kwargs) -> None:
        pass

    @overload
    @abstractmethod
    def list_tables(self, database: Optional[str] = None) -> List[Table]: ...

    @overload
    @abstractmethod
    def list_tables(self, schema: Optional[str] = None) -> List[Table]: ...

    @abstractmethod
    def list_tables(self, *args, **kwargs) -> List[Table]:
        pass

    @abstractmethod
    def has_user(self, username: str) -> bool:
        pass

    @abstractmethod
    def create_user(self, username: str, password: str) -> None:
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
    def has_publication(self, publication: str) -> bool:
        pass

    @abstractmethod
    def create_publication(self, publication: str, tables: List[str]) -> None:
        pass

    @abstractmethod
    def drop_publication(self, publication: str) -> None:
        pass
